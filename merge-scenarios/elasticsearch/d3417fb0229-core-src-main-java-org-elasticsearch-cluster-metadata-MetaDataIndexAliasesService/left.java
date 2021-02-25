package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.AliasAction.NewAliasValidator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import static java.util.Collections.emptyList;

public class MetaDataIndexAliasesService extends AbstractComponent {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AliasValidator aliasValidator;

    private final NodeServicesProvider nodeServicesProvider;

    private final MetaDataDeleteIndexService deleteIndexService;

    @Inject
    public MetaDataIndexAliasesService(Settings settings, ClusterService clusterService, IndicesService indicesService, AliasValidator aliasValidator, NodeServicesProvider nodeServicesProvider, MetaDataDeleteIndexService deleteIndexService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.aliasValidator = aliasValidator;
        this.nodeServicesProvider = nodeServicesProvider;
        this.deleteIndexService = deleteIndexService;
    }

    public void indicesAliases(final IndicesAliasesClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("index-aliases", new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerExecute(currentState, request.actions());
            }
        });
    }

    ClusterState innerExecute(ClusterState currentState, Iterable<AliasAction> actions) {
        List<Index> indicesToClose = new ArrayList<>();
        Map<String, IndexService> indices = new HashMap<>();
        try {
            boolean changed = false;
            Set<Index> indicesToDelete = new HashSet<>();
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    IndexMetaData index = currentState.metaData().getIndices().get(action.getIndex());
                    if (index == null) {
                        throw new IndexNotFoundException(action.getIndex());
                    }
                    indicesToDelete.add(index.getIndex());
                    changed = true;
                }
            }
            if (changed) {
                currentState = deleteIndexService.deleteIndices(currentState, indicesToDelete);
            }
            MetaData.Builder metadata = MetaData.builder(currentState.metaData());
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    continue;
                }
                IndexMetaData index = metadata.get(action.getIndex());
                if (index == null) {
                    throw new IndexNotFoundException(action.getIndex());
                }
                NewAliasValidator newAliasValidator = (alias, indexRouting, filter) -> {
                    Function<String, IndexMetaData> indexLookup = name -> metadata.get(name);
                    aliasValidator.validateAlias(alias, action.getIndex(), indexRouting, indexLookup);
                    if (Strings.hasLength(filter)) {
                        IndexService indexService = indices.get(index.getIndex());
                        if (indexService == null) {
                            indexService = indicesService.indexService(index.getIndex());
                            if (indexService == null) {
                                try {
                                    indexService = indicesService.createIndex(nodeServicesProvider, index, emptyList(), shardId -> {
                                    });
                                } catch (IOException e) {
                                    throw new ElasticsearchException("Failed to create temporary index for parsing the alias", e);
                                }
                                for (ObjectCursor<MappingMetaData> cursor : index.getMappings().values()) {
                                    MappingMetaData mappingMetaData = cursor.value;
                                    indexService.mapperService().merge(mappingMetaData.type(), mappingMetaData.source(), MapperService.MergeReason.MAPPING_RECOVERY, false);
                                }
                                indicesToClose.add(index.getIndex());
                            }
                            indices.put(action.getIndex(), indexService);
                        }
                        aliasValidator.validateAliasFilter(alias, filter, indexService.newQueryShardContext());
                    }
                };
                changed |= action.apply(newAliasValidator, metadata, index);
            }
            if (changed) {
                ClusterState updatedState = ClusterState.builder(currentState).metaData(metadata).build();
                if (!updatedState.metaData().equalsAliases(currentState.metaData())) {
                    return updatedState;
                }
            }
            return currentState;
        } finally {
            for (Index index : indicesToClose) {
                indicesService.removeIndex(index, "created for alias processing");
            }
        }
    }
}