package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaDataIndexAliasesService extends AbstractComponent {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AliasValidator aliasValidator;

    private final NodeServicesProvider nodeServicesProvider;

    @Inject
    public MetaDataIndexAliasesService(Settings settings, ClusterService clusterService, IndicesService indicesService, AliasValidator aliasValidator, NodeServicesProvider nodeServicesProvider) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.aliasValidator = aliasValidator;
        this.nodeServicesProvider = nodeServicesProvider;
    }

    public void indicesAliases(final IndicesAliasesClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("index-aliases", new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                List<Index> indicesToClose = new ArrayList<>();
                Map<String, IndexService> indices = new HashMap<>();
                try {
                    for (AliasAction aliasAction : request.actions()) {
                        aliasValidator.validateAliasAction(aliasAction, currentState.metaData());
                        if (!currentState.metaData().hasIndex(aliasAction.index())) {
                            throw new IndexNotFoundException(aliasAction.index());
                        }
                    }
                    boolean changed = false;
                    MetaData.Builder builder = MetaData.builder(currentState.metaData());
                    for (AliasAction aliasAction : request.actions()) {
                        IndexMetaData indexMetaData = builder.get(aliasAction.index());
                        if (indexMetaData == null) {
                            throw new IndexNotFoundException(aliasAction.index());
                        }
                        IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
                        if (aliasAction.actionType() == AliasAction.Type.ADD) {
                            String filter = aliasAction.filter();
                            if (Strings.hasLength(filter)) {
                                IndexService indexService = indices.get(indexMetaData.getIndex());
                                if (indexService == null) {
                                    indexService = indicesService.indexService(indexMetaData.getIndex());
                                    if (indexService == null) {
                                        try {
                                            indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, Collections.emptyList(), shardId -> {
                                            });
                                            for (ObjectCursor<MappingMetaData> cursor : indexMetaData.getMappings().values()) {
                                                MappingMetaData mappingMetaData = cursor.value;
                                                indexService.mapperService().merge(mappingMetaData.type(), mappingMetaData.source(), MapperService.MergeReason.MAPPING_RECOVERY, false);
                                            }
                                        } catch (Exception e) {
                                            logger.warn("[{}] failed to temporary create in order to apply alias action", e, indexMetaData.getIndex());
                                            continue;
                                        }
                                        indicesToClose.add(indexMetaData.getIndex());
                                    }
                                    indices.put(indexMetaData.getIndex().getName(), indexService);
                                }
                                aliasValidator.validateAliasFilter(aliasAction.alias(), filter, indexService.newQueryShardContext());
                            }
                            AliasMetaData newAliasMd = AliasMetaData.newAliasMetaDataBuilder(aliasAction.alias()).filter(filter).indexRouting(aliasAction.indexRouting()).searchRouting(aliasAction.searchRouting()).build();
                            AliasMetaData aliasMd = indexMetaData.getAliases().get(aliasAction.alias());
                            if (aliasMd != null && aliasMd.equals(newAliasMd)) {
                                continue;
                            }
                            indexMetaDataBuilder.putAlias(newAliasMd);
                        } else if (aliasAction.actionType() == AliasAction.Type.REMOVE) {
                            if (!indexMetaData.getAliases().containsKey(aliasAction.alias())) {
                                continue;
                            }
                            indexMetaDataBuilder.removeAlias(aliasAction.alias());
                        }
                        changed = true;
                        builder.put(indexMetaDataBuilder);
                    }
                    if (changed) {
                        ClusterState updatedState = ClusterState.builder(currentState).metaData(builder).build();
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
        });
    }
}