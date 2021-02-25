package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidTypeNameException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaDataMappingService extends AbstractComponent {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    final ClusterStateTaskExecutor<RefreshTask> refreshExecutor = new RefreshTaskExecutor();

    final ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> putMappingExecutor = new PutMappingExecutor();

    @Inject
    public MetaDataMappingService(Settings settings, ClusterService clusterService, IndicesService indicesService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    static class RefreshTask {

        final String index;

        final String indexUUID;

        RefreshTask(String index, final String indexUUID) {
            this.index = index;
            this.indexUUID = indexUUID;
        }

        @Override
        public String toString() {
            return "[" + index + "][" + indexUUID + "]";
        }
    }

    class RefreshTaskExecutor implements ClusterStateTaskExecutor<RefreshTask> {

        @Override
        public BatchResult<RefreshTask> execute(ClusterState currentState, List<RefreshTask> tasks) throws Exception {
            ClusterState newClusterState = executeRefresh(currentState, tasks);
            return BatchResult.<RefreshTask>builder().successes(tasks).build(newClusterState);
        }
    }

    ClusterState executeRefresh(final ClusterState currentState, final List<RefreshTask> allTasks) throws Exception {
        Map<String, List<RefreshTask>> tasksPerIndex = new HashMap<>();
        for (RefreshTask task : allTasks) {
            if (task.index == null) {
                logger.debug("ignoring a mapping task of type [{}] with a null index.", task);
            }
            tasksPerIndex.computeIfAbsent(task.index, k -> new ArrayList<>()).add(task);
        }
        boolean dirty = false;
        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        for (Map.Entry<String, List<RefreshTask>> entry : tasksPerIndex.entrySet()) {
            IndexMetaData indexMetaData = mdBuilder.get(entry.getKey());
            if (indexMetaData == null) {
                logger.debug("[{}] ignoring tasks - index meta data doesn't exist", entry.getKey());
                continue;
            }
            final Index index = indexMetaData.getIndex();
            List<RefreshTask> allIndexTasks = entry.getValue();
            boolean hasTaskWithRightUUID = false;
            for (RefreshTask task : allIndexTasks) {
                if (indexMetaData.isSameUUID(task.indexUUID)) {
                    hasTaskWithRightUUID = true;
                } else {
                    logger.debug("{} ignoring task [{}] - index meta data doesn't match task uuid", index, task);
                }
            }
            if (hasTaskWithRightUUID == false) {
                continue;
            }
            boolean removeIndex = false;
            IndexService indexService = indicesService.indexService(indexMetaData.getIndex());
            if (indexService == null) {
                indexService = indicesService.createIndex(indexMetaData, Collections.emptyList(), shardId -> {
                });
                removeIndex = true;
                for (ObjectCursor<MappingMetaData> metaData : indexMetaData.getMappings().values()) {
                    indexService.mapperService().merge(metaData.value.type(), metaData.value.source(), MapperService.MergeReason.MAPPING_RECOVERY, true);
                }
            }
            IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
            try {
                boolean indexDirty = refreshIndexMapping(indexService, builder);
                if (indexDirty) {
                    mdBuilder.put(builder);
                    dirty = true;
                }
            } finally {
                if (removeIndex) {
                    indicesService.removeIndex(index, "created for mapping processing");
                }
            }
        }
        if (!dirty) {
            return currentState;
        }
        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }

    private boolean refreshIndexMapping(IndexService indexService, IndexMetaData.Builder builder) {
        boolean dirty = false;
        String index = indexService.index().getName();
        try {
            List<String> updatedTypes = new ArrayList<>();
            for (DocumentMapper mapper : indexService.mapperService().docMappers(true)) {
                final String type = mapper.type();
                if (!mapper.mappingSource().equals(builder.mapping(type).source())) {
                    updatedTypes.add(type);
                }
            }
            if (updatedTypes.isEmpty() == false) {
                logger.warn("[{}] re-syncing mappings with cluster state because of types [{}]", index, updatedTypes);
                dirty = true;
                for (DocumentMapper mapper : indexService.mapperService().docMappers(true)) {
                    builder.putMapping(new MappingMetaData(mapper));
                }
            }
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to refresh-mapping in cluster state", index), e);
        }
        return dirty;
    }

    public void refreshMapping(final String index, final String indexUUID) {
        final RefreshTask refreshTask = new RefreshTask(index, indexUUID);
        clusterService.submitStateUpdateTask("refresh-mapping", refreshTask, ClusterStateTaskConfig.build(Priority.HIGH), refreshExecutor, (source, e) -> logger.warn((Supplier<?>) () -> new ParameterizedMessage("failure during [{}]", source), e));
    }

    class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> {

        @Override
        public BatchResult<PutMappingClusterStateUpdateRequest> execute(ClusterState currentState, List<PutMappingClusterStateUpdateRequest> tasks) throws Exception {
            Map<Index, MapperService> indexMapperServices = new HashMap<>();
            BatchResult.Builder<PutMappingClusterStateUpdateRequest> builder = BatchResult.builder();
            try {
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    try {
                        for (Index index : request.indices()) {
                            final IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
                            if (indexMapperServices.containsKey(indexMetaData.getIndex()) == false) {
                                MapperService mapperService = indicesService.createIndexMapperService(indexMetaData);
                                indexMapperServices.put(index, mapperService);
                                for (ObjectCursor<MappingMetaData> mapping : indexMetaData.getMappings().values()) {
                                    mapperService.merge(mapping.value.type(), mapping.value.source(), MapperService.MergeReason.MAPPING_RECOVERY, request.updateAllTypes());
                                }
                            }
                        }
                        currentState = applyRequest(currentState, request, indexMapperServices);
                        builder.success(request);
                    } catch (Exception e) {
                        builder.failure(request, e);
                    }
                }
                return builder.build(currentState);
            } finally {
                IOUtils.close(indexMapperServices.values());
            }
        }

        private ClusterState applyRequest(ClusterState currentState, PutMappingClusterStateUpdateRequest request, Map<Index, MapperService> indexMapperServices) throws IOException {
            String mappingType = request.type();
            CompressedXContent mappingUpdateSource = new CompressedXContent(request.source());
            final MetaData metaData = currentState.metaData();
            final List<IndexMetaData> updateList = new ArrayList<>();
            for (Index index : request.indices()) {
                MapperService mapperService = indexMapperServices.get(index);
                final IndexMetaData indexMetaData = currentState.getMetaData().getIndexSafe(index);
                updateList.add(indexMetaData);
                DocumentMapper newMapper;
                DocumentMapper existingMapper = mapperService.documentMapper(request.type());
                if (MapperService.DEFAULT_MAPPING.equals(request.type())) {
                    newMapper = mapperService.parse(request.type(), mappingUpdateSource, false);
                } else {
                    newMapper = mapperService.parse(request.type(), mappingUpdateSource, existingMapper == null);
                    if (existingMapper != null) {
                        existingMapper.merge(newMapper.mapping(), request.updateAllTypes());
                    } else {
                        if (newMapper.parentFieldMapper().active()) {
                            for (ObjectCursor<MappingMetaData> mapping : indexMetaData.getMappings().values()) {
                                String parentType = newMapper.parentFieldMapper().type();
                                if (parentType.equals(mapping.value.type()) && mapperService.getParentTypes().contains(parentType) == false) {
                                    throw new IllegalArgumentException("can't add a _parent field that points to an " + "already existing type, that isn't already a parent");
                                }
                            }
                        }
                    }
                }
                if (mappingType == null) {
                    mappingType = newMapper.type();
                } else if (mappingType.equals(newMapper.type()) == false) {
                    throw new InvalidTypeNameException("Type name provided does not match type name within mapping definition");
                }
            }
            assert mappingType != null;
            if (!MapperService.DEFAULT_MAPPING.equals(mappingType) && mappingType.charAt(0) == '_') {
                throw new InvalidTypeNameException("Document mapping type name can't start with '_', found: [" + mappingType + "]");
            }
            MetaData.Builder builder = MetaData.builder(metaData);
            boolean updated = false;
            for (IndexMetaData indexMetaData : updateList) {
                final Index index = indexMetaData.getIndex();
                final MapperService mapperService = indexMapperServices.get(index);
                CompressedXContent existingSource = null;
                DocumentMapper existingMapper = mapperService.documentMapper(mappingType);
                if (existingMapper != null) {
                    existingSource = existingMapper.mappingSource();
                }
                DocumentMapper mergedMapper = mapperService.merge(mappingType, mappingUpdateSource, MapperService.MergeReason.MAPPING_UPDATE, request.updateAllTypes());
                CompressedXContent updatedSource = mergedMapper.mappingSource();
                if (existingSource != null) {
                    if (existingSource.equals(updatedSource)) {
                    } else {
                        updated = true;
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} update_mapping [{}] with source [{}]", index, mergedMapper.type(), updatedSource);
                        } else if (logger.isInfoEnabled()) {
                            logger.info("{} update_mapping [{}]", index, mergedMapper.type());
                        }
                    }
                } else {
                    updated = true;
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} create_mapping [{}] with source [{}]", index, mappingType, updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("{} create_mapping [{}]", index, mappingType);
                    }
                }
                IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
                for (DocumentMapper mapper : mapperService.docMappers(true)) {
                    indexMetaDataBuilder.putMapping(new MappingMetaData(mapper.mappingSource()));
                }
                builder.put(indexMetaDataBuilder);
            }
            if (updated) {
                return ClusterState.builder(currentState).metaData(builder).build();
            } else {
                return currentState;
            }
        }

        @Override
        public String describeTasks(List<PutMappingClusterStateUpdateRequest> tasks) {
            return tasks.stream().map(PutMappingClusterStateUpdateRequest::type).reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        }
    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("put-mapping", request, ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()), putMappingExecutor, new AckedClusterStateTaskListener() {

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Exception e) {
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.ackTimeout();
            }
        });
    }
}