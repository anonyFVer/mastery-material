package org.elasticsearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class IngestService implements ClusterStateApplier {

    public static final String NOOP_PIPELINE_NAME = "_none";

    private static final Logger logger = LogManager.getLogger(IngestService.class);

    private final ClusterService clusterService;

    private final ScriptService scriptService;

    private final Map<String, Processor.Factory> processorFactories;

    private volatile Map<String, Pipeline> pipelines = new HashMap<>();

    private final ThreadPool threadPool;

    private final IngestMetric totalMetrics = new IngestMetric();

    public IngestService(ClusterService clusterService, ThreadPool threadPool, Environment env, ScriptService scriptService, AnalysisRegistry analysisRegistry, List<IngestPlugin> ingestPlugins) {
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.processorFactories = processorFactories(ingestPlugins, new Processor.Parameters(env, scriptService, analysisRegistry, threadPool.getThreadContext(), threadPool::relativeTimeInMillis, (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC), this));
        this.threadPool = threadPool;
    }

    private static Map<String, Processor.Factory> processorFactories(List<IngestPlugin> ingestPlugins, Processor.Parameters parameters) {
        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        for (IngestPlugin ingestPlugin : ingestPlugins) {
            Map<String, Processor.Factory> newProcessors = ingestPlugin.getProcessors(parameters);
            for (Map.Entry<String, Processor.Factory> entry : newProcessors.entrySet()) {
                if (processorFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Ingest processor [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(processorFactories);
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    public void delete(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete-pipeline-" + request.getId(), new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerDelete(request, currentState);
            }
        });
    }

    static ClusterState innerDelete(DeletePipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        if (currentIngestMetadata == null) {
            return currentState;
        }
        Map<String, PipelineConfiguration> pipelines = currentIngestMetadata.getPipelines();
        Set<String> toRemove = new HashSet<>();
        for (String pipelineKey : pipelines.keySet()) {
            if (Regex.simpleMatch(request.getId(), pipelineKey)) {
                toRemove.add(pipelineKey);
            }
        }
        if (toRemove.isEmpty() && Regex.isMatchAllPattern(request.getId()) == false) {
            throw new ResourceNotFoundException("pipeline [{}] is missing", request.getId());
        } else if (toRemove.isEmpty()) {
            return currentState;
        }
        final Map<String, PipelineConfiguration> pipelinesCopy = new HashMap<>(pipelines);
        for (String key : toRemove) {
            pipelinesCopy.remove(key);
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelinesCopy)).build());
        return newState.build();
    }

    public static List<PipelineConfiguration> getPipelines(ClusterState clusterState, String... ids) {
        IngestMetadata ingestMetadata = clusterState.getMetaData().custom(IngestMetadata.TYPE);
        return innerGetPipelines(ingestMetadata, ids);
    }

    static List<PipelineConfiguration> innerGetPipelines(IngestMetadata ingestMetadata, String... ids) {
        if (ingestMetadata == null) {
            return Collections.emptyList();
        }
        if (ids.length == 0) {
            return new ArrayList<>(ingestMetadata.getPipelines().values());
        }
        List<PipelineConfiguration> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineConfiguration> entry : ingestMetadata.getPipelines().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(id);
                if (pipeline != null) {
                    result.add(pipeline);
                }
            }
        }
        return result;
    }

    public void putPipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener) throws Exception {
        validatePipeline(ingestInfos, request);
        clusterService.submitStateUpdateTask("put-pipeline-" + request.getId(), new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerPut(request, currentState);
            }
        });
    }

    public Pipeline getPipeline(String id) {
        return pipelines.get(id);
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }

    public IngestInfo info() {
        Map<String, Processor.Factory> processorFactories = getProcessorFactories();
        List<ProcessorInfo> processorInfoList = new ArrayList<>(processorFactories.size());
        for (Map.Entry<String, Processor.Factory> entry : processorFactories.entrySet()) {
            processorInfoList.add(new ProcessorInfo(entry.getKey()));
        }
        return new IngestInfo(processorInfoList);
    }

    Map<String, Pipeline> pipelines() {
        return pipelines;
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        ClusterState state = event.state();
        Map<String, Pipeline> originalPipelines = pipelines;
        try {
            innerUpdatePipelines(event.previousState(), state);
        } catch (ElasticsearchParseException e) {
            logger.warn("failed to update ingest pipelines", e);
        }
        if (originalPipelines != pipelines) {
            pipelines.forEach((id, pipeline) -> {
                Pipeline originalPipeline = originalPipelines.get(id);
                if (originalPipeline != null) {
                    pipeline.getMetrics().add(originalPipeline.getMetrics());
                    List<Tuple<Processor, IngestMetric>> oldPerProcessMetrics = new ArrayList<>();
                    List<Tuple<Processor, IngestMetric>> newPerProcessMetrics = new ArrayList<>();
                    getProcessorMetrics(originalPipeline.getCompoundProcessor(), oldPerProcessMetrics);
                    getProcessorMetrics(pipeline.getCompoundProcessor(), newPerProcessMetrics);
                    if (newPerProcessMetrics.size() == oldPerProcessMetrics.size()) {
                        Iterator<Tuple<Processor, IngestMetric>> oldMetricsIterator = oldPerProcessMetrics.iterator();
                        for (Tuple<Processor, IngestMetric> compositeMetric : newPerProcessMetrics) {
                            String type = compositeMetric.v1().getType();
                            IngestMetric metric = compositeMetric.v2();
                            if (oldMetricsIterator.hasNext()) {
                                Tuple<Processor, IngestMetric> oldCompositeMetric = oldMetricsIterator.next();
                                String oldType = oldCompositeMetric.v1().getType();
                                IngestMetric oldMetric = oldCompositeMetric.v2();
                                if (type.equals(oldType)) {
                                    metric.add(oldMetric);
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    private static List<Tuple<Processor, IngestMetric>> getProcessorMetrics(CompoundProcessor compoundProcessor, List<Tuple<Processor, IngestMetric>> processorMetrics) {
        for (Tuple<Processor, IngestMetric> processorWithMetric : compoundProcessor.getProcessorsWithMetrics()) {
            Processor processor = processorWithMetric.v1();
            IngestMetric metric = processorWithMetric.v2();
            if (processor instanceof CompoundProcessor) {
                getProcessorMetrics((CompoundProcessor) processor, processorMetrics);
            } else {
                if (processor instanceof ConditionalProcessor) {
                    metric = ((ConditionalProcessor) processor).getMetric();
                }
                processorMetrics.add(new Tuple<>(processor, metric));
            }
        }
        return processorMetrics;
    }

    private static Pipeline substitutePipeline(String id, ElasticsearchParseException e) {
        String tag = e.getHeaderKeys().contains("processor_tag") ? e.getHeader("processor_tag").get(0) : null;
        String type = e.getHeaderKeys().contains("processor_type") ? e.getHeader("processor_type").get(0) : "unknown";
        String errorMessage = "pipeline with id [" + id + "] could not be loaded, caused by [" + e.getDetailedMessage() + "]";
        Processor failureProcessor = new AbstractProcessor(tag) {

            @Override
            public IngestDocument execute(IngestDocument ingestDocument) {
                throw new IllegalStateException(errorMessage);
            }

            @Override
            public String getType() {
                return type;
            }
        };
        String description = "this is a place holder pipeline, because pipeline with id [" + id + "] could not be loaded";
        return new Pipeline(id, description, null, new CompoundProcessor(failureProcessor));
    }

    static ClusterState innerPut(PutPipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        Map<String, PipelineConfiguration> pipelines;
        if (currentIngestMetadata != null) {
            pipelines = new HashMap<>(currentIngestMetadata.getPipelines());
        } else {
            pipelines = new HashMap<>();
        }
        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource(), request.getXContentType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines)).build());
        return newState.build();
    }

    void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
        if (ingestInfos.isEmpty()) {
            throw new IllegalStateException("Ingest info is empty");
        }
        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        Pipeline pipeline = Pipeline.create(request.getId(), pipelineConfig, processorFactories, scriptService);
        List<Exception> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                String type = processor.getType();
                if (entry.getValue().containsProcessor(type) == false && ConditionalProcessor.TYPE.equals(type) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message));
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public void executeBulkRequest(Iterable<DocWriteRequest<?>> actionRequests, BiConsumer<IndexRequest, Exception> itemFailureHandler, Consumer<Exception> completionHandler, Consumer<IndexRequest> itemDroppedHandler) {
        threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                completionHandler.accept(e);
            }

            @Override
            protected void doRun() {
                for (DocWriteRequest<?> actionRequest : actionRequests) {
                    IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(actionRequest);
                    if (indexRequest == null) {
                        continue;
                    }
                    String pipelineId = indexRequest.getPipeline();
                    if (NOOP_PIPELINE_NAME.equals(pipelineId) == false) {
                        try {
                            Pipeline pipeline = pipelines.get(pipelineId);
                            if (pipeline == null) {
                                throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
                            }
                            innerExecute(indexRequest, pipeline, itemDroppedHandler);
                            indexRequest.setPipeline(NOOP_PIPELINE_NAME);
                        } catch (Exception e) {
                            itemFailureHandler.accept(indexRequest, e);
                        }
                    }
                }
                completionHandler.accept(null);
            }
        });
    }

    public IngestStats stats() {
        IngestStats.Builder statsBuilder = new IngestStats.Builder();
        statsBuilder.addTotalMetrics(totalMetrics);
        pipelines.forEach((id, pipeline) -> {
            CompoundProcessor rootProcessor = pipeline.getCompoundProcessor();
            statsBuilder.addPipelineMetrics(id, pipeline.getMetrics());
            List<Tuple<Processor, IngestMetric>> processorMetrics = new ArrayList<>();
            getProcessorMetrics(rootProcessor, processorMetrics);
            processorMetrics.forEach(t -> {
                Processor processor = t.v1();
                IngestMetric processorMetric = t.v2();
                statsBuilder.addProcessorMetrics(id, getProcessorName(processor), processorMetric);
            });
        });
        return statsBuilder.build();
    }

    static String getProcessorName(Processor processor) {
        if (processor instanceof ConditionalProcessor) {
            processor = ((ConditionalProcessor) processor).getProcessor();
        }
        StringBuilder sb = new StringBuilder(5);
        sb.append(processor.getType());
        if (processor instanceof PipelineProcessor) {
            String pipelineName = ((PipelineProcessor) processor).getPipelineName();
            sb.append(":");
            sb.append(pipelineName);
        }
        String tag = processor.getTag();
        if (tag != null && !tag.isEmpty()) {
            sb.append(":");
            sb.append(tag);
        }
        return sb.toString();
    }

    private void innerExecute(IndexRequest indexRequest, Pipeline pipeline, Consumer<IndexRequest> itemDroppedHandler) throws Exception {
        if (pipeline.getProcessors().isEmpty()) {
            return;
        }
        long startTimeInNanos = System.nanoTime();
        try {
            totalMetrics.preIngest();
            String index = indexRequest.index();
            String type = indexRequest.type();
            String id = indexRequest.id();
            String routing = indexRequest.routing();
            Long version = indexRequest.version();
            VersionType versionType = indexRequest.versionType();
            Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
            IngestDocument ingestDocument = new IngestDocument(index, type, id, routing, version, versionType, sourceAsMap);
            if (pipeline.execute(ingestDocument) == null) {
                itemDroppedHandler.accept(indexRequest);
            } else {
                Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.extractMetadata();
                indexRequest.index((String) metadataMap.get(IngestDocument.MetaData.INDEX));
                indexRequest.type((String) metadataMap.get(IngestDocument.MetaData.TYPE));
                indexRequest.id((String) metadataMap.get(IngestDocument.MetaData.ID));
                indexRequest.routing((String) metadataMap.get(IngestDocument.MetaData.ROUTING));
                indexRequest.version(((Number) metadataMap.get(IngestDocument.MetaData.VERSION)).longValue());
                if (metadataMap.get(IngestDocument.MetaData.VERSION_TYPE) != null) {
                    indexRequest.versionType(VersionType.fromString((String) metadataMap.get(IngestDocument.MetaData.VERSION_TYPE)));
                }
                indexRequest.source(ingestDocument.getSourceAndMetadata());
            }
        } catch (Exception e) {
            totalMetrics.ingestFailed();
            throw e;
        } finally {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
            totalMetrics.postIngest(ingestTimeInMillis);
        }
    }

    private void innerUpdatePipelines(ClusterState previousState, ClusterState state) {
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        IngestMetadata previousIngestMetadata = previousState.getMetaData().custom(IngestMetadata.TYPE);
        if (Objects.equals(ingestMetadata, previousIngestMetadata)) {
            return;
        }
        Map<String, Pipeline> pipelines = new HashMap<>();
        List<ElasticsearchParseException> exceptions = new ArrayList<>();
        for (PipelineConfiguration pipeline : ingestMetadata.getPipelines().values()) {
            try {
                pipelines.put(pipeline.getId(), Pipeline.create(pipeline.getId(), pipeline.getConfigAsMap(), processorFactories, scriptService));
            } catch (ElasticsearchParseException e) {
                pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), e));
                exceptions.add(e);
            } catch (Exception e) {
                ElasticsearchParseException parseException = new ElasticsearchParseException("Error updating pipeline with id [" + pipeline.getId() + "]", e);
                pipelines.put(pipeline.getId(), substitutePipeline(pipeline.getId(), parseException));
                exceptions.add(parseException);
            }
        }
        this.pipelines = Collections.unmodifiableMap(pipelines);
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }
}