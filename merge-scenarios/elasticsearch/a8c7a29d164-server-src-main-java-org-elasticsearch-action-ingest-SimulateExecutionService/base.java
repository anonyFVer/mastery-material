package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.threadpool.ThreadPool;
import java.util.ArrayList;
import java.util.List;
import static org.elasticsearch.ingest.TrackingResultProcessor.decorate;

class SimulateExecutionService {

    private static final String THREAD_POOL_NAME = ThreadPool.Names.MANAGEMENT;

    private final ThreadPool threadPool;

    SimulateExecutionService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    SimulateDocumentResult executeDocument(Pipeline pipeline, IngestDocument ingestDocument, boolean verbose) {
        if (verbose) {
            List<SimulateProcessorResult> processorResultList = new ArrayList<>();
            CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), processorResultList);
            try {
                Pipeline verbosePipeline = new Pipeline(pipeline.getId(), pipeline.getDescription(), pipeline.getVersion(), verbosePipelineProcessor);
                ingestDocument.executePipeline(verbosePipeline);
                return new SimulateDocumentVerboseResult(processorResultList);
            } catch (Exception e) {
                return new SimulateDocumentVerboseResult(processorResultList);
            }
        } else {
            try {
                pipeline.execute(ingestDocument);
                return new SimulateDocumentBaseResult(ingestDocument);
            } catch (Exception e) {
                return new SimulateDocumentBaseResult(e);
            }
        }
    }

    public void execute(SimulatePipelineRequest.Parsed request, ActionListener<SimulatePipelineResponse> listener) {
        threadPool.executor(THREAD_POOL_NAME).execute(new ActionRunnable<SimulatePipelineResponse>(listener) {

            @Override
            protected void doRun() throws Exception {
                List<SimulateDocumentResult> responses = new ArrayList<>();
                for (IngestDocument ingestDocument : request.getDocuments()) {
                    SimulateDocumentResult response = executeDocument(request.getPipeline(), ingestDocument, request.isVerbose());
                    if (response != null) {
                        responses.add(response);
                    }
                }
                listener.onResponse(new SimulatePipelineResponse(request.getPipeline().getId(), request.isVerbose(), responses));
            }
        });
    }
}