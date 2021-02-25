package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ingest.SimulateProcessorResult;
import java.util.ArrayList;
import java.util.List;

public final class TrackingResultProcessor implements Processor {

    private final Processor actualProcessor;

    private final List<SimulateProcessorResult> processorResultList;

    private final boolean ignoreFailure;

    TrackingResultProcessor(boolean ignoreFailure, Processor actualProcessor, List<SimulateProcessorResult> processorResultList) {
        this.ignoreFailure = ignoreFailure;
        this.processorResultList = processorResultList;
        this.actualProcessor = actualProcessor;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Processor processor = actualProcessor;
        try {
            if (processor instanceof ConditionalProcessor) {
                ConditionalProcessor conditionalProcessor = (ConditionalProcessor) processor;
                if (conditionalProcessor.evaluate(ingestDocument) == false) {
                    return ingestDocument;
                }
                if (conditionalProcessor.getInnerProcessor() instanceof PipelineProcessor) {
                    processor = conditionalProcessor.getInnerProcessor();
                }
            }
            if (processor instanceof PipelineProcessor) {
                PipelineProcessor pipelineProcessor = ((PipelineProcessor) processor);
                Pipeline pipeline = pipelineProcessor.getPipeline();
                try {
                    IngestDocument ingestDocumentCopy = new IngestDocument(ingestDocument);
                    ingestDocumentCopy.executePipeline(pipelineProcessor.getPipeline());
                } catch (ElasticsearchException elasticsearchException) {
                    if (elasticsearchException.getCause().getCause() instanceof IllegalStateException) {
                        throw elasticsearchException;
                    }
                } catch (Exception e) {
                }
                CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), processorResultList);
                Pipeline verbosePipeline = new Pipeline(pipeline.getId(), pipeline.getDescription(), pipeline.getVersion(), verbosePipelineProcessor);
                ingestDocument.executePipeline(verbosePipeline);
            } else {
                IngestDocument result = processor.execute(ingestDocument);
                if (result != null) {
                    processorResultList.add(new SimulateProcessorResult(processor.getTag(), new IngestDocument(ingestDocument)));
                } else {
                    processorResultList.add(new SimulateProcessorResult(processor.getTag()));
                    return null;
                }
            }
        } catch (Exception e) {
            if (ignoreFailure) {
                processorResultList.add(new SimulateProcessorResult(processor.getTag(), new IngestDocument(ingestDocument), e));
            } else {
                processorResultList.add(new SimulateProcessorResult(processor.getTag(), e));
            }
            throw e;
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return actualProcessor.getType();
    }

    @Override
    public String getTag() {
        return actualProcessor.getTag();
    }

    public static CompoundProcessor decorate(CompoundProcessor compoundProcessor, List<SimulateProcessorResult> processorResultList) {
        List<Processor> processors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getProcessors()) {
            if (processor instanceof CompoundProcessor) {
                processors.add(decorate((CompoundProcessor) processor, processorResultList));
            } else {
                processors.add(new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, processorResultList));
            }
        }
        List<Processor> onFailureProcessors = new ArrayList<>(compoundProcessor.getProcessors().size());
        for (Processor processor : compoundProcessor.getOnFailureProcessors()) {
            if (processor instanceof CompoundProcessor) {
                onFailureProcessors.add(decorate((CompoundProcessor) processor, processorResultList));
            } else {
                onFailureProcessors.add(new TrackingResultProcessor(compoundProcessor.isIgnoreFailure(), processor, processorResultList));
            }
        }
        return new CompoundProcessor(compoundProcessor.isIgnoreFailure(), processors, onFailureProcessors);
    }
}