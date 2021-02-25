package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ingest.SimulateProcessorResult;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

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
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        if (actualProcessor instanceof PipelineProcessor) {
            PipelineProcessor pipelineProcessor = ((PipelineProcessor) actualProcessor);
            Pipeline pipeline = pipelineProcessor.getPipeline();
            IngestDocument ingestDocumentCopy = new IngestDocument(ingestDocument);
            ingestDocumentCopy.executePipeline(pipelineProcessor.getPipeline(), (result, e) -> {
                if (e instanceof ElasticsearchException) {
                    ElasticsearchException elasticsearchException = (ElasticsearchException) e;
                    if (elasticsearchException.getCause().getCause() instanceof IllegalStateException) {
                        if (ignoreFailure) {
                            processorResultList.add(new SimulateProcessorResult(pipelineProcessor.getTag(), new IngestDocument(ingestDocument), e));
                        } else {
                            processorResultList.add(new SimulateProcessorResult(pipelineProcessor.getTag(), e));
                        }
                        handler.accept(null, elasticsearchException);
                    }
                } else {
                    CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), processorResultList);
                    Pipeline verbosePipeline = new Pipeline(pipeline.getId(), pipeline.getDescription(), pipeline.getVersion(), verbosePipelineProcessor);
                    ingestDocument.executePipeline(verbosePipeline, handler);
                }
            });
            return;
        }
        final Processor processor;
        if (actualProcessor instanceof ConditionalProcessor) {
            ConditionalProcessor conditionalProcessor = (ConditionalProcessor) actualProcessor;
            if (conditionalProcessor.evaluate(ingestDocument) == false) {
                handler.accept(ingestDocument, null);
                return;
            }
            if (conditionalProcessor.getProcessor() instanceof PipelineProcessor) {
                processor = conditionalProcessor.getProcessor();
            } else {
                processor = actualProcessor;
            }
        } else {
            processor = actualProcessor;
        }
        processor.execute(ingestDocument, (result, e) -> {
            if (e != null) {
                if (ignoreFailure) {
                    processorResultList.add(new SimulateProcessorResult(processor.getTag(), new IngestDocument(ingestDocument), e));
                } else {
                    processorResultList.add(new SimulateProcessorResult(processor.getTag(), e));
                }
                handler.accept(null, e);
            } else {
                processorResultList.add(new SimulateProcessorResult(processor.getTag(), new IngestDocument(ingestDocument)));
                handler.accept(result, null);
            }
        });
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException();
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