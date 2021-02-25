package org.elasticsearch.ingest;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.Scheduler;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

public interface Processor {

    IngestDocument execute(IngestDocument ingestDocument) throws Exception;

    String getType();

    String getTag();

    interface Factory {

        Processor create(Map<String, Processor.Factory> processorFactories, String tag, Map<String, Object> config) throws Exception;
    }

    class Parameters {

        public final Environment env;

        public final ScriptService scriptService;

        public final AnalysisRegistry analysisRegistry;

        public final ThreadContext threadContext;

        public final LongSupplier relativeTimeSupplier;

        public final IngestService ingestService;

        public final BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler;

        public Parameters(Environment env, ScriptService scriptService, AnalysisRegistry analysisRegistry, ThreadContext threadContext, LongSupplier relativeTimeSupplier, BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler, IngestService ingestService) {
            this.env = env;
            this.scriptService = scriptService;
            this.threadContext = threadContext;
            this.analysisRegistry = analysisRegistry;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
            this.ingestService = ingestService;
        }
    }
}