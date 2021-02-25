package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;

public final class EngineConfig {

    private final ShardId shardId;

    private final String allocationId;

    private final IndexSettings indexSettings;

    private final ByteSizeValue indexingBufferSize;

    private volatile boolean enableGcDeletes = true;

    private final TimeValue flushMergesAfter;

    private final String codecName;

    private final ThreadPool threadPool;

    private final Engine.Warmer warmer;

    private final Store store;

    private final MergePolicy mergePolicy;

    private final Analyzer analyzer;

    private final Similarity similarity;

    private final CodecService codecService;

    private final Engine.EventListener eventListener;

    private final QueryCache queryCache;

    private final QueryCachingPolicy queryCachingPolicy;

    @Nullable
    private final List<ReferenceManager.RefreshListener> externalRefreshListener;

    @Nullable
    private final List<ReferenceManager.RefreshListener> internalRefreshListener;

    @Nullable
    private final Sort indexSort;

    private final TranslogRecoveryRunner translogRecoveryRunner;

    @Nullable
    private final CircuitBreakerService circuitBreakerService;

    private final LongSupplier globalCheckpointSupplier;

    private final LongSupplier primaryTermSupplier;

    public static final Setting<String> INDEX_CODEC_SETTING = new Setting<>("index.codec", "default", s -> {
        switch(s) {
            case "default":
            case "best_compression":
            case "lucene_default":
                return s;
            default:
                if (Codec.availableCodecs().contains(s) == false) {
                    throw new IllegalArgumentException("unknown value for [index.codec] must be one of [default, best_compression] but was: " + s);
                }
                return s;
        }
    }, Property.IndexScope, Property.NodeScope);

    public static final Setting<Boolean> INDEX_OPTIMIZE_AUTO_GENERATED_IDS = Setting.boolSetting("index.optimize_auto_generated_id", true, Property.IndexScope, Property.Dynamic);

    private final TranslogConfig translogConfig;

    public EngineConfig(ShardId shardId, String allocationId, ThreadPool threadPool, IndexSettings indexSettings, Engine.Warmer warmer, Store store, MergePolicy mergePolicy, Analyzer analyzer, Similarity similarity, CodecService codecService, Engine.EventListener eventListener, QueryCache queryCache, QueryCachingPolicy queryCachingPolicy, TranslogConfig translogConfig, TimeValue flushMergesAfter, List<ReferenceManager.RefreshListener> externalRefreshListener, List<ReferenceManager.RefreshListener> internalRefreshListener, Sort indexSort, TranslogRecoveryRunner translogRecoveryRunner, CircuitBreakerService circuitBreakerService, LongSupplier globalCheckpointSupplier, LongSupplier primaryTermSupplier) {
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.indexSettings = indexSettings;
        this.threadPool = threadPool;
        this.warmer = warmer == null ? (a) -> {
        } : warmer;
        this.store = store;
        this.mergePolicy = mergePolicy;
        this.analyzer = analyzer;
        this.similarity = similarity;
        this.codecService = codecService;
        this.eventListener = eventListener;
        codecName = indexSettings.getValue(INDEX_CODEC_SETTING);
        final String escapeHatchProperty = "es.index.memory.max_index_buffer_size";
        String maxBufferSize = System.getProperty(escapeHatchProperty);
        if (maxBufferSize != null) {
            indexingBufferSize = MemorySizeValue.parseBytesSizeValueOrHeapRatio(maxBufferSize, escapeHatchProperty);
        } else {
            indexingBufferSize = IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.get(indexSettings.getNodeSettings());
        }
        this.queryCache = queryCache;
        this.queryCachingPolicy = queryCachingPolicy;
        this.translogConfig = translogConfig;
        this.flushMergesAfter = flushMergesAfter;
        this.externalRefreshListener = externalRefreshListener;
        this.internalRefreshListener = internalRefreshListener;
        this.indexSort = indexSort;
        this.translogRecoveryRunner = translogRecoveryRunner;
        this.circuitBreakerService = circuitBreakerService;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.primaryTermSupplier = primaryTermSupplier;
    }

    public void setEnableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
    }

    public ByteSizeValue getIndexingBufferSize() {
        return indexingBufferSize;
    }

    public boolean isEnableGcDeletes() {
        return enableGcDeletes;
    }

    public Codec getCodec() {
        return codecService.codec(codecName);
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public Engine.Warmer getWarmer() {
        return warmer;
    }

    public Store getStore() {
        return store;
    }

    public LongSupplier getGlobalCheckpointSupplier() {
        return globalCheckpointSupplier;
    }

    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public Engine.EventListener getEventListener() {
        return eventListener;
    }

    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public Similarity getSimilarity() {
        return similarity;
    }

    public QueryCache getQueryCache() {
        return queryCache;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return queryCachingPolicy;
    }

    public TranslogConfig getTranslogConfig() {
        return translogConfig;
    }

    public TimeValue getFlushMergesAfter() {
        return flushMergesAfter;
    }

    @FunctionalInterface
    public interface TranslogRecoveryRunner {

        int run(Engine engine, Translog.Snapshot snapshot) throws IOException;
    }

    public TranslogRecoveryRunner getTranslogRecoveryRunner() {
        return translogRecoveryRunner;
    }

    public List<ReferenceManager.RefreshListener> getExternalRefreshListener() {
        return externalRefreshListener;
    }

    public List<ReferenceManager.RefreshListener> getInternalRefreshListener() {
        return internalRefreshListener;
    }

    public boolean isAutoGeneratedIDsOptimizationEnabled() {
        return indexSettings.getValue(INDEX_OPTIMIZE_AUTO_GENERATED_IDS);
    }

    public Sort getIndexSort() {
        return indexSort;
    }

    @Nullable
    public CircuitBreakerService getCircuitBreakerService() {
        return this.circuitBreakerService;
    }

    public LongSupplier getPrimaryTermSupplier() {
        return primaryTermSupplier;
    }
}