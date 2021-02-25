package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public final class ShadowIndexShard extends IndexShard {

    public ShadowIndexShard(ShardRouting shardRouting, IndexSettings indexSettings, ShardPath path, Store store, IndexCache indexCache, MapperService mapperService, SimilarityService similarityService, IndexFieldDataService indexFieldDataService, @Nullable EngineFactory engineFactory, IndexEventListener indexEventListener, IndexSearcherWrapper wrapper, ThreadPool threadPool, BigArrays bigArrays, Engine.Warmer engineWarmer, List<SearchOperationListener> searchOperationListeners) throws IOException {
        super(shardRouting, indexSettings, path, store, indexCache, mapperService, similarityService, indexFieldDataService, engineFactory, indexEventListener, wrapper, threadPool, bigArrays, engineWarmer, () -> {
        }, searchOperationListeners, Collections.emptyList());
    }

    @Override
    public void updateRoutingEntry(ShardRouting newRouting) throws IOException {
        if (newRouting.primary()) {
            throw new IllegalStateException("can't promote shard to primary");
        }
        super.updateRoutingEntry(newRouting);
    }

    @Override
    public MergeStats mergeStats() {
        return new MergeStats();
    }

    @Override
    public SeqNoStats seqNoStats() {
        return null;
    }

    @Override
    public boolean canIndex() {
        return false;
    }

    @Override
    protected Engine newEngine(EngineConfig config) {
        assert this.shardRouting.primary() == false;
        assert config.getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG;
        return engineFactory.newReadOnlyEngine(config);
    }

    @Override
    protected RefreshListeners buildRefreshListeners() {
        return null;
    }

    @Override
    public boolean shouldFlush() {
        return false;
    }

    @Override
    public TranslogStats translogStats() {
        return null;
    }

    @Override
    public void updateGlobalCheckpointOnReplica(long checkpoint) {
    }

    @Override
    public long getLocalCheckpoint() {
        return -1;
    }

    @Override
    public long getGlobalCheckpoint() {
        return -1;
    }

    @Override
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        throw new UnsupportedOperationException("Can't listen for a refresh on a shadow engine because it doesn't have a translog");
    }

    @Override
    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException {
        throw new UnsupportedOperationException("can't snapshot the directory as the primary may change it underneath us");
    }

    @Override
    protected void onNewEngine(Engine newEngine) {
    }
}