package org.elasticsearch.index;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.cache.query.IndexQueryCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.similarity.BM25SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public final class IndexModule {

    public static final Setting<String> INDEX_STORE_TYPE_SETTING = new Setting<>("index.store.type", "", Function.identity(), Property.IndexScope, Property.NodeScope);

    public static final Setting<List<String>> INDEX_STORE_PRE_LOAD_SETTING = Setting.listSetting("index.store.preload", Collections.emptyList(), Function.identity(), Property.IndexScope, Property.NodeScope);

    public static final String SIMILARITY_SETTINGS_PREFIX = "index.similarity";

    public static final Setting<Boolean> INDEX_QUERY_CACHE_ENABLED_SETTING = Setting.boolSetting("index.queries.cache.enabled", true, Property.IndexScope);

    public static final Setting<Boolean> INDEX_QUERY_CACHE_EVERYTHING_SETTING = Setting.boolSetting("index.queries.cache.everything", false, Property.IndexScope);

    private final IndexSettings indexSettings;

    private final IndexStoreConfig indexStoreConfig;

    private final AnalysisRegistry analysisRegistry;

    final SetOnce<EngineFactory> engineFactory = new SetOnce<>();

    private SetOnce<IndexSearcherWrapperFactory> indexSearcherWrapper = new SetOnce<>();

    private final Set<IndexEventListener> indexEventListeners = new HashSet<>();

    private final Map<String, BiFunction<String, Settings, SimilarityProvider>> similarities = new HashMap<>();

    private final Map<String, BiFunction<IndexSettings, IndexStoreConfig, IndexStore>> storeTypes = new HashMap<>();

    private final SetOnce<BiFunction<IndexSettings, IndicesQueryCache, QueryCache>> forceQueryCacheProvider = new SetOnce<>();

    private final List<SearchOperationListener> searchOperationListeners = new ArrayList<>();

    private final List<IndexingOperationListener> indexOperationListeners = new ArrayList<>();

    private final AtomicBoolean frozen = new AtomicBoolean(false);

    public IndexModule(IndexSettings indexSettings, IndexStoreConfig indexStoreConfig, AnalysisRegistry analysisRegistry) {
        this.indexStoreConfig = indexStoreConfig;
        this.indexSettings = indexSettings;
        this.analysisRegistry = analysisRegistry;
        this.searchOperationListeners.add(new SearchSlowLog(indexSettings));
        this.indexOperationListeners.add(new IndexingSlowLog(indexSettings));
    }

    public <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
        ensureNotFrozen();
        if (setting == null) {
            throw new IllegalArgumentException("setting must not be null");
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(setting, consumer);
    }

    public <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Consumer<T> validator) {
        ensureNotFrozen();
        if (setting == null) {
            throw new IllegalArgumentException("setting must not be null");
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(setting, consumer, validator);
    }

    public Settings getSettings() {
        return indexSettings.getSettings();
    }

    public Index getIndex() {
        return indexSettings.getIndex();
    }

    public void addIndexEventListener(IndexEventListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexEventListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }
        this.indexEventListeners.add(listener);
    }

    public void addSearchOperationListener(SearchOperationListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (searchOperationListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }
        this.searchOperationListeners.add(listener);
    }

    public void addIndexOperationListener(IndexingOperationListener listener) {
        ensureNotFrozen();
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (indexOperationListeners.contains(listener)) {
            throw new IllegalArgumentException("listener already added");
        }
        this.indexOperationListeners.add(listener);
    }

    public void addIndexStore(String type, BiFunction<IndexSettings, IndexStoreConfig, IndexStore> provider) {
        ensureNotFrozen();
        if (storeTypes.containsKey(type)) {
            throw new IllegalArgumentException("key [" + type + "] already registered");
        }
        storeTypes.put(type, provider);
    }

    public void addSimilarity(String name, BiFunction<String, Settings, SimilarityProvider> similarity) {
        ensureNotFrozen();
        if (similarities.containsKey(name) || SimilarityService.BUILT_IN.containsKey(name)) {
            throw new IllegalArgumentException("similarity for name: [" + name + " is already registered");
        }
        similarities.put(name, similarity);
    }

    public void setSearcherWrapper(IndexSearcherWrapperFactory indexSearcherWrapperFactory) {
        ensureNotFrozen();
        this.indexSearcherWrapper.set(indexSearcherWrapperFactory);
    }

    IndexEventListener freeze() {
        if (this.frozen.compareAndSet(false, true)) {
            return new CompositeIndexEventListener(indexSettings, indexEventListeners);
        } else {
            throw new IllegalStateException("already frozen");
        }
    }

    private static boolean isBuiltinType(String storeType) {
        for (Type type : Type.values()) {
            if (type.match(storeType)) {
                return true;
            }
        }
        return false;
    }

    public enum Type {

        NIOFS, MMAPFS, SIMPLEFS, FS, @Deprecated
        DEFAULT;

        public String getSettingsKey() {
            return this.name().toLowerCase(Locale.ROOT);
        }

        public boolean match(String setting) {
            return getSettingsKey().equals(setting);
        }
    }

    public interface IndexSearcherWrapperFactory {

        IndexSearcherWrapper newWrapper(final IndexService indexService);
    }

    public IndexService newIndexService(NodeEnvironment environment, IndexService.ShardStoreDeleter shardStoreDeleter, CircuitBreakerService circuitBreakerService, BigArrays bigArrays, ThreadPool threadPool, ScriptService scriptService, IndicesQueriesRegistry indicesQueriesRegistry, ClusterService clusterService, Client client, IndicesQueryCache indicesQueryCache, MapperRegistry mapperRegistry, IndicesFieldDataCache indicesFieldDataCache) throws IOException {
        final IndexEventListener eventListener = freeze();
        IndexSearcherWrapperFactory searcherWrapperFactory = indexSearcherWrapper.get() == null ? (shard) -> null : indexSearcherWrapper.get();
        eventListener.beforeIndexCreated(indexSettings.getIndex(), indexSettings.getSettings());
        final String storeType = indexSettings.getValue(INDEX_STORE_TYPE_SETTING);
        final IndexStore store;
        if (Strings.isEmpty(storeType) || isBuiltinType(storeType)) {
            store = new IndexStore(indexSettings, indexStoreConfig);
        } else {
            BiFunction<IndexSettings, IndexStoreConfig, IndexStore> factory = storeTypes.get(storeType);
            if (factory == null) {
                throw new IllegalArgumentException("Unknown store type [" + storeType + "]");
            }
            store = factory.apply(indexSettings, indexStoreConfig);
            if (store == null) {
                throw new IllegalStateException("store must not be null");
            }
        }
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(IndexStore.INDEX_STORE_THROTTLE_TYPE_SETTING, store::setType);
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING, store::setMaxRate);
        final QueryCache queryCache;
        if (indexSettings.getValue(INDEX_QUERY_CACHE_ENABLED_SETTING)) {
            BiFunction<IndexSettings, IndicesQueryCache, QueryCache> queryCacheProvider = forceQueryCacheProvider.get();
            if (queryCacheProvider == null) {
                queryCache = new IndexQueryCache(indexSettings, indicesQueryCache);
            } else {
                queryCache = queryCacheProvider.apply(indexSettings, indicesQueryCache);
            }
        } else {
            queryCache = new DisabledQueryCache(indexSettings);
        }
        return new IndexService(indexSettings, environment, new SimilarityService(indexSettings, similarities), shardStoreDeleter, analysisRegistry, engineFactory.get(), circuitBreakerService, bigArrays, threadPool, scriptService, indicesQueriesRegistry, clusterService, client, queryCache, store, eventListener, searcherWrapperFactory, mapperRegistry, indicesFieldDataCache, searchOperationListeners, indexOperationListeners);
    }

    public MapperService newIndexMapperService(MapperRegistry mapperRegistry) throws IOException {
        return new MapperService(indexSettings, analysisRegistry.build(indexSettings), new SimilarityService(indexSettings, similarities), mapperRegistry, () -> {
            throw new UnsupportedOperationException("no index query shard context available");
        });
    }

    public void forceQueryCacheProvider(BiFunction<IndexSettings, IndicesQueryCache, QueryCache> queryCacheProvider) {
        ensureNotFrozen();
        this.forceQueryCacheProvider.set(queryCacheProvider);
    }

    private void ensureNotFrozen() {
        if (this.frozen.get()) {
            throw new IllegalStateException("Can't modify IndexModule once the index service has been created");
        }
    }
}