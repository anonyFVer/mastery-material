package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.DirectoryUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.OldIndexUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InternalEngineTests extends ESTestCase {

    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 1);

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    protected ThreadPool threadPool;

    private Store store;

    private Store storeReplica;

    protected InternalEngine engine;

    protected InternalEngine replicaEngine;

    private IndexSettings defaultSettings;

    private String codecName;

    private Path primaryTranslogDir;

    private Path replicaTranslogDir;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        CodecService codecService = new CodecService(null, logger);
        String name = Codec.getDefault().getName();
        if (Arrays.asList(codecService.availableCodecs()).contains(name)) {
            codecName = name;
        } else {
            codecName = "default";
        }
        defaultSettings = IndexSettingsModule.newIndexSettings("test", Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h").put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(), between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY))).build());
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
        storeReplica = createStore();
        Lucene.cleanLuceneIndex(store.directory());
        Lucene.cleanLuceneIndex(storeReplica.directory());
        primaryTranslogDir = createTempDir("translog-primary");
        engine = createEngine(store, primaryTranslogDir);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();
        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
        replicaTranslogDir = createTempDir("translog-replica");
        replicaEngine = createEngine(storeReplica, replicaTranslogDir);
        currentIndexWriterConfig = replicaEngine.getCurrentIndexWriterConfig();
        assertEquals(replicaEngine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
    }

    public EngineConfig copy(EngineConfig config, EngineConfig.OpenMode openMode) {
        return new EngineConfig(openMode, config.getShardId(), config.getThreadPool(), config.getIndexSettings(), config.getWarmer(), config.getStore(), config.getDeletionPolicy(), config.getMergePolicy(), config.getAnalyzer(), config.getSimilarity(), new CodecService(null, logger), config.getEventListener(), config.getTranslogRecoveryPerformer(), config.getQueryCache(), config.getQueryCachingPolicy(), config.getTranslogConfig(), config.getFlushMergesAfter(), config.getRefreshListeners(), config.getMaxUnsafeAutoIdTimestamp());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(replicaEngine, storeReplica, engine, store);
        terminate(threadPool);
    }

    private Document testDocumentWithTextField() {
        Document document = testDocument();
        document.add(new TextField("value", "test", Field.Store.YES));
        return document;
    }

    private Document testDocument() {
        return new Document();
    }

    private ParsedDocument testParsedDocument(String uid, String id, String type, String routing, long timestamp, long ttl, Document document, BytesReference source, Mapping mappingUpdate) {
        Field uidField = new Field("_uid", uid, UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        document.add(uidField);
        document.add(versionField);
        return new ParsedDocument(versionField, id, type, routing, timestamp, ttl, Arrays.asList(document), source, mappingUpdate);
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        final DirectoryService directoryService = new DirectoryService(shardId, INDEX_SETTINGS) {

            @Override
            public Directory newDirectory() throws IOException {
                return directory;
            }

            @Override
            public long throttleTimeInNanos() {
                return 0;
            }
        };
        return new Store(shardId, INDEX_SETTINGS, directoryService, new DummyShardLock(shardId));
    }

    protected Translog createTranslog() throws IOException {
        return createTranslog(primaryTranslogDir);
    }

    protected Translog createTranslog(Path translogPath) throws IOException {
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE);
        return new Translog(translogConfig, null);
    }

    protected SnapshotDeletionPolicy createSnapshotDeletionPolicy() {
        return new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    }

    protected InternalEngine createEngine(Store store, Path translogPath) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null);
    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, null);
    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, Supplier<IndexWriter> indexWriterSupplier) throws IOException {
        EngineConfig config = config(indexSettings, store, translogPath, mergePolicy, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null);
        InternalEngine internalEngine = new InternalEngine(config) {

            @Override
            IndexWriter createWriter(boolean create) throws IOException {
                return (indexWriterSupplier != null) ? indexWriterSupplier.get() : super.createWriter(create);
            }
        };
        if (config.getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            internalEngine.recoverFromTranslog();
        }
        return internalEngine;
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, long maxUnsafeAutoIdTimestamp, ReferenceManager.RefreshListener refreshListener) {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        final EngineConfig.OpenMode openMode;
        try {
            if (Lucene.indexExists(store.directory()) == false) {
                openMode = EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG;
            } else {
                openMode = EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
            }
        } catch (IOException e) {
            throw new ElasticsearchException("can't find index?", e);
        }
        Engine.EventListener listener = new Engine.EventListener() {

            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
            }
        };
        EngineConfig config = new EngineConfig(openMode, shardId, threadPool, indexSettings, null, store, createSnapshotDeletionPolicy(), mergePolicy, iwc.getAnalyzer(), iwc.getSimilarity(), new CodecService(null, logger), listener, new TranslogHandler(shardId.getIndexName(), logger), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(), translogConfig, TimeValue.timeValueMinutes(5), refreshListener, maxUnsafeAutoIdTimestamp);
        return config;
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[] { 1 });

    protected static final BytesReference B_2 = new BytesArray(new byte[] { 2 });

    protected static final BytesReference B_3 = new BytesArray(new byte[] { 3 });

    public void testSegments() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(false);
            assertThat(segments.isEmpty(), equalTo(true));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(0L));
            assertThat(engine.segmentsStats(false).getMemoryInBytes(), equalTo(0L));
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            Engine.Index first = new Engine.Index(newUid("1"), doc);
            Engine.IndexResult firstResult = engine.index(first);
            ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
            Engine.Index second = new Engine.Index(newUid("2"), doc2);
            Engine.IndexResult secondResult = engine.index(second);
            assertThat(secondResult.getTranslogLocation(), greaterThan(firstResult.getTranslogLocation()));
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            SegmentsStats stats = engine.segmentsStats(false);
            assertThat(stats.getCount(), equalTo(1L));
            assertThat(stats.getTermsMemoryInBytes(), greaterThan(0L));
            assertThat(stats.getStoredFieldsMemoryInBytes(), greaterThan(0L));
            assertThat(stats.getTermVectorsMemoryInBytes(), equalTo(0L));
            assertThat(stats.getNormsMemoryInBytes(), greaterThan(0L));
            assertThat(stats.getDocValuesMemoryInBytes(), greaterThan(0L));
            assertThat(segments.get(0).isCommitted(), equalTo(false));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));
            assertThat(segments.get(0).ramTree, nullValue());
            engine.flush();
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(1L));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));
            ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
            engine.index(new Engine.Index(newUid("3"), doc3));
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(2L));
            assertThat(engine.segmentsStats(false).getTermsMemoryInBytes(), greaterThan(stats.getTermsMemoryInBytes()));
            assertThat(engine.segmentsStats(false).getStoredFieldsMemoryInBytes(), greaterThan(stats.getStoredFieldsMemoryInBytes()));
            assertThat(engine.segmentsStats(false).getTermVectorsMemoryInBytes(), equalTo(0L));
            assertThat(engine.segmentsStats(false).getNormsMemoryInBytes(), greaterThan(stats.getNormsMemoryInBytes()));
            assertThat(engine.segmentsStats(false).getDocValuesMemoryInBytes(), greaterThan(stats.getDocValuesMemoryInBytes()));
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));
            assertThat(segments.get(1).isCommitted(), equalTo(false));
            assertThat(segments.get(1).isSearch(), equalTo(true));
            assertThat(segments.get(1).getNumDocs(), equalTo(1));
            assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(1).isCompound(), equalTo(true));
            engine.delete(new Engine.Delete("test", "1", newUid("1")));
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(2L));
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(1));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
            assertThat(segments.get(0).isCompound(), equalTo(true));
            assertThat(segments.get(1).isCommitted(), equalTo(false));
            assertThat(segments.get(1).isSearch(), equalTo(true));
            assertThat(segments.get(1).getNumDocs(), equalTo(1));
            assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(1).isCompound(), equalTo(true));
            engine.onSettingsChanged();
            ParsedDocument doc4 = testParsedDocument("4", "4", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
            engine.index(new Engine.Index(newUid("4"), doc4));
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(3));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(3L));
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(1));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
            assertThat(segments.get(0).isCompound(), equalTo(true));
            assertThat(segments.get(1).isCommitted(), equalTo(false));
            assertThat(segments.get(1).isSearch(), equalTo(true));
            assertThat(segments.get(1).getNumDocs(), equalTo(1));
            assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(1).isCompound(), equalTo(true));
            assertThat(segments.get(2).isCommitted(), equalTo(false));
            assertThat(segments.get(2).isSearch(), equalTo(true));
            assertThat(segments.get(2).getNumDocs(), equalTo(1));
            assertThat(segments.get(2).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(2).isCompound(), equalTo(true));
        }
    }

    public void testVerboseSegments() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(true);
            assertThat(segments.isEmpty(), equalTo(true));
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(new Engine.Index(newUid("1"), doc));
            engine.refresh("test");
            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(1));
            assertThat(segments.get(0).ramTree, notNullValue());
            ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
            engine.index(new Engine.Index(newUid("2"), doc2));
            engine.refresh("test");
            ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
            engine.index(new Engine.Index(newUid("3"), doc3));
            engine.refresh("test");
            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(3));
            assertThat(segments.get(0).ramTree, notNullValue());
            assertThat(segments.get(1).ramTree, notNullValue());
            assertThat(segments.get(2).ramTree, notNullValue());
        }
    }

    public void testSegmentsWithMergeFlag() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), new TieredMergePolicy())) {
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
            Engine.Index index = new Engine.Index(newUid("1"), doc);
            engine.index(index);
            engine.flush();
            assertThat(engine.segments(false).size(), equalTo(1));
            index = new Engine.Index(newUid("2"), doc);
            engine.index(index);
            engine.flush();
            List<Segment> segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }
            index = new Engine.Index(newUid("3"), doc);
            engine.index(index);
            engine.flush();
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(3));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }
            index = new Engine.Index(newUid("4"), doc);
            engine.index(index);
            engine.flush();
            final long gen1 = store.readLastCommittedSegmentsInfo().getGeneration();
            engine.forceMerge(true);
            for (Segment segment : engine.segments(false)) {
                assertThat(segment.getMergeId(), nullValue());
            }
            assertTrue(store.readLastCommittedSegmentsInfo().getGeneration() > gen1);
            final boolean flush = randomBoolean();
            final long gen2 = store.readLastCommittedSegmentsInfo().getGeneration();
            engine.forceMerge(flush);
            for (Segment segment : engine.segments(false)) {
                assertThat(segment.getMergeId(), nullValue());
            }
            if (flush) {
                assertEquals(gen2, store.readLastCommittedSegmentsInfo().getLastGeneration());
            }
        }
    }

    public void testSegmentsStatsIncludingFileSizes() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            assertThat(engine.segmentsStats(true).getFileSizes().size(), equalTo(0));
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(new Engine.Index(newUid("1"), doc));
            engine.refresh("test");
            SegmentsStats stats = engine.segmentsStats(true);
            assertThat(stats.getFileSizes().size(), greaterThan(0));
            assertThat(() -> stats.getFileSizes().valuesIt(), everyItem(greaterThan(0L)));
            ObjectObjectCursor<String, Long> firstEntry = stats.getFileSizes().iterator().next();
            ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
            engine.index(new Engine.Index(newUid("2"), doc2));
            engine.refresh("test");
            assertThat(engine.segmentsStats(true).getFileSizes().get(firstEntry.key), greaterThan(firstEntry.value));
        }
    }

    public void testCommitStats() {
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        CommitStats stats1 = engine.commitStats();
        assertThat(stats1.getGeneration(), greaterThan(0L));
        assertThat(stats1.getId(), notNullValue());
        assertThat(stats1.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
        engine.flush(true, true);
        CommitStats stats2 = engine.commitStats();
        assertThat(stats2.getGeneration(), greaterThan(stats1.getGeneration()));
        assertThat(stats2.getId(), notNullValue());
        assertThat(stats2.getId(), not(equalTo(stats1.getId())));
        assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
        assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_UUID_KEY));
        assertThat(stats2.getUserData().get(Translog.TRANSLOG_GENERATION_KEY), not(equalTo(stats1.getUserData().get(Translog.TRANSLOG_GENERATION_KEY))));
        assertThat(stats2.getUserData().get(Translog.TRANSLOG_UUID_KEY), equalTo(stats1.getUserData().get(Translog.TRANSLOG_UUID_KEY)));
    }

    public void testIndexSearcherWrapper() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {

            @Override
            public DirectoryReader wrap(DirectoryReader reader) {
                counter.incrementAndGet();
                return reader;
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                counter.incrementAndGet();
                return searcher;
            }
        };
        Store store = createStore();
        Path translog = createTempDir("translog-test");
        InternalEngine engine = createEngine(store, translog);
        engine.close();
        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        assertTrue(engine.isRecovering());
        engine.recoverFromTranslog();
        Engine.Searcher searcher = wrapper.wrap(engine.acquireSearcher("test"));
        assertThat(counter.get(), equalTo(2));
        searcher.close();
        IOUtils.close(store, engine);
    }

    public void testFlushIsDisabledDuringTranslogRecovery() throws IOException {
        assertFalse(engine.isRecovering());
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        engine.close();
        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        expectThrows(IllegalStateException.class, () -> engine.flush(true, true));
        assertTrue(engine.isRecovering());
        engine.recoverFromTranslog();
        assertFalse(engine.isRecovering());
        doc = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(new Engine.Index(newUid("2"), doc));
        engine.flush();
    }

    public void testTranslogMultipleOperationsSameDocument() throws IOException {
        final int ops = randomIntBetween(1, 32);
        Engine initialEngine;
        final List<Engine.Operation> operations = new ArrayList<>();
        try {
            initialEngine = engine;
            for (int i = 0; i < ops; i++) {
                final ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
                if (randomBoolean()) {
                    final Engine.Index operation = new Engine.Index(newUid("test#1"), doc, i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
                    operations.add(operation);
                    initialEngine.index(operation);
                } else {
                    final Engine.Delete operation = new Engine.Delete("test", "1", newUid("test#1"), i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime());
                    operations.add(operation);
                    initialEngine.delete(operation);
                }
            }
        } finally {
            IOUtils.close(engine);
        }
        Engine recoveringEngine = null;
        try {
            recoveringEngine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
            recoveringEngine.recoverFromTranslog();
            try (Engine.Searcher searcher = recoveringEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new MatchAllDocsQuery(), collector);
                assertThat(collector.getTotalHits(), equalTo(operations.get(operations.size() - 1) instanceof Engine.Delete ? 0 : 1));
            }
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void testTranslogRecoveryDoesNotReplayIntoTranslog() throws IOException {
        final int docs = randomIntBetween(1, 32);
        Engine initialEngine = null;
        try {
            initialEngine = engine;
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, id, "test", null, -1, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
                initialEngine.index(new Engine.Index(newUid(id), doc));
            }
        } finally {
            IOUtils.close(initialEngine);
        }
        Engine recoveringEngine = null;
        try {
            final AtomicBoolean flushed = new AtomicBoolean();
            recoveringEngine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG)) {

                @Override
                public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
                    assertThat(getTranslog().totalOperations(), equalTo(docs));
                    final CommitId commitId = super.flush(force, waitIfOngoing);
                    flushed.set(true);
                    return commitId;
                }
            };
            assertThat(recoveringEngine.getTranslog().totalOperations(), equalTo(docs));
            recoveringEngine.recoverFromTranslog();
            assertTrue(flushed.get());
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void testConcurrentGetAndFlush() throws Exception {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        final AtomicReference<Engine.GetResult> latestGetResult = new AtomicReference<>();
        latestGetResult.set(engine.get(new Engine.Get(true, newUid("1"))));
        final AtomicBoolean flushFinished = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        Thread getThread = new Thread() {

            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                while (flushFinished.get() == false) {
                    Engine.GetResult previousGetResult = latestGetResult.get();
                    if (previousGetResult != null) {
                        previousGetResult.release();
                    }
                    latestGetResult.set(engine.get(new Engine.Get(true, newUid("1"))));
                    if (latestGetResult.get().exists() == false) {
                        break;
                    }
                }
            }
        };
        getThread.start();
        barrier.await();
        engine.flush();
        flushFinished.set(true);
        getThread.join();
        assertTrue(latestGetResult.get().exists());
        latestGetResult.get().release();
    }

    public void testSimpleOperations() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.close();
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();
        Engine.GetResult getResult = engine.get(new Engine.Get(false, newUid("1")));
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();
        getResult = engine.get(new Engine.Get(false, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_2), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_2, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();
        engine.delete(new Engine.Delete("test", "1", newUid("1")));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc, Versions.MATCH_DELETED));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();
        engine.flush();
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();
    }

    public void testSearchResultRelease() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.close();
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        engine.delete(new Engine.Delete("test", "1", newUid("1")));
        engine.refresh("test");
        Engine.Searcher updateSearchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(updateSearchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        updateSearchResult.close();
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();
    }

    public void testSyncedFlush() throws IOException {
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), new LogByteSizeMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(new Engine.Index(newUid("1"), doc));
            Engine.CommitId commitID = engine.flush();
            assertThat(commitID, equalTo(new Engine.CommitId(store.readLastCommittedSegmentsInfo().getId())));
            byte[] wrongBytes = Base64.getDecoder().decode(commitID.toString());
            wrongBytes[0] = (byte) ~wrongBytes[0];
            Engine.CommitId wrongId = new Engine.CommitId(wrongBytes);
            assertEquals("should fail to sync flush with wrong id (but no docs)", engine.syncFlush(syncId + "1", wrongId), Engine.SyncedFlushResult.COMMIT_MISMATCH);
            engine.index(new Engine.Index(newUid("2"), doc));
            assertEquals("should fail to sync flush with right id but pending doc", engine.syncFlush(syncId + "2", commitID), Engine.SyncedFlushResult.PENDING_OPERATIONS);
            commitID = engine.flush();
            assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID), Engine.SyncedFlushResult.SUCCESS);
            assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
            assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        }
    }

    public void testRenewSyncFlush() throws Exception {
        final int iters = randomIntBetween(2, 5);
        for (int i = 0; i < iters; i++) {
            try (Store store = createStore();
                InternalEngine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), new LogDocMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
                final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
                ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
                Engine.Index doc1 = new Engine.Index(newUid("1"), doc);
                engine.index(doc1);
                assertEquals(engine.getLastWriteNanos(), doc1.startTime());
                engine.flush();
                Engine.Index doc2 = new Engine.Index(newUid("2"), doc);
                engine.index(doc2);
                assertEquals(engine.getLastWriteNanos(), doc2.startTime());
                engine.flush();
                final boolean forceMergeFlushes = randomBoolean();
                if (forceMergeFlushes) {
                    engine.index(new Engine.Index(newUid("3"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime() - engine.engineConfig.getFlushMergesAfter().nanos(), -1, false));
                } else {
                    engine.index(new Engine.Index(newUid("3"), doc));
                }
                Engine.CommitId commitID = engine.flush();
                assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID), Engine.SyncedFlushResult.SUCCESS);
                assertEquals(3, engine.segments(false).size());
                engine.forceMerge(forceMergeFlushes, 1, false, false, false);
                if (forceMergeFlushes == false) {
                    engine.refresh("make all segments visible");
                    assertEquals(4, engine.segments(false).size());
                    assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                    assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                    assertTrue(engine.tryRenewSyncCommit());
                    assertEquals(1, engine.segments(false).size());
                } else {
                    assertBusy(() -> assertEquals(1, engine.segments(false).size()));
                }
                assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                if (randomBoolean()) {
                    Engine.Index doc4 = new Engine.Index(newUid("4"), doc);
                    engine.index(doc4);
                    assertEquals(engine.getLastWriteNanos(), doc4.startTime());
                } else {
                    Engine.Delete delete = new Engine.Delete(doc1.type(), doc1.id(), doc1.uid());
                    engine.delete(delete);
                    assertEquals(engine.getLastWriteNanos(), delete.startTime());
                }
                assertFalse(engine.tryRenewSyncCommit());
                engine.flush(false, true);
                assertNull(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID));
                assertNull(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
            }
        }
    }

    public void testSycnedFlushSurvivesEngineRestart() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        final Engine.CommitId commitID = engine.flush();
        assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID), Engine.SyncedFlushResult.SUCCESS);
        assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        EngineConfig config = engine.config();
        if (randomBoolean()) {
            engine.close();
        } else {
            engine.flushAndClose();
        }
        engine = new InternalEngine(copy(config, randomFrom(EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG)));
        if (engine.config().getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG && randomBoolean()) {
            engine.recoverFromTranslog();
        }
        assertEquals(engine.config().getOpenMode().toString(), engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
    }

    public void testSycnedFlushVanishesOnReplay() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(new Engine.Index(newUid("1"), doc));
        final Engine.CommitId commitID = engine.flush();
        assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID), Engine.SyncedFlushResult.SUCCESS);
        assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        doc = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), new BytesArray("{}"), null);
        engine.index(new Engine.Index(newUid("2"), doc));
        EngineConfig config = engine.config();
        engine.close();
        engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        engine.recoverFromTranslog();
        assertNull("Sync ID must be gone since we have a document to replay", engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    public void testVersioningNewCreate() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid("1"), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        create = new Engine.Index(newUid("1"), doc, indexResult.getVersion(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
    }

    public void testVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc, indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
    }

    public void testExternalVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(12L));
        index = new Engine.Index(newUid("1"), doc, indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(12L));
    }

    public void testVersioningIndexConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        index = new Engine.Index(newUid("1"), doc, 1L, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
        index = new Engine.Index(newUid("1"), doc, 3L, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testExternalVersioningIndexConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(12L));
        index = new Engine.Index(newUid("1"), doc, 14, VersionType.EXTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(14L));
        index = new Engine.Index(newUid("1"), doc, 13, VersionType.EXTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testForceVersioningNotAllowedExceptForOlderIndices() throws Exception {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc, 42, VersionType.FORCE, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(IllegalArgumentException.class));
        assertThat(indexResult.getFailure().getMessage(), containsString("version type [FORCE] may not be used for indices created after 6.0"));
        IndexSettings oldIndexSettings = IndexSettingsModule.newIndexSettings("test", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_0_0_beta1).build());
        try (Store store = createStore();
            Engine engine = createEngine(oldIndexSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            index = new Engine.Index(newUid("1"), doc, 84, VersionType.FORCE, PRIMARY, 0, -1, false);
            Engine.IndexResult result = engine.index(index);
            assertTrue(result.hasFailure());
            assertThat(result.getFailure(), instanceOf(IllegalArgumentException.class));
            assertThat(result.getFailure().getMessage(), containsString("version type [FORCE] may not be used for non-translog operations"));
            index = new Engine.Index(newUid("1"), doc, 84, VersionType.FORCE, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY, 0, -1, false);
            result = engine.index(index);
            assertThat(result.getVersion(), equalTo(84L));
        }
    }

    public void testVersioningIndexConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        engine.flush();
        index = new Engine.Index(newUid("1"), doc, 1L, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
        index = new Engine.Index(newUid("1"), doc, 3L, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testExternalVersioningIndexConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(12L));
        index = new Engine.Index(newUid("1"), doc, 14, VersionType.EXTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(14L));
        engine.flush();
        index = new Engine.Index(newUid("1"), doc, 13, VersionType.EXTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testForceMerge() throws IOException {
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), new LogByteSizeMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            int numDocs = randomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), B_1, null);
                Engine.Index index = new Engine.Index(newUid(Integer.toString(i)), doc);
                engine.index(index);
                engine.refresh("test");
            }
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs, test.reader().numDocs());
            }
            engine.forceMerge(true, 1, false, false, false);
            assertEquals(engine.segments(true).size(), 1);
            ParsedDocument doc = testParsedDocument(Integer.toString(0), Integer.toString(0), "test", null, -1, -1, testDocument(), B_1, null);
            Engine.Index index = new Engine.Index(newUid(Integer.toString(0)), doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, true, false, false);
            assertEquals(engine.segments(true).size(), 1);
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs - 1, test.reader().numDocs());
                assertEquals(engine.config().getMergePolicy().toString(), numDocs - 1, test.reader().maxDoc());
            }
            doc = testParsedDocument(Integer.toString(1), Integer.toString(1), "test", null, -1, -1, testDocument(), B_1, null);
            index = new Engine.Index(newUid(Integer.toString(1)), doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, false, false, false);
            assertEquals(engine.segments(true).size(), 1);
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs - 2, test.reader().numDocs());
                assertEquals(numDocs - 1, test.reader().maxDoc());
            }
        }
    }

    public void testForceMergeAndClose() throws IOException, InterruptedException {
        int numIters = randomIntBetween(2, 10);
        for (int j = 0; j < numIters; j++) {
            try (Store store = createStore()) {
                final InternalEngine engine = createEngine(store, createTempDir());
                final CountDownLatch startGun = new CountDownLatch(1);
                final CountDownLatch indexed = new CountDownLatch(1);
                Thread thread = new Thread() {

                    @Override
                    public void run() {
                        try {
                            try {
                                startGun.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            int i = 0;
                            while (true) {
                                int numDocs = randomIntBetween(1, 20);
                                for (int j = 0; j < numDocs; j++) {
                                    i++;
                                    ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), B_1, null);
                                    Engine.Index index = new Engine.Index(newUid(Integer.toString(i)), doc);
                                    engine.index(index);
                                }
                                engine.refresh("test");
                                indexed.countDown();
                                try {
                                    engine.forceMerge(randomBoolean(), 1, false, randomBoolean(), randomBoolean());
                                } catch (IOException e) {
                                    return;
                                }
                            }
                        } catch (AlreadyClosedException | EngineClosedException ex) {
                        }
                    }
                };
                thread.start();
                startGun.countDown();
                int someIters = randomIntBetween(1, 10);
                for (int i = 0; i < someIters; i++) {
                    engine.forceMerge(randomBoolean(), 1, false, randomBoolean(), randomBoolean());
                }
                indexed.await();
                IOUtils.close(engine);
                thread.join();
            }
        }
    }

    public void testVersioningDeleteConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1"), 1L, VersionType.INTERNAL, PRIMARY, 0);
        Engine.DeleteResult result = engine.delete(delete);
        assertTrue(result.hasFailure());
        assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
        delete = new Engine.Delete("test", "1", newUid("1"), 3L, VersionType.INTERNAL, PRIMARY, 0);
        result = engine.delete(delete);
        assertTrue(result.hasFailure());
        assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
        delete = new Engine.Delete("test", "1", newUid("1"), 2L, VersionType.INTERNAL, PRIMARY, 0);
        result = engine.delete(delete);
        assertThat(result.getVersion(), equalTo(3L));
        index = new Engine.Index(newUid("1"), doc, 2L, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testVersioningDeleteConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        engine.flush();
        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1"), 1L, VersionType.INTERNAL, PRIMARY, 0);
        Engine.DeleteResult deleteResult = engine.delete(delete);
        assertTrue(deleteResult.hasFailure());
        assertThat(deleteResult.getFailure(), instanceOf(VersionConflictEngineException.class));
        delete = new Engine.Delete("test", "1", newUid("1"), 3L, VersionType.INTERNAL, PRIMARY, 0);
        deleteResult = engine.delete(delete);
        assertTrue(deleteResult.hasFailure());
        assertThat(deleteResult.getFailure(), instanceOf(VersionConflictEngineException.class));
        engine.flush();
        delete = new Engine.Delete("test", "1", newUid("1"), 2L, VersionType.INTERNAL, PRIMARY, 0);
        deleteResult = engine.delete(delete);
        assertThat(deleteResult.getVersion(), equalTo(3L));
        engine.flush();
        index = new Engine.Index(newUid("1"), doc, 2L, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testVersioningCreateExistsException() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid("1"), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        create = new Engine.Index(newUid("1"), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(create);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testVersioningCreateExistsExceptionWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid("1"), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        engine.flush();
        create = new Engine.Index(newUid("1"), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(create);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testVersioningReplicaConflict1() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        index = new Engine.Index(newUid("1"), doc, indexResult.getVersion(), VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        index = new Engine.Index(newUid("1"), doc, 1L, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
        index = new Engine.Index(newUid("1"), doc, 2L, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testVersioningReplicaConflict2() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc, 1L, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(2L));
        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1"));
        Engine.DeleteResult deleteResult = engine.delete(delete);
        assertThat(deleteResult.getVersion(), equalTo(3L));
        delete = new Engine.Delete("test", "1", newUid("1"), 3L, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0);
        deleteResult = replicaEngine.delete(delete);
        assertThat(deleteResult.getVersion(), equalTo(3L));
        delete = new Engine.Delete("test", "1", newUid("1"), 3L, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0);
        deleteResult = replicaEngine.delete(delete);
        assertTrue(deleteResult.hasFailure());
        assertThat(deleteResult.getFailure(), instanceOf(VersionConflictEngineException.class));
        index = new Engine.Index(newUid("1"), doc, 2L, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    public void testBasicCreatedFlag() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertFalse(indexResult.isCreated());
        engine.delete(new Engine.Delete(null, "1", newUid("1")));
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
    }

    public void testCreatedFlagAfterFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(newUid("1"), doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
        engine.delete(new Engine.Delete(null, "1", newUid("1")));
        engine.flush();
        index = new Engine.Index(newUid("1"), doc);
        indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
    }

    private static class MockAppender extends AbstractAppender {

        public boolean sawIndexWriterMessage;

        public boolean sawIndexWriterIFDMessage;

        public MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.TRACE && event.getMarker().getName().contains("[index][1] ")) {
                if (event.getLoggerName().endsWith(".IW") && formattedMessage.contains("IW: apply all deletes during flush")) {
                    sawIndexWriterMessage = true;
                }
                if (event.getLoggerName().endsWith(".IFD")) {
                    sawIndexWriterIFDMessage = true;
                }
            }
        }
    }

    public void testIndexWriterInfoStream() throws IllegalAccessException {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterInfoStream");
        mockAppender.start();
        Logger rootLogger = LogManager.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        Loggers.addAppender(rootLogger, mockAppender);
        Loggers.setLevel(rootLogger, Level.DEBUG);
        rootLogger = LogManager.getRootLogger();
        try {
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(new Engine.Index(newUid("1"), doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            Loggers.setLevel(rootLogger, Level.TRACE);
            engine.index(new Engine.Index(newUid("2"), doc));
            engine.flush();
            assertTrue(mockAppender.sawIndexWriterMessage);
        } finally {
            Loggers.removeAppender(rootLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(rootLogger, savedLevel);
        }
    }

    public void testIndexWriterIFDInfoStream() throws IllegalAccessException {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterIFDInfoStream");
        mockAppender.start();
        final Logger iwIFDLogger = Loggers.getLogger("org.elasticsearch.index.engine.Engine.IFD");
        Loggers.addAppender(iwIFDLogger, mockAppender);
        Loggers.setLevel(iwIFDLogger, Level.DEBUG);
        try {
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(new Engine.Index(newUid("1"), doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            assertFalse(mockAppender.sawIndexWriterIFDMessage);
            Loggers.setLevel(iwIFDLogger, Level.TRACE);
            engine.index(new Engine.Index(newUid("2"), doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            assertTrue(mockAppender.sawIndexWriterIFDMessage);
        } finally {
            Loggers.removeAppender(iwIFDLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(iwIFDLogger, (Level) null);
        }
    }

    public void testEnableGcDeletes() throws Exception {
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            engine.config().setEnableGcDeletes(false);
            Document document = testDocument();
            document.add(new TextField("value", "test1", Field.Store.YES));
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_2, null);
            engine.index(new Engine.Index(newUid("1"), doc, 1, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false));
            engine.delete(new Engine.Delete("test", "1", newUid("1"), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));
            Engine.GetResult getResult = engine.get(new Engine.Get(true, newUid("1")));
            assertThat(getResult.exists(), equalTo(false));
            Thread.sleep(1000);
            if (randomBoolean()) {
                engine.refresh("test");
            }
            engine.delete(new Engine.Delete("test", "2", newUid("2"), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));
            getResult = engine.get(new Engine.Get(true, newUid("2")));
            assertThat(getResult.exists(), equalTo(false));
            Engine.Index index = new Engine.Index(newUid("1"), doc, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(index);
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
            getResult = engine.get(new Engine.Get(true, newUid("1")));
            assertThat(getResult.exists(), equalTo(false));
            Engine.Index index1 = new Engine.Index(newUid("2"), doc, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
            indexResult = engine.index(index1);
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
            getResult = engine.get(new Engine.Get(true, newUid("2")));
            assertThat(getResult.exists(), equalTo(false));
        }
    }

    protected Term newUid(String id) {
        return new Term("_uid", id);
    }

    public void testExtractShardId() {
        try (Engine.Searcher test = this.engine.acquireSearcher("test")) {
            ShardId shardId = ShardUtils.extractShardId(test.getDirectoryReader());
            assertNotNull(shardId);
            assertEquals(shardId, engine.config().getShardId());
        }
    }

    public void testFailStart() throws IOException {
        final int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            MockDirectoryWrapper wrapper = newMockDirectory();
            wrapper.setFailOnOpenInput(randomBoolean());
            wrapper.setAllowRandomFileNotFoundException(randomBoolean());
            wrapper.setRandomIOExceptionRate(randomDouble());
            wrapper.setRandomIOExceptionRateOnOpen(randomDouble());
            final Path translogPath = createTempDir("testFailStart");
            try (Store store = createStore(wrapper)) {
                int refCount = store.refCount();
                assertTrue("refCount: " + store.refCount(), store.refCount() > 0);
                InternalEngine holder;
                try {
                    holder = createEngine(store, translogPath);
                } catch (EngineCreationFailureException ex) {
                    assertEquals(store.refCount(), refCount);
                    continue;
                }
                assertEquals(store.refCount(), refCount + 1);
                final int numStarts = scaledRandomIntBetween(1, 5);
                for (int j = 0; j < numStarts; j++) {
                    try {
                        assertEquals(store.refCount(), refCount + 1);
                        holder.close();
                        holder = createEngine(store, translogPath);
                        assertEquals(store.refCount(), refCount + 1);
                    } catch (EngineCreationFailureException ex) {
                        assertEquals(store.refCount(), refCount);
                        break;
                    }
                }
                holder.close();
                assertEquals(store.refCount(), refCount);
            }
        }
    }

    public void testSettings() {
        CodecService codecService = new CodecService(null, logger);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();
        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
    }

    public void testMissingTranslog() throws IOException {
        engine.close();
        Translog translog = createTranslog();
        long id = translog.currentFileGeneration();
        translog.close();
        IOUtils.rm(translog.location().resolve(Translog.getFilename(id)));
        try {
            engine = createEngine(store, primaryTranslogDir);
            fail("engine shouldn't start without a valid translog id");
        } catch (EngineCreationFailureException ex) {
        }
        EngineConfig config = copy(config(defaultSettings, store, primaryTranslogDir, newMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null), EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG);
        engine = new InternalEngine(config);
    }

    public void testTranslogReplayWithFailure() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(i)), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        engine.close();
        final MockDirectoryWrapper directory = DirectoryUtils.getLeaf(store.directory(), MockDirectoryWrapper.class);
        if (directory != null) {
            boolean started = false;
            final int numIters = randomIntBetween(10, 20);
            for (int i = 0; i < numIters; i++) {
                directory.setRandomIOExceptionRateOnOpen(randomDouble());
                directory.setRandomIOExceptionRate(randomDouble());
                directory.setFailOnOpenInput(randomBoolean());
                directory.setAllowRandomFileNotFoundException(randomBoolean());
                try {
                    engine = createEngine(store, primaryTranslogDir);
                    started = true;
                    break;
                } catch (EngineException | IOException e) {
                }
            }
            directory.setRandomIOExceptionRateOnOpen(0.0);
            directory.setRandomIOExceptionRate(0.0);
            directory.setFailOnOpenInput(false);
            directory.setAllowRandomFileNotFoundException(false);
            if (started == false) {
                engine = createEngine(store, primaryTranslogDir);
            }
        } else {
            engine = createEngine(store, primaryTranslogDir);
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
    }

    public void testSkipTranslogReplay() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(i)), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        engine.close();
        engine = new InternalEngine(engine.config());
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(0));
        }
    }

    private Mapping dynamicUpdate() {
        BuilderContext context = new BuilderContext(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build(), new ContentPath());
        final RootObjectMapper root = new RootObjectMapper.Builder("some_type").build(context);
        return new Mapping(Version.CURRENT, root, new MetadataFieldMapper[0], emptyMap());
    }

    public void testUpgradeOldIndex() throws IOException {
        List<Path> indexes = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getBwcIndicesPath(), "index-*.zip")) {
            for (Path path : stream) {
                indexes.add(path);
            }
        }
        Collections.shuffle(indexes, random());
        for (Path indexFile : indexes.subList(0, scaledRandomIntBetween(1, indexes.size() / 2))) {
            final String indexName = indexFile.getFileName().toString().replace(".zip", "").toLowerCase(Locale.ROOT);
            Path unzipDir = createTempDir();
            Path unzipDataDir = unzipDir.resolve("data");
            try (InputStream stream = Files.newInputStream(indexFile)) {
                TestUtil.unzip(stream, unzipDir);
            }
            assertTrue(Files.exists(unzipDataDir));
            Path[] list = filterExtraFSFiles(FileSystemUtils.files(unzipDataDir));
            if (list.length != 1) {
                throw new IllegalStateException("Backwards index must contain exactly one cluster but was " + list.length + " " + Arrays.toString(list));
            }
            Path src = OldIndexUtils.getIndexDir(logger, indexName, indexFile.toString(), list[0]);
            Path translog = src.resolve("0").resolve("translog");
            assertTrue("[" + indexFile + "] missing translog dir: " + translog.toString(), Files.exists(translog));
            Path[] tlogFiles = filterExtraFSFiles(FileSystemUtils.files(translog));
            assertEquals(Arrays.toString(tlogFiles), tlogFiles.length, 2);
            Path tlogFile = tlogFiles[0].getFileName().toString().endsWith("tlog") ? tlogFiles[0] : tlogFiles[1];
            final long size = Files.size(tlogFile);
            logger.debug("upgrading index {} file: {} size: {}", indexName, tlogFiles[0].getFileName(), size);
            Directory directory = newFSDirectory(src.resolve("0").resolve("index"));
            Store store = createStore(directory);
            final int iters = randomIntBetween(0, 2);
            int numDocs = -1;
            for (int i = 0; i < iters; i++) {
                try (InternalEngine engine = createEngine(store, translog)) {
                    try (Searcher searcher = engine.acquireSearcher("test")) {
                        if (i > 0) {
                            assertEquals(numDocs, searcher.reader().numDocs());
                        }
                        TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 1);
                        numDocs = searcher.reader().numDocs();
                        assertTrue(search.totalHits > 1);
                    }
                    CommitStats commitStats = engine.commitStats();
                    Map<String, String> userData = commitStats.getUserData();
                    assertTrue("userdata dosn't contain uuid", userData.containsKey(Translog.TRANSLOG_UUID_KEY));
                    assertTrue("userdata doesn't contain generation key", userData.containsKey(Translog.TRANSLOG_GENERATION_KEY));
                    assertFalse("userdata contains legacy marker", userData.containsKey("translog_id"));
                }
            }
            try (InternalEngine engine = createEngine(store, translog)) {
                if (numDocs == -1) {
                    try (Searcher searcher = engine.acquireSearcher("test")) {
                        numDocs = searcher.reader().numDocs();
                    }
                }
                final int numExtraDocs = randomIntBetween(1, 10);
                for (int i = 0; i < numExtraDocs; i++) {
                    ParsedDocument doc = testParsedDocument("extra" + Integer.toString(i), "extra" + Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
                    Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(i)), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
                    Engine.IndexResult indexResult = engine.index(firstIndexRequest);
                    assertThat(indexResult.getVersion(), equalTo(1L));
                }
                engine.refresh("test");
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + numExtraDocs));
                    assertThat(topDocs.totalHits, equalTo(numDocs + numExtraDocs));
                }
            }
            IOUtils.close(store, directory);
        }
    }

    private Path[] filterExtraFSFiles(Path[] files) {
        List<Path> paths = new ArrayList<>();
        for (Path p : files) {
            if (p.getFileName().toString().startsWith("extra")) {
                continue;
            }
            paths.add(p);
        }
        return paths.toArray(new Path[0]);
    }

    public void testTranslogReplay() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(i)), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        TranslogHandler parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        parser.mappingUpdate = dynamicUpdate();
        engine.close();
        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        engine.recoverFromTranslog();
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(numDocs, parser.recoveredOps.get());
        if (parser.mappingUpdate != null) {
            assertEquals(1, parser.getRecoveredTypes().size());
            assertTrue(parser.getRecoveredTypes().containsKey("test"));
        } else {
            assertEquals(0, parser.getRecoveredTypes().size());
        }
        engine.close();
        engine = createEngine(store, primaryTranslogDir);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(0, parser.recoveredOps.get());
        final boolean flush = randomBoolean();
        int randomId = randomIntBetween(numDocs + 1, numDocs + 10);
        String uuidValue = "test#" + Integer.toString(randomId);
        ParsedDocument doc = testParsedDocument(uuidValue, Integer.toString(randomId), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
        Engine.Index firstIndexRequest = new Engine.Index(newUid(uuidValue), doc, 1, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult indexResult = engine.index(firstIndexRequest);
        assertThat(indexResult.getVersion(), equalTo(1L));
        if (flush) {
            engine.flush();
        }
        doc = testParsedDocument(uuidValue, Integer.toString(randomId), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
        Engine.Index idxRequest = new Engine.Index(newUid(uuidValue), doc, 2, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult result = engine.index(idxRequest);
        engine.refresh("test");
        assertThat(result.getVersion(), equalTo(2L));
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1));
        }
        engine.close();
        engine = createEngine(store, primaryTranslogDir);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(flush ? 1 : 2, parser.recoveredOps.get());
        engine.delete(new Engine.Delete("test", Integer.toString(randomId), newUid(uuidValue)));
        if (randomBoolean()) {
            engine.refresh("test");
        } else {
            engine.close();
            engine = createEngine(store, primaryTranslogDir);
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs);
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
    }

    public static class TranslogHandler extends TranslogRecoveryPerformer {

        private final MapperService mapperService;

        public Mapping mappingUpdate = null;

        public final AtomicInteger recoveredOps = new AtomicInteger(0);

        public TranslogHandler(String indexName, Logger logger) {
            super(new ShardId("test", "_na_", 0), null, logger);
            Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
            Index index = new Index(indexName, "_na_");
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);
            IndexAnalyzers indexAnalyzers = null;
            NamedAnalyzer defaultAnalyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
            indexAnalyzers = new IndexAnalyzers(indexSettings, defaultAnalyzer, defaultAnalyzer, defaultAnalyzer, Collections.emptyMap());
            SimilarityService similarityService = new SimilarityService(indexSettings, Collections.emptyMap());
            MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
            mapperService = new MapperService(indexSettings, indexAnalyzers, similarityService, mapperRegistry, () -> null);
        }

        @Override
        protected DocumentMapperForType docMapper(String type) {
            RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder(type);
            DocumentMapper.Builder b = new DocumentMapper.Builder(rootBuilder, mapperService);
            return new DocumentMapperForType(b.build(mapperService), mappingUpdate);
        }

        @Override
        protected void operationProcessed() {
            recoveredOps.incrementAndGet();
        }
    }

    public void testRecoverFromForeignTranslog() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(i)), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion(), equalTo(1L));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        Translog.TranslogGeneration generation = engine.getTranslog().getGeneration();
        engine.close();
        Translog translog = new Translog(new TranslogConfig(shardId, createTempDir(), INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE), null);
        translog.add(new Translog.Index("test", "SomeBogusId", "{}".getBytes(Charset.forName("UTF-8"))));
        assertEquals(generation.translogFileGeneration, translog.currentFileGeneration());
        translog.close();
        EngineConfig config = engine.config();
        TranslogConfig translogConfig = new TranslogConfig(shardId, translog.location(), config.getIndexSettings(), BigArrays.NON_RECYCLING_INSTANCE);
        EngineConfig brokenConfig = new EngineConfig(EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, shardId, threadPool, config.getIndexSettings(), null, store, createSnapshotDeletionPolicy(), newMergePolicy(), config.getAnalyzer(), config.getSimilarity(), new CodecService(null, logger), config.getEventListener(), config.getTranslogRecoveryPerformer(), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(), translogConfig, TimeValue.timeValueMinutes(5), config.getRefreshListeners(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP);
        try {
            InternalEngine internalEngine = new InternalEngine(brokenConfig);
            fail("translog belongs to a different engine");
        } catch (EngineCreationFailureException ex) {
        }
        engine = createEngine(store, primaryTranslogDir);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
    }

    public void testShardNotAvailableExceptionWhenEngineClosedConcurrently() throws IOException, InterruptedException {
        AtomicReference<Exception> exception = new AtomicReference<>();
        String operation = randomFrom("optimize", "refresh", "flush");
        Thread mergeThread = new Thread() {

            @Override
            public void run() {
                boolean stop = false;
                logger.info("try with {}", operation);
                while (stop == false) {
                    try {
                        switch(operation) {
                            case "optimize":
                                {
                                    engine.forceMerge(true, 1, false, false, false);
                                    break;
                                }
                            case "refresh":
                                {
                                    engine.refresh("test refresh");
                                    break;
                                }
                            case "flush":
                                {
                                    engine.flush(true, false);
                                    break;
                                }
                        }
                    } catch (Exception e) {
                        exception.set(e);
                        stop = true;
                    }
                }
            }
        };
        mergeThread.start();
        engine.close();
        mergeThread.join();
        logger.info("exception caught: ", exception.get());
        assertTrue("expected an Exception that signals shard is not available", TransportActions.isShardNotAvailableException(exception.get()));
    }

    public void testCurrentTranslogIDisCommitted() throws IOException {
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null);
            {
                ParsedDocument doc = testParsedDocument(Integer.toString(0), Integer.toString(0), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
                Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(0)), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
                try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG))) {
                    assertFalse(engine.isRecovering());
                    engine.index(firstIndexRequest);
                    expectThrows(IllegalStateException.class, () -> engine.recoverFromTranslog());
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                    assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                }
            }
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG))) {
                        assertTrue(engine.isRecovering());
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        if (i == 0) {
                            assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        } else {
                            assertEquals("3", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        }
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                        engine.recoverFromTranslog();
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("3", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    }
                }
            }
            {
                try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG))) {
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                    assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    expectThrows(IllegalStateException.class, () -> engine.recoverFromTranslog());
                }
            }
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG))) {
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                        engine.recoverFromTranslog();
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("no changes - nothing to commit", "1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    }
                }
            }
        }
    }

    public void testCheckDocumentFailure() throws Exception {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        Exception documentFailure = engine.checkIfDocumentFailureOrThrow(new Engine.Index(newUid("1"), doc), new IOException("simulated document failure"));
        assertThat(documentFailure, instanceOf(IOException.class));
        try {
            engine.checkIfDocumentFailureOrThrow(new Engine.Index(newUid("1"), doc), new CorruptIndexException("simulated environment failure", ""));
            fail("expected exception to be thrown");
        } catch (Exception envirnomentException) {
            assertThat(envirnomentException.getMessage(), containsString("simulated environment failure"));
        }
    }

    private static class ThrowingIndexWriter extends IndexWriter {

        private boolean throwDocumentFailure;

        public ThrowingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

        @Override
        public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
            if (throwDocumentFailure) {
                throw new IOException("simulated");
            } else {
                return super.addDocument(doc);
            }
        }

        @Override
        public long deleteDocuments(Term... terms) throws IOException {
            if (throwDocumentFailure) {
                throw new IOException("simulated");
            } else {
                return super.deleteDocuments(terms);
            }
        }

        public void setThrowDocumentFailure(boolean throwDocumentFailure) {
            this.throwDocumentFailure = throwDocumentFailure;
        }
    }

    public void testHandleDocumentFailure() throws Exception {
        try (Store store = createStore()) {
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            ThrowingIndexWriter throwingIndexWriter = new ThrowingIndexWriter(store.directory(), new IndexWriterConfig());
            try (Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, () -> throwingIndexWriter)) {
                throwingIndexWriter.setThrowDocumentFailure(true);
                Engine.IndexResult indexResult = engine.index(randomAppendOnly(1, doc, false));
                assertNotNull(indexResult.getFailure());
                throwingIndexWriter.setThrowDocumentFailure(false);
                indexResult = engine.index(randomAppendOnly(1, doc, false));
                assertNull(indexResult.getFailure());
                throwingIndexWriter.setThrowDocumentFailure(true);
                Engine.DeleteResult deleteResult = engine.delete(new Engine.Delete("test", "", newUid("1")));
                assertNotNull(deleteResult.getFailure());
            }
        }
    }

    public void testDocStats() throws IOException {
        final int numDocs = randomIntBetween(2, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(i)), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        DocsStats docStats = engine.getDocStats();
        assertEquals(numDocs, docStats.getCount());
        assertEquals(0, docStats.getDeleted());
        engine.forceMerge(randomBoolean(), 1, false, false, false);
        ParsedDocument doc = testParsedDocument(Integer.toString(0), Integer.toString(0), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
        Engine.Index firstIndexRequest = new Engine.Index(newUid(Integer.toString(0)), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult index = engine.index(firstIndexRequest);
        assertThat(index.getVersion(), equalTo(2L));
        engine.flush();
        docStats = engine.getDocStats();
        assertEquals(1, docStats.getDeleted());
        assertEquals(numDocs, docStats.getCount());
        engine.forceMerge(randomBoolean(), 1, false, false, false);
        docStats = engine.getDocStats();
        assertEquals(0, docStats.getDeleted());
        assertEquals(numDocs, docStats.getCount());
    }

    public void testDoubleDelivery() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", "1", "test", null, 100, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = randomAppendOnly(1, doc, false);
        Engine.Index retry = randomAppendOnly(1, doc, true);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            Engine.IndexResult retryResult = engine.index(retry);
            assertTrue(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertTrue(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertTrue(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        operation = randomAppendOnly(1, doc, false);
        retry = randomAppendOnly(1, doc, true);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertNotNull(indexResult.getTranslogLocation());
            Engine.IndexResult retryResult = engine.index(retry);
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertNotNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testRetryWithAutogeneratedIdWorksAndNoDuplicateDocs() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", "1", "test", null, 100, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = false;
        long autoGeneratedIdTimestamp = 0;
        Engine.Index index = new Engine.Index(newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid("1"), doc, indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        isRetry = true;
        index = new Engine.Index(newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        index = new Engine.Index(newUid("1"), doc, indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.hasFailure(), equalTo(false));
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testRetryWithAutogeneratedIdsAndWrongOrderWorksAndNoDuplicateDocs() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", "1", "test", null, 100, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = true;
        long autoGeneratedIdTimestamp = 0;
        Engine.Index firstIndexRequest = new Engine.Index(newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult result = engine.index(firstIndexRequest);
        assertThat(result.getVersion(), equalTo(1L));
        Engine.Index firstIndexRequestReplica = new Engine.Index(newUid("1"), doc, result.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexReplicaResult = replicaEngine.index(firstIndexRequestReplica);
        assertThat(indexReplicaResult.getVersion(), equalTo(1L));
        isRetry = false;
        Engine.Index secondIndexRequest = new Engine.Index(newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(secondIndexRequest);
        assertTrue(indexResult.isCreated());
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        Engine.Index secondIndexRequestReplica = new Engine.Index(newUid("1"), doc, result.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        replicaEngine.index(secondIndexRequestReplica);
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public Engine.Index randomAppendOnly(int docId, ParsedDocument doc, boolean retry) {
        if (randomBoolean()) {
            return new Engine.Index(newUid(Integer.toString(docId)), doc, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), docId, retry);
        }
        return new Engine.Index(newUid(Integer.toString(docId)), doc, 1, VersionType.EXTERNAL, Engine.Operation.Origin.REPLICA, System.nanoTime(), docId, retry);
    }

    public void testRetryConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        List<Engine.Index> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, i, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            Engine.Index originalIndex = randomAppendOnly(i, doc, false);
            Engine.Index retryIndex = randomAppendOnly(i, doc, true);
            docs.add(originalIndex);
            docs.add(retryIndex);
        }
        Collections.shuffle(docs, random());
        CountDownLatch startGun = new CountDownLatch(thread.length);
        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {

                @Override
                public void run() {
                    startGun.countDown();
                    try {
                        startGun.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    int docOffset;
                    while ((docOffset = offset.incrementAndGet()) < docs.size()) {
                        engine.index(docs.get(docOffset));
                    }
                }
            };
            thread[i].start();
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        assertEquals(0, engine.getNumVersionLookups());
        assertEquals(0, engine.getNumIndexVersionsLookups());
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(numDocs, topDocs.totalHits);
        }
        assertTrue(engine.indexWriterHasDeletions());
    }

    public void testEngineMaxTimestampIsInitialized() throws IOException {
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            assertEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
        long maxTimestamp = Math.abs(randomLong());
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, maxTimestamp, null))) {
            assertEquals(maxTimestamp, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
    }

    public void testAppendConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        assertEquals(0, engine.getNumVersionLookups());
        assertEquals(0, engine.getNumIndexVersionsLookups());
        List<Engine.Index> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, i, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            Engine.Index index = randomAppendOnly(i, doc, false);
            docs.add(index);
        }
        Collections.shuffle(docs, random());
        CountDownLatch startGun = new CountDownLatch(thread.length);
        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {

                @Override
                public void run() {
                    startGun.countDown();
                    try {
                        startGun.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    int docOffset;
                    while ((docOffset = offset.incrementAndGet()) < docs.size()) {
                        engine.index(docs.get(docOffset));
                    }
                }
            };
            thread[i].start();
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(docs.size(), topDocs.totalHits);
        }
        assertEquals(0, engine.getNumVersionLookups());
        assertEquals(0, engine.getNumIndexVersionsLookups());
        assertFalse(engine.indexWriterHasDeletions());
    }

    public static long getNumVersionLookups(InternalEngine engine) {
        return engine.getNumVersionLookups();
    }

    public static long getNumIndexVersionsLookups(InternalEngine engine) {
        return engine.getNumIndexVersionsLookups();
    }

    public void testFailEngineOnRandomIO() throws IOException, InterruptedException {
        MockDirectoryWrapper wrapper = newMockDirectory();
        final Path translogPath = createTempDir("testFailEngineOnRandomIO");
        try (Store store = createStore(wrapper)) {
            CyclicBarrier join = new CyclicBarrier(2);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger controller = new AtomicInteger(0);
            EngineConfig config = config(defaultSettings, store, translogPath, newMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, new ReferenceManager.RefreshListener() {

                @Override
                public void beforeRefresh() throws IOException {
                }

                @Override
                public void afterRefresh(boolean didRefresh) throws IOException {
                    int i = controller.incrementAndGet();
                    if (i == 1) {
                        throw new MockDirectoryWrapper.FakeIOException();
                    } else if (i == 2) {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        throw new AlreadyClosedException("boom");
                    }
                }
            });
            InternalEngine internalEngine = new InternalEngine(config);
            int docId = 0;
            final ParsedDocument doc = testParsedDocument(Integer.toString(docId), Integer.toString(docId), "test", null, docId, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            Engine.Index index = randomAppendOnly(docId, doc, false);
            internalEngine.index(index);
            Runnable r = () -> {
                try {
                    join.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                try {
                    internalEngine.refresh("test");
                    fail();
                } catch (EngineClosedException ex) {
                    assertTrue(ex.toString(), ex.getCause() instanceof MockDirectoryWrapper.FakeIOException);
                } catch (RefreshFailedEngineException | AlreadyClosedException ex) {
                } finally {
                    start.countDown();
                }
            };
            Thread t = new Thread(r);
            Thread t1 = new Thread(r);
            t.start();
            t1.start();
            t.join();
            t1.join();
            assertTrue(internalEngine.isClosed.get());
            assertTrue(internalEngine.failedEngine.get() instanceof MockDirectoryWrapper.FakeIOException);
        }
    }
}