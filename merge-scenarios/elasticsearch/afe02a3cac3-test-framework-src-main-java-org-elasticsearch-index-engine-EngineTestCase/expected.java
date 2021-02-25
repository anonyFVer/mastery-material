package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;
import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class EngineTestCase extends ESTestCase {

    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 0);

    protected final AllocationId allocationId = AllocationId.newInitializing();

    protected static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    protected ThreadPool threadPool;

    protected Store store;

    protected Store storeReplica;

    protected InternalEngine engine;

    protected InternalEngine replicaEngine;

    protected IndexSettings defaultSettings;

    protected String codecName;

    protected Path primaryTranslogDir;

    protected Path replicaTranslogDir;

    protected AtomicLong primaryTerm = new AtomicLong();

    protected static void assertVisibleCount(Engine engine, int numDocs) throws IOException {
        assertVisibleCount(engine, numDocs, true);
    }

    protected static void assertVisibleCount(Engine engine, int numDocs, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.searcher().search(new MatchAllDocsQuery(), collector);
            assertThat(collector.getTotalHits(), equalTo(numDocs));
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomLongBetween(1, Long.MAX_VALUE));
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

    public EngineConfig copy(EngineConfig config, LongSupplier globalCheckpointSupplier) {
        return new EngineConfig(config.getShardId(), config.getAllocationId(), config.getThreadPool(), config.getIndexSettings(), config.getWarmer(), config.getStore(), config.getMergePolicy(), config.getAnalyzer(), config.getSimilarity(), new CodecService(null, logger), config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(), config.getTranslogConfig(), config.getFlushMergesAfter(), config.getExternalRefreshListener(), Collections.emptyList(), config.getIndexSort(), config.getTranslogRecoveryRunner(), config.getCircuitBreakerService(), globalCheckpointSupplier, config.getPrimaryTermSupplier(), tombstoneDocSupplier());
    }

    public EngineConfig copy(EngineConfig config, Analyzer analyzer) {
        return new EngineConfig(config.getShardId(), config.getAllocationId(), config.getThreadPool(), config.getIndexSettings(), config.getWarmer(), config.getStore(), config.getMergePolicy(), analyzer, config.getSimilarity(), new CodecService(null, logger), config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(), config.getTranslogConfig(), config.getFlushMergesAfter(), config.getExternalRefreshListener(), Collections.emptyList(), config.getIndexSort(), config.getTranslogRecoveryRunner(), config.getCircuitBreakerService(), config.getGlobalCheckpointSupplier(), config.getPrimaryTermSupplier(), config.getTombstoneDocSupplier());
    }

    public EngineConfig copy(EngineConfig config, MergePolicy mergePolicy) {
        return new EngineConfig(config.getShardId(), config.getAllocationId(), config.getThreadPool(), config.getIndexSettings(), config.getWarmer(), config.getStore(), mergePolicy, config.getAnalyzer(), config.getSimilarity(), new CodecService(null, logger), config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(), config.getTranslogConfig(), config.getFlushMergesAfter(), config.getExternalRefreshListener(), Collections.emptyList(), config.getIndexSort(), config.getTranslogRecoveryRunner(), config.getCircuitBreakerService(), config.getGlobalCheckpointSupplier(), config.getPrimaryTermSupplier(), config.getTombstoneDocSupplier());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (engine != null && engine.isClosed.get() == false) {
            engine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine, createMapperService("test"));
        }
        if (replicaEngine != null && replicaEngine.isClosed.get() == false) {
            replicaEngine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(replicaEngine, createMapperService("test"));
        }
        IOUtils.close(replicaEngine, storeReplica, engine, store);
        terminate(threadPool);
    }

    protected static ParseContext.Document testDocumentWithTextField() {
        return testDocumentWithTextField("test");
    }

    protected static ParseContext.Document testDocumentWithTextField(String value) {
        ParseContext.Document document = testDocument();
        document.add(new TextField("value", value, Field.Store.YES));
        return document;
    }

    protected static ParseContext.Document testDocument() {
        return new ParseContext.Document();
    }

    public static ParsedDocument createParsedDoc(String id, String routing) {
        return testParsedDocument(id, routing, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"), null);
    }

    public static ParsedDocument createParsedDoc(String id, String routing, boolean recoverySource) {
        return testParsedDocument(id, routing, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"), null, recoverySource);
    }

    protected static ParsedDocument testParsedDocument(String id, String routing, ParseContext.Document document, BytesReference source, Mapping mappingUpdate) {
        return testParsedDocument(id, routing, document, source, mappingUpdate, false);
    }

    protected static ParsedDocument testParsedDocument(String id, String routing, ParseContext.Document document, BytesReference source, Mapping mappingUpdate, boolean recoverySource) {
        Field uidField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesRef ref = source.toBytesRef();
        if (recoverySource) {
            document.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            document.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1));
        } else {
            document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        }
        return new ParsedDocument(versionField, seqID, id, "test", routing, Arrays.asList(document), source, XContentType.JSON, mappingUpdate);
    }

    public static EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
        return new EngineConfig.TombstoneDocSupplier() {

            @Override
            public ParsedDocument newDeleteTombstoneDoc(String type, String id) {
                final ParseContext.Document doc = new ParseContext.Document();
                Field uidField = new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
                doc.add(uidField);
                Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
                doc.add(versionField);
                SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
                doc.add(seqID.seqNo);
                doc.add(seqID.seqNoDocValue);
                doc.add(seqID.primaryTerm);
                seqID.tombstoneField.setLongValue(1);
                doc.add(seqID.tombstoneField);
                return new ParsedDocument(versionField, seqID, id, type, null, Collections.singletonList(doc), new BytesArray("{}"), XContentType.JSON, null);
            }

            @Override
            public ParsedDocument newNoopTombstoneDoc(String reason) {
                final ParseContext.Document doc = new ParseContext.Document();
                SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
                doc.add(seqID.seqNo);
                doc.add(seqID.seqNoDocValue);
                doc.add(seqID.primaryTerm);
                seqID.tombstoneField.setLongValue(1);
                doc.add(seqID.tombstoneField);
                Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
                doc.add(versionField);
                BytesRef byteRef = new BytesRef(reason);
                doc.add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
                return new ParsedDocument(versionField, seqID, null, null, null, Collections.singletonList(doc), null, XContentType.JSON, null);
            }
        };
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        return createStore(INDEX_SETTINGS, directory);
    }

    protected Store createStore(final IndexSettings indexSettings, final Directory directory) throws IOException {
        final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {

            @Override
            public Directory newDirectory() throws IOException {
                return directory;
            }
        };
        return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
    }

    protected Translog createTranslog(LongSupplier primaryTermSupplier) throws IOException {
        return createTranslog(primaryTranslogDir, primaryTermSupplier);
    }

    protected Translog createTranslog(Path translogPath, LongSupplier primaryTermSupplier) throws IOException {
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE);
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTermSupplier.getAsLong());
        return new Translog(translogConfig, translogUUID, createTranslogDeletionPolicy(INDEX_SETTINGS), () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTermSupplier);
    }

    protected InternalEngine createEngine(Store store, Path translogPath) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null);
    }

    protected InternalEngine createEngine(Store store, Path translogPath, LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(Store store, Path translogPath, BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null);
    }

    protected InternalEngine createEngine(Store store, Path translogPath, BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier, ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null, seqNoForOperation);
    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, null);
    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, @Nullable IndexWriterFactory indexWriterFactory) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, indexWriterFactory, null, null);
    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, @Nullable IndexWriterFactory indexWriterFactory, @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier, @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, indexWriterFactory, localCheckpointTrackerSupplier, null, null, globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, @Nullable IndexWriterFactory indexWriterFactory, @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier, @Nullable LongSupplier globalCheckpointSupplier, @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, null, globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, @Nullable IndexWriterFactory indexWriterFactory, @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier, @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation, @Nullable Sort indexSort, @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        EngineConfig config = config(indexSettings, store, translogPath, mergePolicy, null, indexSort, globalCheckpointSupplier);
        return createEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
    }

    protected InternalEngine createEngine(EngineConfig config) throws IOException {
        return createEngine(null, null, null, config);
    }

    private InternalEngine createEngine(@Nullable IndexWriterFactory indexWriterFactory, @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier, @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation, EngineConfig config) throws IOException {
        final Store store = config.getStore();
        final Directory directory = store.directory();
        if (Lucene.indexExists(directory) == false) {
            store.createEmpty();
            final String translogUuid = Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUuid);
        }
        InternalEngine internalEngine = createInternalEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
        internalEngine.recoverFromTranslog();
        return internalEngine;
    }

    @FunctionalInterface
    public interface IndexWriterFactory {

        IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException;
    }

    public static long generateNewSeqNo(final Engine engine) {
        assert engine instanceof InternalEngine : "expected InternalEngine, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.getLocalCheckpointTracker().generateSeqNo();
    }

    public static InternalEngine createInternalEngine(@Nullable final IndexWriterFactory indexWriterFactory, @Nullable final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier, @Nullable final ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation, final EngineConfig config) {
        if (localCheckpointTrackerSupplier == null) {
            return new InternalEngine(config) {

                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ? indexWriterFactory.createWriter(directory, iwc) : super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null ? seqNoForOperation.applyAsLong(this, operation) : super.doGenerateSeqNoForOperation(operation);
                }
            };
        } else {
            return new InternalEngine(config, localCheckpointTrackerSupplier) {

                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ? indexWriterFactory.createWriter(directory, iwc) : super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null ? seqNoForOperation.applyAsLong(this, operation) : super.doGenerateSeqNoForOperation(operation);
                }
            };
        }
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, ReferenceManager.RefreshListener refreshListener) {
        return config(indexSettings, store, translogPath, mergePolicy, refreshListener, null, () -> SequenceNumbers.NO_OPS_PERFORMED);
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy, ReferenceManager.RefreshListener refreshListener, Sort indexSort, LongSupplier globalCheckpointSupplier) {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        Engine.EventListener listener = new Engine.EventListener() {

            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
            }
        };
        final TranslogHandler handler = new TranslogHandler(xContentRegistry(), IndexSettingsModule.newIndexSettings(shardId.getIndexName(), indexSettings.getSettings()));
        final List<ReferenceManager.RefreshListener> refreshListenerList = refreshListener == null ? emptyList() : Collections.singletonList(refreshListener);
        EngineConfig config = new EngineConfig(shardId, allocationId.getId(), threadPool, indexSettings, null, store, mergePolicy, iwc.getAnalyzer(), iwc.getSimilarity(), new CodecService(null, logger), listener, IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(), translogConfig, TimeValue.timeValueMinutes(5), refreshListenerList, Collections.emptyList(), indexSort, handler, new NoneCircuitBreakerService(), globalCheckpointSupplier == null ? new ReplicationTracker(shardId, allocationId.getId(), indexSettings, SequenceNumbers.NO_OPS_PERFORMED) : globalCheckpointSupplier, primaryTerm::get, tombstoneDocSupplier());
        return config;
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[] { 1 });

    protected static final BytesReference B_2 = new BytesArray(new byte[] { 2 });

    protected static final BytesReference B_3 = new BytesArray(new byte[] { 3 });

    protected static final BytesArray SOURCE = bytesArray("{}");

    protected static BytesArray bytesArray(String string) {
        return new BytesArray(string.getBytes(Charset.defaultCharset()));
    }

    protected static Term newUid(String id) {
        return new Term("_id", Uid.encodeId(id));
    }

    protected Term newUid(ParsedDocument doc) {
        return newUid(doc.id());
    }

    protected Engine.Get newGet(boolean realtime, ParsedDocument doc) {
        return new Engine.Get(realtime, false, doc.type(), doc.id(), newUid(doc));
    }

    protected Engine.Index indexForDoc(ParsedDocument doc) {
        return new Engine.Index(newUid(doc), primaryTerm.get(), doc);
    }

    protected Engine.Index replicaIndexForDoc(ParsedDocument doc, long version, long seqNo, boolean isRetry) {
        return new Engine.Index(newUid(doc), doc, seqNo, primaryTerm.get(), version, VersionType.EXTERNAL, Engine.Operation.Origin.REPLICA, System.nanoTime(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, isRetry);
    }

    protected Engine.Delete replicaDeleteForDoc(String id, long version, long seqNo, long startTime) {
        return new Engine.Delete("test", id, newUid(id), seqNo, primaryTerm.get(), version, VersionType.EXTERNAL, Engine.Operation.Origin.REPLICA, startTime);
    }

    protected static void assertVisibleCount(InternalEngine engine, int numDocs) throws IOException {
        assertVisibleCount(engine, numDocs, true);
    }

    protected static void assertVisibleCount(InternalEngine engine, int numDocs, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.searcher().search(new MatchAllDocsQuery(), collector);
            assertThat(collector.getTotalHits(), equalTo(numDocs));
        }
    }

    public static List<Engine.Operation> generateSingleDocHistory(boolean forReplica, VersionType versionType, long primaryTerm, int minOpCount, int maxOpCount, String docId) {
        final int numOfOps = randomIntBetween(minOpCount, maxOpCount);
        final List<Engine.Operation> ops = new ArrayList<>();
        final Term id = newUid(docId);
        final int startWithSeqNo = 0;
        final String valuePrefix = (forReplica ? "r_" : "p_") + docId + "_";
        final boolean incrementTermWhenIntroducingSeqNo = randomBoolean();
        for (int i = 0; i < numOfOps; i++) {
            final Engine.Operation op;
            final long version;
            switch(versionType) {
                case INTERNAL:
                    version = forReplica ? i : Versions.MATCH_ANY;
                    break;
                case EXTERNAL:
                    version = i;
                    break;
                case EXTERNAL_GTE:
                    version = randomBoolean() ? Math.max(i - 1, 0) : i;
                    break;
                case FORCE:
                    version = randomNonNegativeLong();
                    break;
                default:
                    throw new UnsupportedOperationException("unknown version type: " + versionType);
            }
            if (randomBoolean()) {
                op = new Engine.Index(id, testParsedDocument(docId, null, testDocumentWithTextField(valuePrefix + i), B_1, null), forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO, forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm, version, forReplica ? versionType.versionTypeForReplicationAndRecovery() : versionType, forReplica ? REPLICA : PRIMARY, System.currentTimeMillis(), -1, false);
            } else {
                op = new Engine.Delete("test", docId, id, forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO, forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm, version, forReplica ? versionType.versionTypeForReplicationAndRecovery() : versionType, forReplica ? REPLICA : PRIMARY, System.currentTimeMillis());
            }
            ops.add(op);
        }
        return ops;
    }

    public static void assertOpsOnReplica(final List<Engine.Operation> ops, final InternalEngine replicaEngine, boolean shuffleOps, final Logger logger) throws IOException {
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            lastFieldValue = null;
        }
        if (shuffleOps) {
            int firstOpWithSeqNo = 0;
            while (firstOpWithSeqNo < ops.size() && ops.get(firstOpWithSeqNo).seqNo() < 0) {
                firstOpWithSeqNo++;
            }
            shuffle(ops.subList(0, firstOpWithSeqNo), random());
            shuffle(ops.subList(firstOpWithSeqNo, ops.size()), random());
        }
        boolean firstOp = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}], seq# [{}], term [{}]", op.operationType().name().charAt(0), op.version(), op.seqNo(), op.primaryTerm());
            if (op instanceof Engine.Index) {
                Engine.IndexResult result = replicaEngine.index((Engine.Index) op);
                assertThat(result.isCreated(), equalTo(firstOp));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            } else {
                Engine.DeleteResult result = replicaEngine.delete((Engine.Delete) op);
                assertThat(result.isFound(), equalTo(firstOp == false));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            if (randomBoolean()) {
                replicaEngine.refresh("test");
            }
            if (randomBoolean()) {
                replicaEngine.flush();
                replicaEngine.refresh("test");
            }
            firstOp = false;
        }
        assertVisibleCount(replicaEngine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    protected void concurrentlyApplyOps(List<Engine.Operation> ops, InternalEngine engine) throws InterruptedException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch startGun = new CountDownLatch(thread.length);
        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                int docOffset;
                while ((docOffset = offset.incrementAndGet()) < ops.size()) {
                    try {
                        final Engine.Operation op = ops.get(docOffset);
                        if (op instanceof Engine.Index) {
                            engine.index((Engine.Index) op);
                        } else if (op instanceof Engine.Delete) {
                            engine.delete((Engine.Delete) op);
                        } else {
                            engine.noOp((Engine.NoOp) op);
                        }
                        if ((docOffset + 1) % 4 == 0) {
                            engine.refresh("test");
                        }
                        if (rarely()) {
                            engine.flush();
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            });
            thread[i].start();
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
    }

    public static Set<String> getDocIds(Engine engine, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test_get_doc_ids");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test_get_doc_ids")) {
            Set<String> ids = new HashSet<>();
            for (LeafReaderContext leafContext : searcher.reader().leaves()) {
                LeafReader reader = leafContext.reader();
                Bits liveDocs = reader.getLiveDocs();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        Document uuid = reader.document(i, Collections.singleton(IdFieldMapper.NAME));
                        BytesRef binaryID = uuid.getBinaryValue(IdFieldMapper.NAME);
                        ids.add(Uid.decodeId(Arrays.copyOfRange(binaryID.bytes, binaryID.offset, binaryID.offset + binaryID.length)));
                    }
                }
            }
            return ids;
        }
    }

    public static List<Translog.Operation> readAllOperationsInLucene(Engine engine, MapperService mapper) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        long maxSeqNo = Math.max(0, engine.getLocalCheckpointTracker().getMaxSeqNo());
        try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, 0, maxSeqNo, false)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operations.add(op);
            }
        }
        return operations;
    }

    public static void assertConsistentHistoryBetweenTranslogAndLuceneIndex(Engine engine, MapperService mapper) throws IOException {
        assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine, mapper, (luceneOp, translogOp) -> assertThat(luceneOp.getSource().source, equalTo(translogOp.getSource().source)));
    }

    public static void assertConsistentHistoryBetweenTranslogAndLuceneIndex(Engine engine, MapperService mapper, BiConsumer<Translog.Operation, Translog.Operation> assertSource) throws IOException {
        if (mapper.documentMapper() == null || engine.config().getIndexSettings().isSoftDeleteEnabled() == false) {
            return;
        }
        final Map<Long, Translog.Operation> translogOps = new HashMap<>();
        try (Translog.Snapshot snapshot = engine.getTranslog().newSnapshot()) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                translogOps.put(op.seqNo(), op);
            }
        }
        final Map<Long, Translog.Operation> luceneOps = readAllOperationsInLucene(engine, mapper).stream().collect(Collectors.toMap(Translog.Operation::seqNo, Function.identity()));
        final long globalCheckpoint = engine.getTranslog().getLastSyncedGlobalCheckpoint();
        final long retainedOps = engine.config().getIndexSettings().getSoftDeleteRetentionOperations();
        final long maxSeqNo = engine.getLocalCheckpointTracker().getMaxSeqNo();
        for (Translog.Operation translogOp : translogOps.values()) {
            final Translog.Operation luceneOp = luceneOps.get(translogOp.seqNo());
            if (luceneOp == null) {
                if (globalCheckpoint + 1 - retainedOps <= translogOp.seqNo() && translogOp.seqNo() <= maxSeqNo) {
                    fail("Operation not found seq# [" + translogOp.seqNo() + "], global checkpoint [" + globalCheckpoint + "], " + "retention policy [" + retainedOps + "], maxSeqNo [" + maxSeqNo + "], translog op [" + translogOp + "]");
                } else {
                    continue;
                }
            }
            assertThat(luceneOp, notNullValue());
            assertThat(luceneOp.toString(), luceneOp.primaryTerm(), equalTo(translogOp.primaryTerm()));
            assertThat(luceneOp.opType(), equalTo(translogOp.opType()));
            if (luceneOp.opType() == Translog.Operation.Type.INDEX) {
                assertSource.accept(luceneOp, translogOp);
            }
        }
    }

    protected MapperService createMapperService(String type) throws IOException {
        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)).putMapping(type, "{\"properties\": {}}").build();
        MapperService mapperService = MapperTestUtils.newMapperService(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()), createTempDir(), Settings.EMPTY, "test");
        mapperService.merge(indexMetaData, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

    public static Translog getTranslog(Engine engine) {
        return engine.getTranslog();
    }
}