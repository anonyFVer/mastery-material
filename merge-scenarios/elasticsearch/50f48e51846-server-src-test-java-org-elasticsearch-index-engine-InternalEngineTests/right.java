package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.SnapshotMatchers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import static java.util.Collections.emptyMap;
import static java.util.Collections.shuffle;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PEER_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InternalEngineTests extends EngineTestCase {

    public void testVersionMapAfterAutoIDDocument() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField("test"), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = randomBoolean() ? appendOnlyPrimary(doc, false, 1) : appendOnlyReplica(doc, false, 1, randomIntBetween(0, 5));
        engine.index(operation);
        assertFalse(engine.isSafeAccessRequired());
        doc = testParsedDocument("1", null, testDocumentWithTextField("updated"), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index update = indexForDoc(doc);
        engine.index(update);
        assertTrue(engine.isSafeAccessRequired());
        assertEquals(1, engine.getVersionMapSize());
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(0, searcher.reader().numDocs());
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(1, searcher.reader().numDocs());
            TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 1);
            org.apache.lucene.document.Document luceneDoc = searcher.searcher().doc(search.scoreDocs[0].doc);
            assertEquals("test", luceneDoc.get("value"));
        }
        engine.refresh("test");
        if (randomBoolean()) {
            engine.refresh("test");
        }
        assertTrue("safe access should be required we carried it over", engine.isSafeAccessRequired());
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(1, searcher.reader().numDocs());
            TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 1);
            org.apache.lucene.document.Document luceneDoc = searcher.searcher().doc(search.scoreDocs[0].doc);
            assertEquals("updated", luceneDoc.get("value"));
        }
        doc = testParsedDocument("2", null, testDocumentWithTextField("test"), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        operation = randomBoolean() ? appendOnlyPrimary(doc, false, 1) : appendOnlyReplica(doc, false, 1, engine.getLocalCheckpointTracker().generateSeqNo());
        engine.index(operation);
        assertTrue("safe access should be required", engine.isSafeAccessRequired());
        assertEquals(1, engine.getVersionMapSize());
        engine.refresh("test");
        if (randomBoolean()) {
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(2, searcher.reader().numDocs());
        }
        assertFalse("safe access should NOT be required last indexing round was only append only", engine.isSafeAccessRequired());
        engine.delete(new Engine.Delete(operation.type(), operation.id(), operation.uid()));
        assertTrue("safe access should be required", engine.isSafeAccessRequired());
        engine.refresh("test");
        assertTrue("safe access should be required", engine.isSafeAccessRequired());
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(1, searcher.reader().numDocs());
        }
    }

    public void testSegments() throws Exception {
        try (Store store = createStore();
            InternalEngine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(false);
            assertThat(segments.isEmpty(), equalTo(true));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(0L));
            assertThat(engine.segmentsStats(false).getMemoryInBytes(), equalTo(0L));
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            Engine.Index first = indexForDoc(doc);
            Engine.IndexResult firstResult = engine.index(first);
            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
            Engine.Index second = indexForDoc(doc2);
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
            assertThat(segments.get(0).getAttributes().keySet(), Matchers.contains(Lucene50StoredFieldsFormat.MODE_KEY));
            engine.flush();
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(1L));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));
            ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc3));
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
            engine.delete(new Engine.Delete("test", "1", newUid(doc)));
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
            ParsedDocument doc4 = testParsedDocument("4", null, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc4));
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
            ParsedDocument doc5 = testParsedDocument("5", null, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc5));
            engine.refresh("test", Engine.SearcherScope.INTERNAL);
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(4));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(4L));
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
            assertThat(segments.get(3).isCommitted(), equalTo(false));
            assertThat(segments.get(3).isSearch(), equalTo(false));
            assertThat(segments.get(3).getNumDocs(), equalTo(1));
            assertThat(segments.get(3).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(3).isCompound(), equalTo(true));
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(4));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(4L));
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
            assertThat(segments.get(3).isCommitted(), equalTo(false));
            assertThat(segments.get(3).isSearch(), equalTo(true));
            assertThat(segments.get(3).getNumDocs(), equalTo(1));
            assertThat(segments.get(3).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(3).isCompound(), equalTo(true));
        }
    }

    public void testVerboseSegments() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(true);
            assertThat(segments.isEmpty(), equalTo(true));
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");
            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(1));
            assertThat(segments.get(0).ramTree, notNullValue());
            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");
            ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc3));
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
            ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
            Engine.Index index = indexForDoc(doc);
            engine.index(index);
            engine.flush();
            assertThat(engine.segments(false).size(), equalTo(1));
            index = indexForDoc(testParsedDocument("2", null, testDocument(), B_1, null));
            engine.index(index);
            engine.flush();
            List<Segment> segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }
            index = indexForDoc(testParsedDocument("3", null, testDocument(), B_1, null));
            engine.index(index);
            engine.flush();
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(3));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }
            index = indexForDoc(doc);
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

    public void testSegmentsWithIndexSort() throws Exception {
        Sort indexSort = new Sort(new SortedSetSortField("_type", false));
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, null, indexSort, null)) {
            List<Segment> segments = engine.segments(true);
            assertThat(segments.isEmpty(), equalTo(true));
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            assertThat(segments.get(0).getSegmentSort(), equalTo(indexSort));
            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");
            ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc3));
            engine.refresh("test");
            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(3));
            assertThat(segments.get(0).getSegmentSort(), equalTo(indexSort));
            assertThat(segments.get(1).getSegmentSort(), equalTo(indexSort));
            assertThat(segments.get(2).getSegmentSort(), equalTo(indexSort));
        }
    }

    public void testSegmentsStatsIncludingFileSizes() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            assertThat(engine.segmentsStats(true).getFileSizes().size(), equalTo(0));
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");
            SegmentsStats stats = engine.segmentsStats(true);
            assertThat(stats.getFileSizes().size(), greaterThan(0));
            assertThat(() -> stats.getFileSizes().valuesIt(), everyItem(greaterThan(0L)));
            ObjectObjectCursor<String, Long> firstEntry = stats.getFileSizes().iterator().next();
            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");
            assertThat(engine.segmentsStats(true).getFileSizes().get(firstEntry.key), greaterThan(firstEntry.value));
        }
    }

    public void testCommitStats() throws IOException {
        final AtomicLong maxSeqNo = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong localCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        try (Store store = createStore();
            InternalEngine engine = createEngine(store, createTempDir(), (maxSeq, localCP) -> new LocalCheckpointTracker(maxSeq, localCP) {

                @Override
                public long getMaxSeqNo() {
                    return maxSeqNo.get();
                }

                @Override
                public long getCheckpoint() {
                    return localCheckpoint.get();
                }
            })) {
            CommitStats stats1 = engine.commitStats();
            assertThat(stats1.getGeneration(), greaterThan(0L));
            assertThat(stats1.getId(), notNullValue());
            assertThat(stats1.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
            assertThat(stats1.getUserData(), hasKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            assertThat(Long.parseLong(stats1.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
            assertThat(stats1.getUserData(), hasKey(SequenceNumbers.MAX_SEQ_NO));
            assertThat(Long.parseLong(stats1.getUserData().get(SequenceNumbers.MAX_SEQ_NO)), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
            maxSeqNo.set(rarely() ? SequenceNumbers.NO_OPS_PERFORMED : randomIntBetween(0, 1024));
            localCheckpoint.set(rarely() || maxSeqNo.get() == SequenceNumbers.NO_OPS_PERFORMED ? SequenceNumbers.NO_OPS_PERFORMED : randomIntBetween(0, 1024));
            globalCheckpoint.set(rarely() || localCheckpoint.get() == SequenceNumbers.NO_OPS_PERFORMED ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomIntBetween(0, (int) localCheckpoint.get()));
            final Engine.CommitId commitId = engine.flush(true, true);
            CommitStats stats2 = engine.commitStats();
            assertThat(stats2.getRawCommitId(), equalTo(commitId));
            assertThat(stats2.getGeneration(), greaterThan(stats1.getGeneration()));
            assertThat(stats2.getId(), notNullValue());
            assertThat(stats2.getId(), not(equalTo(stats1.getId())));
            assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
            assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_UUID_KEY));
            assertThat(stats2.getUserData().get(Translog.TRANSLOG_GENERATION_KEY), not(equalTo(stats1.getUserData().get(Translog.TRANSLOG_GENERATION_KEY))));
            assertThat(stats2.getUserData().get(Translog.TRANSLOG_UUID_KEY), equalTo(stats1.getUserData().get(Translog.TRANSLOG_UUID_KEY)));
            assertThat(Long.parseLong(stats2.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)), equalTo(localCheckpoint.get()));
            assertThat(stats2.getUserData(), hasKey(SequenceNumbers.MAX_SEQ_NO));
            assertThat(Long.parseLong(stats2.getUserData().get(SequenceNumbers.MAX_SEQ_NO)), equalTo(maxSeqNo.get()));
        }
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
        engine = new InternalEngine(engine.config());
        assertTrue(engine.isRecovering());
        engine.recoverFromTranslog();
        Engine.Searcher searcher = wrapper.wrap(engine.acquireSearcher("test"));
        assertThat(counter.get(), equalTo(2));
        searcher.close();
        IOUtils.close(store, engine);
    }

    public void testFlushIsDisabledDuringTranslogRecovery() throws IOException {
        assertFalse(engine.isRecovering());
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), SOURCE, null);
        engine.index(indexForDoc(doc));
        engine.close();
        engine = new InternalEngine(engine.config());
        expectThrows(IllegalStateException.class, () -> engine.flush(true, true));
        assertTrue(engine.isRecovering());
        engine.recoverFromTranslog();
        assertFalse(engine.isRecovering());
        doc = testParsedDocument("2", null, testDocumentWithTextField(), SOURCE, null);
        engine.index(indexForDoc(doc));
        engine.flush();
    }

    public void testTranslogMultipleOperationsSameDocument() throws IOException {
        final int ops = randomIntBetween(1, 32);
        Engine initialEngine;
        final List<Engine.Operation> operations = new ArrayList<>();
        try {
            initialEngine = engine;
            for (int i = 0; i < ops; i++) {
                final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), SOURCE, null);
                if (randomBoolean()) {
                    final Engine.Index operation = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
                    operations.add(operation);
                    initialEngine.index(operation);
                } else {
                    final Engine.Delete operation = new Engine.Delete("test", "1", newUid(doc), SequenceNumbers.UNASSIGNED_SEQ_NO, 0, i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime());
                    operations.add(operation);
                    initialEngine.delete(operation);
                }
            }
        } finally {
            IOUtils.close(engine);
        }
        Engine recoveringEngine = null;
        try {
            recoveringEngine = new InternalEngine(engine.config());
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
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                initialEngine.index(indexForDoc(doc));
            }
        } finally {
            IOUtils.close(initialEngine);
        }
        Engine recoveringEngine = null;
        try {
            final AtomicBoolean committed = new AtomicBoolean();
            recoveringEngine = new InternalEngine(initialEngine.config()) {

                @Override
                protected void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
                    committed.set(true);
                    super.commitIndexWriter(writer, translog, syncId);
                }
            };
            assertThat(recoveringEngine.getTranslog().uncommittedOperations(), equalTo(docs));
            recoveringEngine.recoverFromTranslog();
            assertTrue(committed.get());
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void testTranslogRecoveryWithMultipleGenerations() throws IOException {
        final int docs = randomIntBetween(1, 4096);
        final List<Long> seqNos = LongStream.range(0, docs).boxed().collect(Collectors.toList());
        Randomness.shuffle(seqNos);
        Engine initialEngine = null;
        Engine recoveringEngine = null;
        Store store = createStore();
        final AtomicInteger counter = new AtomicInteger();
        try {
            initialEngine = createEngine(store, createTempDir(), LocalCheckpointTracker::new, (engine, operation) -> seqNos.get(counter.getAndIncrement()));
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                initialEngine.index(indexForDoc(doc));
                if (rarely()) {
                    initialEngine.getTranslog().rollGeneration();
                } else if (rarely()) {
                    initialEngine.flush();
                }
            }
            initialEngine.close();
            recoveringEngine = new InternalEngine(initialEngine.config());
            recoveringEngine.recoverFromTranslog();
            try (Engine.Searcher searcher = recoveringEngine.acquireSearcher("test")) {
                TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), docs);
                assertEquals(docs, topDocs.totalHits);
            }
        } finally {
            IOUtils.close(initialEngine, recoveringEngine, store);
        }
    }

    public void testConcurrentGetAndFlush() throws Exception {
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));
        final AtomicReference<Engine.GetResult> latestGetResult = new AtomicReference<>();
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        latestGetResult.set(engine.get(newGet(true, doc), searcherFactory));
        final AtomicBoolean flushFinished = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        Thread getThread = new Thread(() -> {
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
                latestGetResult.set(engine.get(newGet(true, doc), searcherFactory));
                if (latestGetResult.get().exists() == false) {
                    break;
                }
            }
        });
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
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(indexForDoc(doc));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();
        Engine.GetResult getResult = engine.get(newGet(false, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();
        getResult = engine.get(newGet(true, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        getResult = engine.get(newGet(false, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();
        getResult = engine.get(newGet(false, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_2), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", null, document, B_2, null);
        engine.index(indexForDoc(doc));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();
        getResult = engine.get(newGet(true, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();
        engine.delete(new Engine.Delete("test", "1", newUid(doc)));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();
        getResult = engine.get(newGet(true, doc), searcherFactory);
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
        doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED));
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
        getResult = engine.get(newGet(true, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(indexForDoc(doc));
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
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();
        engine.refresh("test");
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        engine.delete(new Engine.Delete("test", "1", newUid(doc)));
        engine.refresh("test");
        Engine.Searcher updateSearchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(updateSearchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        updateSearchResult.close();
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();
    }

    public void testCommitAdvancesMinTranslogForRecovery() throws IOException {
        IOUtils.close(engine, store);
        final Path translogPath = createTempDir();
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final LongSupplier globalCheckpointSupplier = () -> globalCheckpoint.get();
        engine = createEngine(config(defaultSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier));
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));
        boolean inSync = randomBoolean();
        if (inSync) {
            globalCheckpoint.set(engine.getLocalCheckpointTracker().getCheckpoint());
        }
        engine.flush();
        assertThat(engine.getTranslog().currentFileGeneration(), equalTo(3L));
        assertThat(engine.getTranslog().getDeletionPolicy().getMinTranslogGenerationForRecovery(), equalTo(inSync ? 3L : 1L));
        assertThat(engine.getTranslog().getDeletionPolicy().getTranslogGenerationOfLastCommit(), equalTo(3L));
        engine.flush();
        assertThat(engine.getTranslog().currentFileGeneration(), equalTo(3L));
        assertThat(engine.getTranslog().getDeletionPolicy().getMinTranslogGenerationForRecovery(), equalTo(inSync ? 3L : 1L));
        assertThat(engine.getTranslog().getDeletionPolicy().getTranslogGenerationOfLastCommit(), equalTo(3L));
        engine.flush(true, true);
        assertThat(engine.getTranslog().currentFileGeneration(), equalTo(4L));
        assertThat(engine.getTranslog().getDeletionPolicy().getMinTranslogGenerationForRecovery(), equalTo(inSync ? 4L : 1L));
        assertThat(engine.getTranslog().getDeletionPolicy().getTranslogGenerationOfLastCommit(), equalTo(4L));
        globalCheckpoint.set(engine.getLocalCheckpointTracker().getCheckpoint());
        engine.flush(true, true);
        assertThat(engine.getTranslog().currentFileGeneration(), equalTo(5L));
        assertThat(engine.getTranslog().getDeletionPolicy().getMinTranslogGenerationForRecovery(), equalTo(5L));
        assertThat(engine.getTranslog().getDeletionPolicy().getTranslogGenerationOfLastCommit(), equalTo(5L));
    }

    public void testSyncedFlush() throws IOException {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), new LogByteSizeMergePolicy(), null)) {
            final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            Engine.CommitId commitID = engine.flush();
            assertThat(commitID, equalTo(new Engine.CommitId(store.readLastCommittedSegmentsInfo().getId())));
            byte[] wrongBytes = Base64.getDecoder().decode(commitID.toString());
            wrongBytes[0] = (byte) ~wrongBytes[0];
            Engine.CommitId wrongId = new Engine.CommitId(wrongBytes);
            assertEquals("should fail to sync flush with wrong id (but no docs)", engine.syncFlush(syncId + "1", wrongId), Engine.SyncedFlushResult.COMMIT_MISMATCH);
            engine.index(indexForDoc(doc));
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
                InternalEngine engine = createEngine(config(defaultSettings, store, createTempDir(), new LogDocMergePolicy(), null))) {
                final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
                Engine.Index doc1 = indexForDoc(testParsedDocument("1", null, testDocumentWithTextField(), B_1, null));
                engine.index(doc1);
                assertEquals(engine.getLastWriteNanos(), doc1.startTime());
                engine.flush();
                Engine.Index doc2 = indexForDoc(testParsedDocument("2", null, testDocumentWithTextField(), B_1, null));
                engine.index(doc2);
                assertEquals(engine.getLastWriteNanos(), doc2.startTime());
                engine.flush();
                final boolean forceMergeFlushes = randomBoolean();
                final ParsedDocument parsedDoc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_1, null);
                if (forceMergeFlushes) {
                    engine.index(new Engine.Index(newUid(parsedDoc3), parsedDoc3, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime() - engine.engineConfig.getFlushMergesAfter().nanos(), -1, false));
                } else {
                    engine.index(indexForDoc(parsedDoc3));
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
                    engine.refresh("test");
                    assertBusy(() -> assertEquals(1, engine.segments(false).size()));
                }
                assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                if (randomBoolean()) {
                    Engine.Index doc4 = indexForDoc(testParsedDocument("4", null, testDocumentWithTextField(), B_1, null));
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

    public void testSyncedFlushSurvivesEngineRestart() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        IOUtils.close(store, engine);
        store = createStore();
        engine = createEngine(store, primaryTranslogDir, globalCheckpoint::get);
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}"), null);
        engine.index(indexForDoc(doc));
        globalCheckpoint.set(0L);
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
        if (randomBoolean()) {
            EngineDiskUtils.createNewTranslog(store.directory(), config.getTranslogConfig().getTranslogPath(), SequenceNumbers.UNASSIGNED_SEQ_NO, shardId);
        }
        engine = new InternalEngine(config);
        engine.recoverFromTranslog();
        assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
    }

    public void testSyncedFlushVanishesOnReplay() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}"), null);
        engine.index(indexForDoc(doc));
        final Engine.CommitId commitID = engine.flush();
        assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID), Engine.SyncedFlushResult.SUCCESS);
        assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        doc = testParsedDocument("2", null, testDocumentWithTextField(), new BytesArray("{}"), null);
        engine.index(indexForDoc(doc));
        EngineConfig config = engine.config();
        engine.close();
        engine = new InternalEngine(config);
        engine.recoverFromTranslog();
        assertNull("Sync ID must be gone since we have a document to replay", engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    public void testVersioningNewCreate() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        create = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), create.primaryTerm(), indexResult.getVersion(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
    }

    public void testReplicatedVersioningWithFlush() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        assertTrue(indexResult.isCreated());
        create = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), create.primaryTerm(), indexResult.getVersion(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        assertTrue(indexResult.isCreated());
        if (randomBoolean()) {
            engine.flush();
        }
        if (randomBoolean()) {
            replicaEngine.flush();
        }
        Engine.Index update = new Engine.Index(newUid(doc), doc, 1);
        Engine.IndexResult updateResult = engine.index(update);
        assertThat(updateResult.getVersion(), equalTo(2L));
        assertFalse(updateResult.isCreated());
        update = new Engine.Index(newUid(doc), doc, updateResult.getSeqNo(), update.primaryTerm(), updateResult.getVersion(), update.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        updateResult = replicaEngine.index(update);
        assertThat(updateResult.getVersion(), equalTo(2L));
        assertFalse(updateResult.isCreated());
        replicaEngine.refresh("test");
        try (Searcher searcher = replicaEngine.acquireSearcher("test")) {
            assertEquals(1, searcher.getDirectoryReader().numDocs());
        }
        engine.refresh("test");
        try (Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(1, searcher.getDirectoryReader().numDocs());
        }
    }

    public void testVersionedUpdate() throws IOException {
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), create.uid()), searcherFactory)) {
            assertEquals(1, get.version());
        }
        Engine.Index update_1 = new Engine.Index(newUid(doc), doc, 1);
        Engine.IndexResult update_1_result = engine.index(update_1);
        assertThat(update_1_result.getVersion(), equalTo(2L));
        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), create.uid()), searcherFactory)) {
            assertEquals(2, get.version());
        }
        Engine.Index update_2 = new Engine.Index(newUid(doc), doc, 2);
        Engine.IndexResult update_2_result = engine.index(update_2);
        assertThat(update_2_result.getVersion(), equalTo(3L));
        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), create.uid()), searcherFactory)) {
            assertEquals(3, get.version());
        }
    }

    public void testVersioningNewIndex() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
    }

    public void testForceMerge() throws IOException {
        try (Store store = createStore();
            Engine engine = createEngine(config(defaultSettings, store, createTempDir(), new LogByteSizeMergePolicy(), null))) {
            int numDocs = randomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), B_1, null);
                Engine.Index index = indexForDoc(doc);
                engine.index(index);
                engine.refresh("test");
            }
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs, test.reader().numDocs());
            }
            engine.forceMerge(true, 1, false, false, false);
            engine.refresh("test");
            assertEquals(engine.segments(true).size(), 1);
            ParsedDocument doc = testParsedDocument(Integer.toString(0), null, testDocument(), B_1, null);
            Engine.Index index = indexForDoc(doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, true, false, false);
            engine.refresh("test");
            assertEquals(engine.segments(true).size(), 1);
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs - 1, test.reader().numDocs());
                assertEquals(engine.config().getMergePolicy().toString(), numDocs - 1, test.reader().maxDoc());
            }
            doc = testParsedDocument(Integer.toString(1), null, testDocument(), B_1, null);
            index = indexForDoc(doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, false, false, false);
            engine.refresh("test");
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
                                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), B_1, null);
                                    Engine.Index index = indexForDoc(doc);
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
                        } catch (AlreadyClosedException ex) {
                        } catch (IOException e) {
                            throw new AssertionError(e);
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

    public void testVersioningCreateExistsException() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        create = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(create);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    protected List<Engine.Operation> generateSingleDocHistory(boolean forReplica, VersionType versionType, boolean partialOldPrimary, long primaryTerm, int minOpCount, int maxOpCount) {
        final int numOfOps = randomIntBetween(minOpCount, maxOpCount);
        final List<Engine.Operation> ops = new ArrayList<>();
        final Term id = newUid("1");
        final int startWithSeqNo;
        if (partialOldPrimary) {
            startWithSeqNo = randomBoolean() ? numOfOps - 1 : randomIntBetween(0, numOfOps - 1);
        } else {
            startWithSeqNo = 0;
        }
        final String valuePrefix = forReplica ? "r_" : "p_";
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
                op = new Engine.Index(id, testParsedDocument("1", null, testDocumentWithTextField(valuePrefix + i), B_1, null), forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO, forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm, version, forReplica ? versionType.versionTypeForReplicationAndRecovery() : versionType, forReplica ? REPLICA : PRIMARY, System.currentTimeMillis(), -1, false);
            } else {
                op = new Engine.Delete("test", "1", id, forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO, forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm, version, forReplica ? versionType.versionTypeForReplicationAndRecovery() : versionType, forReplica ? REPLICA : PRIMARY, System.currentTimeMillis());
            }
            ops.add(op);
        }
        return ops;
    }

    public void testOutOfOrderDocsOnReplica() throws IOException {
        final List<Engine.Operation> ops = generateSingleDocHistory(true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE, VersionType.FORCE), false, 2, 2, 20);
        assertOpsOnReplica(ops, replicaEngine, true);
    }

    private void assertOpsOnReplica(List<Engine.Operation> ops, InternalEngine replicaEngine, boolean shuffleOps) throws IOException {
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
                assertThat(result.hasFailure(), equalTo(false));
            } else {
                Engine.DeleteResult result = replicaEngine.delete((Engine.Delete) op);
                assertThat(result.isFound(), equalTo(firstOp == false));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.hasFailure(), equalTo(false));
            }
            if (randomBoolean()) {
                engine.refresh("test");
            }
            if (randomBoolean()) {
                engine.flush();
                engine.refresh("test");
            }
            firstOp = false;
        }
        assertVisibleCount(replicaEngine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Searcher searcher = replicaEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public void testConcurrentOutOfDocsOnReplica() throws IOException, InterruptedException {
        final List<Engine.Operation> ops = generateSingleDocHistory(true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), false, 2, 100, 300);
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            lastFieldValue = null;
        }
        shuffle(ops, random());
        concurrentlyApplyOps(ops, engine);
        assertVisibleCount(engine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    private void concurrentlyApplyOps(List<Engine.Operation> ops, InternalEngine engine) throws InterruptedException {
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
                        } else {
                            engine.delete((Engine.Delete) op);
                        }
                        if ((docOffset + 1) % 4 == 0) {
                            engine.refresh("test");
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

    public void testInternalVersioningOnPrimary() throws IOException {
        final List<Engine.Operation> ops = generateSingleDocHistory(false, VersionType.INTERNAL, false, 2, 2, 20);
        assertOpsOnPrimary(ops, Versions.NOT_FOUND, true, engine);
    }

    private int assertOpsOnPrimary(List<Engine.Operation> ops, long currentOpVersion, boolean docDeleted, InternalEngine engine) throws IOException {
        String lastFieldValue = null;
        int opsPerformed = 0;
        long lastOpVersion = currentOpVersion;
        BiFunction<Long, Engine.Index, Engine.Index> indexWithVersion = (version, index) -> new Engine.Index(index.uid(), index.parsedDoc(), index.seqNo(), index.primaryTerm(), version, index.versionType(), index.origin(), index.startTime(), index.getAutoGeneratedIdTimestamp(), index.isRetry());
        BiFunction<Long, Engine.Delete, Engine.Delete> delWithVersion = (version, delete) -> new Engine.Delete(delete.type(), delete.id(), delete.uid(), delete.seqNo(), delete.primaryTerm(), version, delete.versionType(), delete.origin(), delete.startTime());
        for (Engine.Operation op : ops) {
            final boolean versionConflict = rarely();
            final boolean versionedOp = versionConflict || randomBoolean();
            final long conflictingVersion = docDeleted || randomBoolean() ? lastOpVersion + (randomBoolean() ? 1 : -1) : Versions.MATCH_DELETED;
            final long correctVersion = docDeleted && randomBoolean() ? Versions.MATCH_DELETED : lastOpVersion;
            logger.info("performing [{}]{}{}", op.operationType().name().charAt(0), versionConflict ? " (conflict " + conflictingVersion + ")" : "", versionedOp ? " (versioned " + correctVersion + ")" : "");
            if (op instanceof Engine.Index) {
                final Engine.Index index = (Engine.Index) op;
                if (versionConflict) {
                    Engine.IndexResult result = engine.index(indexWithVersion.apply(conflictingVersion, index));
                    assertThat(result.isCreated(), equalTo(false));
                    assertThat(result.getVersion(), equalTo(lastOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                } else {
                    Engine.IndexResult result = engine.index(versionedOp ? indexWithVersion.apply(correctVersion, index) : index);
                    assertThat(result.isCreated(), equalTo(docDeleted));
                    assertThat(result.getVersion(), equalTo(Math.max(lastOpVersion + 1, 1)));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    lastFieldValue = index.docs().get(0).get("value");
                    docDeleted = false;
                    lastOpVersion = result.getVersion();
                    opsPerformed++;
                }
            } else {
                final Engine.Delete delete = (Engine.Delete) op;
                if (versionConflict) {
                    Engine.DeleteResult result = engine.delete(delWithVersion.apply(conflictingVersion, delete));
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(lastOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                } else {
                    Engine.DeleteResult result = engine.delete(versionedOp ? delWithVersion.apply(correctVersion, delete) : delete);
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(Math.max(lastOpVersion + 1, 1)));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    docDeleted = true;
                    lastOpVersion = result.getVersion();
                    opsPerformed++;
                }
            }
            if (randomBoolean()) {
                assertVisibleCount(engine, docDeleted ? 0 : 1);
                if (docDeleted == false && lastFieldValue != null) {
                    try (Searcher searcher = engine.acquireSearcher("test")) {
                        final TotalHitCountCollector collector = new TotalHitCountCollector();
                        searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                        assertThat(collector.getTotalHits(), equalTo(1));
                    }
                }
            }
            if (randomBoolean()) {
                engine.flush();
                engine.refresh("test");
            }
            if (rarely()) {
                engine.refresh("gc_simulation", Engine.SearcherScope.INTERNAL);
                engine.clearDeletedTombstones();
                if (docDeleted) {
                    lastOpVersion = Versions.NOT_FOUND;
                }
            }
        }
        assertVisibleCount(engine, docDeleted ? 0 : 1);
        if (docDeleted == false) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
        return opsPerformed;
    }

    public void testNonInternalVersioningOnPrimary() throws IOException {
        final Set<VersionType> nonInternalVersioning = new HashSet<>(Arrays.asList(VersionType.values()));
        nonInternalVersioning.remove(VersionType.INTERNAL);
        final VersionType versionType = randomFrom(nonInternalVersioning);
        final List<Engine.Operation> ops = generateSingleDocHistory(false, versionType, false, 2, 2, 20);
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            lastFieldValue = null;
        }
        if (versionType == VersionType.EXTERNAL) {
            shuffle(ops, random());
        }
        long highestOpVersion = Versions.NOT_FOUND;
        long seqNo = -1;
        boolean docDeleted = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}], seq# [{}], term [{}]", op.operationType().name().charAt(0), op.version(), op.seqNo(), op.primaryTerm());
            if (op instanceof Engine.Index) {
                final Engine.Index index = (Engine.Index) op;
                Engine.IndexResult result = engine.index(index);
                if (op.versionType().isVersionConflictForWrites(highestOpVersion, op.version(), docDeleted) == false) {
                    seqNo++;
                    assertThat(result.getSeqNo(), equalTo(seqNo));
                    assertThat(result.isCreated(), equalTo(docDeleted));
                    assertThat(result.getVersion(), equalTo(op.version()));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    docDeleted = false;
                    highestOpVersion = op.version();
                } else {
                    assertThat(result.isCreated(), equalTo(false));
                    assertThat(result.getVersion(), equalTo(highestOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                }
            } else {
                final Engine.Delete delete = (Engine.Delete) op;
                Engine.DeleteResult result = engine.delete(delete);
                if (op.versionType().isVersionConflictForWrites(highestOpVersion, op.version(), docDeleted) == false) {
                    seqNo++;
                    assertThat(result.getSeqNo(), equalTo(seqNo));
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(op.version()));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    docDeleted = true;
                    highestOpVersion = op.version();
                } else {
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(highestOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                }
            }
            if (randomBoolean()) {
                engine.refresh("test");
            }
            if (randomBoolean()) {
                engine.flush();
                engine.refresh("test");
            }
        }
        assertVisibleCount(engine, docDeleted ? 0 : 1);
        if (docDeleted == false) {
            logger.info("searching for [{}]", lastFieldValue);
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public void testVersioningPromotedReplica() throws IOException {
        final List<Engine.Operation> replicaOps = generateSingleDocHistory(true, VersionType.INTERNAL, false, 1, 2, 20);
        List<Engine.Operation> primaryOps = generateSingleDocHistory(false, VersionType.INTERNAL, false, 2, 2, 20);
        Engine.Operation lastReplicaOp = replicaOps.get(replicaOps.size() - 1);
        final boolean deletedOnReplica = lastReplicaOp instanceof Engine.Delete;
        final long finalReplicaVersion = lastReplicaOp.version();
        final long finalReplicaSeqNo = lastReplicaOp.seqNo();
        assertOpsOnReplica(replicaOps, replicaEngine, true);
        final int opsOnPrimary = assertOpsOnPrimary(primaryOps, finalReplicaVersion, deletedOnReplica, replicaEngine);
        final long currentSeqNo = getSequenceID(replicaEngine, new Engine.Get(false, "type", lastReplicaOp.uid().text(), lastReplicaOp.uid())).v1();
        try (Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.searcher().search(new MatchAllDocsQuery(), collector);
            if (collector.getTotalHits() > 0) {
                assertThat(currentSeqNo, equalTo(finalReplicaSeqNo + opsOnPrimary));
            }
        }
    }

    public void testConcurrentExternalVersioningOnPrimary() throws IOException, InterruptedException {
        final List<Engine.Operation> ops = generateSingleDocHistory(false, VersionType.EXTERNAL, false, 2, 100, 300);
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            lastFieldValue = null;
        }
        shuffle(ops, random());
        concurrentlyApplyOps(ops, engine);
        assertVisibleCount(engine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public void testConcurrentGetAndSetOnPrimary() throws IOException, InterruptedException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch startGun = new CountDownLatch(thread.length);
        final int opsPerThread = randomIntBetween(10, 20);
        class OpAndVersion {

            final long version;

            final String removed;

            final String added;

            OpAndVersion(long version, String removed, String added) {
                this.version = version;
                this.removed = removed;
                this.added = added;
            }
        }
        final AtomicInteger idGenerator = new AtomicInteger();
        final Queue<OpAndVersion> history = ConcurrentCollections.newQueue();
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), bytesArray(""), null);
        final Term uidTerm = newUid(doc);
        engine.index(indexForDoc(doc));
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                for (int op = 0; op < opsPerThread; op++) {
                    try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), uidTerm), searcherFactory)) {
                        FieldsVisitor visitor = new FieldsVisitor(true);
                        get.docIdAndVersion().context.reader().document(get.docIdAndVersion().docId, visitor);
                        List<String> values = new ArrayList<>(Strings.commaDelimitedListToSet(visitor.source().utf8ToString()));
                        String removed = op % 3 == 0 && values.size() > 0 ? values.remove(0) : null;
                        String added = "v_" + idGenerator.incrementAndGet();
                        values.add(added);
                        Engine.Index index = new Engine.Index(uidTerm, testParsedDocument("1", null, testDocument(), bytesArray(Strings.collectionToCommaDelimitedString(values)), null), SequenceNumbers.UNASSIGNED_SEQ_NO, 2, get.version(), VersionType.INTERNAL, PRIMARY, System.currentTimeMillis(), -1, false);
                        Engine.IndexResult indexResult = engine.index(index);
                        if (indexResult.hasFailure() == false) {
                            history.add(new OpAndVersion(indexResult.getVersion(), removed, added));
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
        List<OpAndVersion> sortedHistory = new ArrayList<>(history);
        sortedHistory.sort(Comparator.comparing(o -> o.version));
        Set<String> currentValues = new HashSet<>();
        for (int i = 0; i < sortedHistory.size(); i++) {
            OpAndVersion op = sortedHistory.get(i);
            if (i > 0) {
                assertThat("duplicate version", op.version, not(equalTo(sortedHistory.get(i - 1).version)));
            }
            boolean exists = op.removed == null ? true : currentValues.remove(op.removed);
            assertTrue(op.removed + " should exist", exists);
            exists = currentValues.add(op.added);
            assertTrue(op.added + " should not exist", exists);
        }
        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), uidTerm), searcherFactory)) {
            FieldsVisitor visitor = new FieldsVisitor(true);
            get.docIdAndVersion().context.reader().document(get.docIdAndVersion().docId, visitor);
            List<String> values = Arrays.asList(Strings.commaDelimitedListToStringArray(visitor.source().utf8ToString()));
            assertThat(currentValues, equalTo(new HashSet<>(values)));
        }
    }

    public void testBasicCreatedFlag() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
        index = indexForDoc(doc);
        indexResult = engine.index(index);
        assertFalse(indexResult.isCreated());
        engine.delete(new Engine.Delete("doc", "1", newUid(doc)));
        index = indexForDoc(doc);
        indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
    }

    private static class MockAppender extends AbstractAppender {

        public boolean sawIndexWriterMessage;

        public boolean sawIndexWriterIFDMessage;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.TRACE && event.getMarker().getName().contains("[index][0] ")) {
                if (event.getLoggerName().endsWith(".IW") && formattedMessage.contains("IW: now apply all deletes")) {
                    sawIndexWriterMessage = true;
                }
                if (event.getLoggerName().endsWith(".IFD")) {
                    sawIndexWriterIFDMessage = true;
                }
            }
        }
    }

    public void testIndexWriterInfoStream() throws IllegalAccessException, IOException {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterInfoStream");
        mockAppender.start();
        Logger rootLogger = LogManager.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        Loggers.addAppender(rootLogger, mockAppender);
        Loggers.setLevel(rootLogger, Level.DEBUG);
        rootLogger = LogManager.getRootLogger();
        try {
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            Loggers.setLevel(rootLogger, Level.TRACE);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertTrue(mockAppender.sawIndexWriterMessage);
        } finally {
            Loggers.removeAppender(rootLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(rootLogger, savedLevel);
        }
    }

    public void testSeqNoAndCheckpoints() throws IOException {
        final int opCount = randomIntBetween(1, 256);
        long primarySeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final String[] ids = new String[] { "1", "2", "3" };
        final Set<String> indexedIds = new HashSet<>();
        long localCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
        long replicaLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
        final long globalCheckpoint;
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        InternalEngine initialEngine = null;
        try {
            initialEngine = engine;
            final ShardRouting primary = TestShardRouting.newShardRouting("test", shardId.id(), "node1", null, true, ShardRoutingState.STARTED, allocationId);
            final ShardRouting replica = TestShardRouting.newShardRouting(shardId, "node2", false, ShardRoutingState.STARTED);
            ReplicationTracker gcpTracker = (ReplicationTracker) initialEngine.config().getGlobalCheckpointSupplier();
            gcpTracker.updateFromMaster(1L, new HashSet<>(Arrays.asList(primary.allocationId().getId(), replica.allocationId().getId())), new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replica).build(), Collections.emptySet());
            gcpTracker.activatePrimaryMode(primarySeqNo);
            for (int op = 0; op < opCount; op++) {
                final String id;
                if (rarely() && indexedIds.isEmpty() == false) {
                    id = randomFrom(indexedIds);
                    final Engine.Delete delete = new Engine.Delete("test", id, newUid(id), SequenceNumbers.UNASSIGNED_SEQ_NO, 0, rarely() ? 100 : Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0);
                    final Engine.DeleteResult result = initialEngine.delete(delete);
                    if (!result.hasFailure()) {
                        assertThat(result.getSeqNo(), equalTo(primarySeqNo + 1));
                        assertThat(initialEngine.getLocalCheckpointTracker().getMaxSeqNo(), equalTo(primarySeqNo + 1));
                        indexedIds.remove(id);
                        primarySeqNo++;
                    } else {
                        assertThat(result.getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                        assertThat(initialEngine.getLocalCheckpointTracker().getMaxSeqNo(), equalTo(primarySeqNo));
                    }
                } else {
                    id = randomFrom(ids);
                    ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                    final Engine.Index index = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, rarely() ? 100 : Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0, -1, false);
                    final Engine.IndexResult result = initialEngine.index(index);
                    if (!result.hasFailure()) {
                        assertThat(result.getSeqNo(), equalTo(primarySeqNo + 1));
                        assertThat(initialEngine.getLocalCheckpointTracker().getMaxSeqNo(), equalTo(primarySeqNo + 1));
                        indexedIds.add(id);
                        primarySeqNo++;
                    } else {
                        assertThat(result.getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                        assertThat(initialEngine.getLocalCheckpointTracker().getMaxSeqNo(), equalTo(primarySeqNo));
                    }
                }
                if (randomInt(10) < 3) {
                    replicaLocalCheckpoint = randomIntBetween(Math.toIntExact(replicaLocalCheckpoint), Math.toIntExact(primarySeqNo));
                }
                gcpTracker.updateLocalCheckpoint(primary.allocationId().getId(), initialEngine.getLocalCheckpointTracker().getCheckpoint());
                gcpTracker.updateLocalCheckpoint(replica.allocationId().getId(), replicaLocalCheckpoint);
                if (rarely()) {
                    localCheckpoint = primarySeqNo;
                    maxSeqNo = primarySeqNo;
                    initialEngine.flush(true, true);
                }
            }
            logger.info("localcheckpoint {}, global {}", replicaLocalCheckpoint, primarySeqNo);
            globalCheckpoint = gcpTracker.getGlobalCheckpoint();
            assertEquals(primarySeqNo, initialEngine.getLocalCheckpointTracker().getMaxSeqNo());
            assertEquals(primarySeqNo, initialEngine.getLocalCheckpointTracker().getCheckpoint());
            assertThat(globalCheckpoint, equalTo(replicaLocalCheckpoint));
            assertThat(Long.parseLong(initialEngine.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)), equalTo(localCheckpoint));
            initialEngine.getTranslog().sync();
            assertThat(initialEngine.getTranslog().getLastSyncedGlobalCheckpoint(), equalTo(globalCheckpoint));
            assertThat(Long.parseLong(initialEngine.commitStats().getUserData().get(SequenceNumbers.MAX_SEQ_NO)), equalTo(maxSeqNo));
        } finally {
            IOUtils.close(initialEngine);
        }
        InternalEngine recoveringEngine = null;
        try {
            recoveringEngine = new InternalEngine(initialEngine.config());
            recoveringEngine.recoverFromTranslog();
            assertEquals(primarySeqNo, recoveringEngine.getLocalCheckpointTracker().getMaxSeqNo());
            assertThat(Long.parseLong(recoveringEngine.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)), equalTo(primarySeqNo));
            assertThat(recoveringEngine.getTranslog().getLastSyncedGlobalCheckpoint(), equalTo(globalCheckpoint));
            assertThat(Long.parseLong(recoveringEngine.commitStats().getUserData().get(SequenceNumbers.MAX_SEQ_NO)), equalTo(primarySeqNo));
            assertThat(recoveringEngine.getLocalCheckpointTracker().getCheckpoint(), equalTo(primarySeqNo));
            assertThat(recoveringEngine.getLocalCheckpointTracker().getMaxSeqNo(), equalTo(primarySeqNo));
            assertThat(recoveringEngine.getLocalCheckpointTracker().generateSeqNo(), equalTo(primarySeqNo + 1));
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void testConcurrentWritesAndCommits() throws Exception {
        List<Engine.IndexCommitRef> commits = new ArrayList<>();
        try (Store store = createStore();
            InternalEngine engine = createEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), null))) {
            final int numIndexingThreads = scaledRandomIntBetween(2, 4);
            final int numDocsPerThread = randomIntBetween(500, 1000);
            final CyclicBarrier barrier = new CyclicBarrier(numIndexingThreads + 1);
            final List<Thread> indexingThreads = new ArrayList<>();
            final CountDownLatch doneLatch = new CountDownLatch(numIndexingThreads);
            for (int threadNum = 0; threadNum < numIndexingThreads; threadNum++) {
                final int threadIdx = threadNum;
                Thread indexingThread = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < numDocsPerThread; i++) {
                            final String id = "thread" + threadIdx + "#" + i;
                            ParsedDocument doc = testParsedDocument(id, null, testDocument(), B_1, null);
                            engine.index(indexForDoc(doc));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
                indexingThreads.add(indexingThread);
            }
            for (Thread thread : indexingThreads) {
                thread.start();
            }
            barrier.await();
            int commitLimit = randomIntBetween(10, 20);
            long sleepTime = 1;
            boolean doneIndexing;
            do {
                doneIndexing = doneLatch.await(sleepTime, TimeUnit.MILLISECONDS);
                commits.add(engine.acquireLastIndexCommit(true));
                if (commits.size() > commitLimit) {
                    IOUtils.close(commits.remove(randomIntBetween(0, commits.size() - 1)));
                    sleepTime = sleepTime * 2;
                }
            } while (doneIndexing == false);
            long prevLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
            long prevMaxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
            for (Engine.IndexCommitRef commitRef : commits) {
                final IndexCommit commit = commitRef.getIndexCommit();
                Map<String, String> userData = commit.getUserData();
                long localCheckpoint = userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) ? Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) : SequenceNumbers.NO_OPS_PERFORMED;
                long maxSeqNo = userData.containsKey(SequenceNumbers.MAX_SEQ_NO) ? Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO)) : SequenceNumbers.UNASSIGNED_SEQ_NO;
                assertThat(localCheckpoint, greaterThanOrEqualTo(prevLocalCheckpoint));
                assertThat(maxSeqNo, greaterThanOrEqualTo(prevMaxSeqNo));
                try (IndexReader reader = DirectoryReader.open(commit)) {
                    Long highest = getHighestSeqNo(reader);
                    final long highestSeqNo;
                    if (highest != null) {
                        highestSeqNo = highest.longValue();
                    } else {
                        highestSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                    }
                    assertThat(highestSeqNo, greaterThanOrEqualTo(localCheckpoint));
                    assertThat(highestSeqNo, lessThanOrEqualTo(maxSeqNo));
                    FixedBitSet seqNosBitSet = getSeqNosSet(reader, highestSeqNo);
                    for (int i = 0; i <= localCheckpoint; i++) {
                        assertTrue("local checkpoint [" + localCheckpoint + "], _seq_no [" + i + "] should be indexed", seqNosBitSet.get(i));
                    }
                }
                prevLocalCheckpoint = localCheckpoint;
                prevMaxSeqNo = maxSeqNo;
            }
        }
    }

    private static Long getHighestSeqNo(final IndexReader reader) throws IOException {
        final String fieldName = SeqNoFieldMapper.NAME;
        long size = PointValues.size(reader, fieldName);
        if (size == 0) {
            return null;
        }
        byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
        return LongPoint.decodeDimension(max, 0);
    }

    private static FixedBitSet getSeqNosSet(final IndexReader reader, final long highestSeqNo) throws IOException {
        final FixedBitSet bitSet = new FixedBitSet((int) highestSeqNo + 1);
        final List<LeafReaderContext> leaves = reader.leaves();
        if (leaves.isEmpty()) {
            return bitSet;
        }
        for (int i = 0; i < leaves.size(); i++) {
            final LeafReader leaf = leaves.get(i).reader();
            final NumericDocValues values = leaf.getNumericDocValues(SeqNoFieldMapper.NAME);
            if (values == null) {
                continue;
            }
            final Bits bits = leaf.getLiveDocs();
            for (int docID = 0; docID < leaf.maxDoc(); docID++) {
                if (bits == null || bits.get(docID)) {
                    if (values.advanceExact(docID) == false) {
                        throw new AssertionError("Document does not have a seq number: " + docID);
                    }
                    final long seqNo = values.longValue();
                    assertFalse("should not have more than one document with the same seq_no[" + seqNo + "]", bitSet.get((int) seqNo));
                    bitSet.set((int) seqNo);
                }
            }
        }
        return bitSet;
    }

    public void testIndexWriterIFDInfoStream() throws IllegalAccessException, IOException {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterIFDInfoStream");
        mockAppender.start();
        final Logger iwIFDLogger = Loggers.getLogger("org.elasticsearch.index.engine.Engine.IFD");
        Loggers.addAppender(iwIFDLogger, mockAppender);
        Loggers.setLevel(iwIFDLogger, Level.DEBUG);
        try {
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            assertFalse(mockAppender.sawIndexWriterIFDMessage);
            Loggers.setLevel(iwIFDLogger, Level.TRACE);
            engine.index(indexForDoc(doc));
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
            Engine engine = createEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), null))) {
            engine.config().setEnableGcDeletes(false);
            final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
            Document document = testDocument();
            document.add(new TextField("value", "test1", Field.Store.YES));
            ParsedDocument doc = testParsedDocument("1", null, document, B_2, null);
            engine.index(new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 1, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false));
            engine.delete(new Engine.Delete("test", "1", newUid(doc), SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));
            Engine.GetResult getResult = engine.get(newGet(true, doc), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));
            Thread.sleep(1000);
            if (randomBoolean()) {
                engine.refresh("test");
            }
            engine.delete(new Engine.Delete("test", "2", newUid("2"), SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));
            getResult = engine.get(new Engine.Get(true, "type", "2", newUid("2")), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));
            Engine.Index index = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(index);
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
            getResult = engine.get(newGet(true, doc), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));
            Engine.Index index1 = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
            indexResult = engine.index(index1);
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
            getResult = engine.get(newGet(true, doc), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));
        }
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
                } catch (EngineCreationFailureException | IOException ex) {
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
        EngineDiskUtils.createNewTranslog(store.directory(), primaryTranslogDir, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId);
        EngineConfig config = config(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null);
        engine = new InternalEngine(config);
    }

    public void testTranslogReplayWithFailure() throws IOException {
        final MockDirectoryWrapper directory = newMockDirectory();
        final Path translogPath = createTempDir("testTranslogReplayWithFailure");
        try (Store store = createStore(directory)) {
            final int numDocs = randomIntBetween(1, 10);
            try (InternalEngine engine = createEngine(store, translogPath)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
                    Engine.IndexResult indexResult = engine.index(firstIndexRequest);
                    assertThat(indexResult.getVersion(), equalTo(1L));
                }
                assertVisibleCount(engine, numDocs);
            }
            final int numIters = randomIntBetween(3, 5);
            for (int i = 0; i < numIters; i++) {
                directory.setRandomIOExceptionRateOnOpen(randomDouble());
                directory.setRandomIOExceptionRate(randomDouble());
                directory.setFailOnOpenInput(randomBoolean());
                directory.setAllowRandomFileNotFoundException(randomBoolean());
                boolean started = false;
                InternalEngine engine = null;
                try {
                    engine = createEngine(store, translogPath);
                    started = true;
                } catch (EngineException | IOException e) {
                    logger.trace("exception on open", e);
                }
                directory.setRandomIOExceptionRateOnOpen(0.0);
                directory.setRandomIOExceptionRate(0.0);
                directory.setFailOnOpenInput(false);
                directory.setAllowRandomFileNotFoundException(false);
                if (started) {
                    assertVisibleCount(engine, numDocs, false);
                    engine.close();
                }
            }
        }
    }

    public void testTranslogCleanUpPostCommitCrash() throws Exception {
        IndexSettings indexSettings = new IndexSettings(defaultSettings.getIndexMetaData(), defaultSettings.getNodeSettings(), defaultSettings.getScopedSettings());
        IndexMetaData.Builder builder = IndexMetaData.builder(indexSettings.getIndexMetaData());
        builder.settings(Settings.builder().put(indexSettings.getSettings()).put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1").put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1"));
        indexSettings.updateIndexMetaData(builder.build());
        try (Store store = createStore()) {
            AtomicBoolean throwErrorOnCommit = new AtomicBoolean();
            final Path translogPath = createTempDir();
            final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final LongSupplier globalCheckpointSupplier = () -> globalCheckpoint.get();
            EngineDiskUtils.createEmpty(store.directory(), translogPath, shardId);
            try (InternalEngine engine = new InternalEngine(config(indexSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier)) {

                @Override
                protected void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
                    super.commitIndexWriter(writer, translog, syncId);
                    if (throwErrorOnCommit.get()) {
                        throw new RuntimeException("power's out");
                    }
                }
            }) {
                engine.recoverFromTranslog();
                final ParsedDocument doc1 = testParsedDocument("1", null, testDocumentWithTextField(), SOURCE, null);
                engine.index(indexForDoc(doc1));
                globalCheckpoint.set(engine.getLocalCheckpointTracker().getCheckpoint());
                throwErrorOnCommit.set(true);
                FlushFailedEngineException e = expectThrows(FlushFailedEngineException.class, engine::flush);
                assertThat(e.getCause().getMessage(), equalTo("power's out"));
            }
            try (InternalEngine engine = new InternalEngine(config(indexSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier))) {
                engine.recoverFromTranslog();
                assertVisibleCount(engine, 1);
                final long committedGen = Long.valueOf(engine.getLastCommittedSegmentInfos().getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
                for (int gen = 1; gen < committedGen; gen++) {
                    final Path genFile = translogPath.resolve(Translog.getFilename(gen));
                    assertFalse(genFile + " wasn't cleaned up", Files.exists(genFile));
                }
            }
        }
    }

    public void testSkipTranslogReplay() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        engine.close();
        engine = new InternalEngine(engine.config());
        engine.skipTranslogRecovery();
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(0L));
        }
    }

    private Mapping dynamicUpdate() {
        BuilderContext context = new BuilderContext(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build(), new ContentPath());
        final RootObjectMapper root = new RootObjectMapper.Builder("some_type").build(context);
        return new Mapping(Version.CURRENT, root, new MetadataFieldMapper[0], emptyMap());
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
        final LongSupplier inSyncGlobalCheckpointSupplier = () -> this.engine.getLocalCheckpointTracker().getCheckpoint();
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        TranslogHandler parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        parser.mappingUpdate = dynamicUpdate();
        engine.close();
        engine = new InternalEngine(copy(engine.config(), inSyncGlobalCheckpointSupplier));
        engine.recoverFromTranslog();
        assertVisibleCount(engine, numDocs, false);
        parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        assertEquals(numDocs, parser.appliedOperations());
        if (parser.mappingUpdate != null) {
            assertEquals(1, parser.getRecoveredTypes().size());
            assertTrue(parser.getRecoveredTypes().containsKey("test"));
        } else {
            assertEquals(0, parser.getRecoveredTypes().size());
        }
        engine.close();
        engine = createEngine(store, primaryTranslogDir, inSyncGlobalCheckpointSupplier);
        assertVisibleCount(engine, numDocs, false);
        parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        assertEquals(0, parser.appliedOperations());
        final boolean flush = randomBoolean();
        int randomId = randomIntBetween(numDocs + 1, numDocs + 10);
        ParsedDocument doc = testParsedDocument(Integer.toString(randomId), null, testDocument(), new BytesArray("{}"), null);
        Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 1, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult indexResult = engine.index(firstIndexRequest);
        assertThat(indexResult.getVersion(), equalTo(1L));
        if (flush) {
            engine.flush();
            engine.refresh("test");
        }
        doc = testParsedDocument(Integer.toString(randomId), null, testDocument(), new BytesArray("{}"), null);
        Engine.Index idxRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 2, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult result = engine.index(idxRequest);
        engine.refresh("test");
        assertThat(result.getVersion(), equalTo(2L));
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1L));
        }
        engine.close();
        engine = createEngine(store, primaryTranslogDir, inSyncGlobalCheckpointSupplier);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1L));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        assertEquals(flush ? 1 : 2, parser.appliedOperations());
        engine.delete(new Engine.Delete("test", Integer.toString(randomId), newUid(doc)));
        if (randomBoolean()) {
            engine.refresh("test");
        } else {
            engine.close();
            engine = createEngine(store, primaryTranslogDir, inSyncGlobalCheckpointSupplier);
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs);
            assertThat(topDocs.totalHits, equalTo((long) numDocs));
        }
    }

    public void testRecoverFromForeignTranslog() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        Translog.TranslogGeneration generation = engine.getTranslog().getGeneration();
        engine.close();
        final Path badTranslogLog = createTempDir();
        final String badUUID = Translog.createEmptyTranslog(badTranslogLog, SequenceNumbers.NO_OPS_PERFORMED, shardId);
        Translog translog = new Translog(new TranslogConfig(shardId, badTranslogLog, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE), badUUID, createTranslogDeletionPolicy(INDEX_SETTINGS), () -> SequenceNumbers.NO_OPS_PERFORMED);
        translog.add(new Translog.Index("test", "SomeBogusId", 0, "{}".getBytes(Charset.forName("UTF-8"))));
        assertEquals(generation.translogFileGeneration, translog.currentFileGeneration());
        translog.close();
        EngineConfig config = engine.config();
        TranslogConfig translogConfig = new TranslogConfig(shardId, translog.location(), config.getIndexSettings(), BigArrays.NON_RECYCLING_INSTANCE);
        EngineConfig brokenConfig = new EngineConfig(shardId, allocationId.getId(), threadPool, config.getIndexSettings(), null, store, newMergePolicy(), config.getAnalyzer(), config.getSimilarity(), new CodecService(null, logger), config.getEventListener(), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(), translogConfig, TimeValue.timeValueMinutes(5), config.getExternalRefreshListener(), config.getInternalRefreshListener(), null, config.getTranslogRecoveryRunner(), new NoneCircuitBreakerService(), () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
        try {
            InternalEngine internalEngine = new InternalEngine(brokenConfig);
            fail("translog belongs to a different engine");
        } catch (EngineCreationFailureException ex) {
        }
        engine = createEngine(store, primaryTranslogDir);
        assertVisibleCount(engine, numDocs, false);
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

    public void testConcurrentEngineClosed() throws BrokenBarrierException, InterruptedException {
        Thread[] closingThreads = new Thread[3];
        CyclicBarrier barrier = new CyclicBarrier(1 + closingThreads.length + 1);
        Thread failEngine = new Thread(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            protected void doRun() throws Exception {
                barrier.await();
                engine.failEngine("test", new RuntimeException("test"));
            }
        });
        failEngine.start();
        for (int i = 0; i < closingThreads.length; i++) {
            boolean flushAndClose = randomBoolean();
            closingThreads[i] = new Thread(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    if (flushAndClose) {
                        engine.flushAndClose();
                    } else {
                        engine.close();
                    }
                    synchronized (closingThreads) {
                        try (Lock ignored = store.directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                        }
                    }
                }
            });
            closingThreads[i].setName("closingThread_" + i);
            closingThreads[i].start();
        }
        barrier.await();
        failEngine.join();
        for (Thread t : closingThreads) {
            t.join();
        }
    }

    private static class ThrowingIndexWriter extends IndexWriter {

        private AtomicReference<Supplier<Exception>> failureToThrow = new AtomicReference<>();

        ThrowingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

        @Override
        public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
            maybeThrowFailure();
            return super.addDocument(doc);
        }

        private void maybeThrowFailure() throws IOException {
            if (failureToThrow.get() != null) {
                Exception failure = failureToThrow.get().get();
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                } else if (failure instanceof IOException) {
                    throw (IOException) failure;
                } else {
                    assert false : "unsupported failure class: " + failure.getClass().getCanonicalName();
                }
            }
        }

        @Override
        public long deleteDocuments(Term... terms) throws IOException {
            maybeThrowFailure();
            return super.deleteDocuments(terms);
        }

        public void setThrowFailure(Supplier<Exception> failureSupplier) {
            failureToThrow.set(failureSupplier);
        }

        public void clearFailure() {
            failureToThrow.set(null);
        }
    }

    public void testHandleDocumentFailure() throws Exception {
        try (Store store = createStore()) {
            final ParsedDocument doc1 = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            final ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_1, null);
            final ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_1, null);
            AtomicReference<ThrowingIndexWriter> throwingIndexWriter = new AtomicReference<>();
            try (Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, (directory, iwc) -> {
                throwingIndexWriter.set(new ThrowingIndexWriter(directory, iwc));
                return throwingIndexWriter.get();
            })) {
                if (randomBoolean()) {
                    throwingIndexWriter.get().setThrowFailure(() -> new IOException("simulated"));
                } else {
                    throwingIndexWriter.get().setThrowFailure(() -> new IllegalArgumentException("simulated max token length"));
                }
                Engine.IndexResult indexResult = engine.index(indexForDoc(doc1));
                assertNotNull(indexResult.getFailure());
                assertThat(indexResult.getSeqNo(), equalTo(0L));
                assertThat(indexResult.getVersion(), equalTo(Versions.MATCH_ANY));
                assertNotNull(indexResult.getTranslogLocation());
                throwingIndexWriter.get().clearFailure();
                indexResult = engine.index(indexForDoc(doc1));
                assertThat(indexResult.getSeqNo(), equalTo(1L));
                assertThat(indexResult.getVersion(), equalTo(1L));
                assertNull(indexResult.getFailure());
                assertNotNull(indexResult.getTranslogLocation());
                engine.index(indexForDoc(doc2));
                final Engine.DeleteResult deleteResult;
                if (randomBoolean()) {
                    throwingIndexWriter.get().setThrowFailure(() -> new IOException("simulated"));
                    deleteResult = engine.delete(new Engine.Delete("test", "1", newUid(doc1)));
                    assertThat(deleteResult.getFailure(), instanceOf(IOException.class));
                } else {
                    throwingIndexWriter.get().setThrowFailure(() -> new IllegalArgumentException("simulated max token length"));
                    deleteResult = engine.delete(new Engine.Delete("test", "1", newUid(doc1)));
                    assertThat(deleteResult.getFailure(), instanceOf(IllegalArgumentException.class));
                }
                assertThat(deleteResult.getVersion(), equalTo(2L));
                assertThat(deleteResult.getSeqNo(), equalTo(3L));
                if (randomBoolean()) {
                    throwingIndexWriter.get().setThrowFailure(null);
                    UncheckedIOException uncheckedIOException = expectThrows(UncheckedIOException.class, () -> {
                        Engine.Index index = indexForDoc(doc3);
                        index.parsedDoc().rootDoc().add(new StoredField("foo", "bar") {

                            @Override
                            public BytesRef binaryValue() {
                                throw new UncheckedIOException(new MockDirectoryWrapper.FakeIOException());
                            }
                        });
                        engine.index(index);
                    });
                    assertTrue(uncheckedIOException.getCause() instanceof MockDirectoryWrapper.FakeIOException);
                } else {
                    engine.close();
                }
                try {
                    if (randomBoolean()) {
                        engine.index(indexForDoc(doc1));
                    } else {
                        engine.delete(new Engine.Delete("test", "", newUid(doc1)));
                    }
                    fail("engine should be closed");
                } catch (Exception e) {
                    assertThat(e, instanceOf(AlreadyClosedException.class));
                }
            }
        }
    }

    public void testDoubleDeliveryPrimary() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        Engine.Index retry = appendOnlyPrimary(doc, true, 1);
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
        operation = appendOnlyPrimary(doc, false, 1);
        retry = appendOnlyPrimary(doc, true, 1);
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

    public void testDoubleDeliveryReplicaAppendingAndDeleteOnly() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = appendOnlyReplica(doc, false, 1, randomIntBetween(0, 5));
        Engine.Index retry = appendOnlyReplica(doc, true, 1, randomIntBetween(0, 5));
        Engine.Delete delete = new Engine.Delete(operation.type(), operation.id(), operation.uid(), Math.max(retry.seqNo(), operation.seqNo()) + 1, operation.primaryTerm(), operation.version() + 1, operation.versionType(), REPLICA, operation.startTime() + 1);
        final boolean belowLckp = operation.seqNo() == 0 && retry.seqNo() == 0;
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            engine.delete(delete);
            assertEquals(1, engine.getNumVersionLookups());
            assertTrue(engine.indexWriterHasDeletions());
            Engine.IndexResult retryResult = engine.index(retry);
            assertEquals(belowLckp ? 1 : 2, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            engine.delete(delete);
            assertTrue(engine.indexWriterHasDeletions());
            assertEquals(2, engine.getNumVersionLookups());
            Engine.IndexResult indexResult = engine.index(operation);
            assertEquals(belowLckp ? 2 : 3, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(0, topDocs.totalHits);
        }
    }

    public void testDoubleDeliveryReplicaAppendingOnly() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = appendOnlyReplica(doc, false, 1, randomIntBetween(0, 5));
        Engine.Index retry = appendOnlyReplica(doc, true, 1, randomIntBetween(0, 5));
        final boolean belowLckp = operation.seqNo() == 0 && retry.seqNo() == 0;
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            Engine.IndexResult retryResult = engine.index(retry);
            assertEquals(retry.seqNo() > operation.seqNo(), engine.indexWriterHasDeletions());
            assertEquals(belowLckp ? 0 : 1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertEquals(operation.seqNo() > retry.seqNo(), engine.indexWriterHasDeletions());
            assertEquals(belowLckp ? 1 : 2, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        operation = randomAppendOnly(doc, false, 1);
        retry = randomAppendOnly(doc, true, 1);
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

    public void testDoubleDeliveryReplica() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = replicaIndexForDoc(doc, 1, 20, false);
        Engine.Index duplicate = replicaIndexForDoc(doc, 1, 20, true);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            if (randomBoolean()) {
                engine.refresh("test");
            }
            Engine.IndexResult retryResult = engine.index(duplicate);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(2, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(duplicate);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            if (randomBoolean()) {
                engine.refresh("test");
            }
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(2, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testRetryWithAutogeneratedIdWorksAndNoDuplicateDocs() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = false;
        long autoGeneratedIdTimestamp = 0;
        Engine.Index index = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        isRetry = true;
        index = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.hasFailure(), equalTo(false));
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testRetryWithAutogeneratedIdsAndWrongOrderWorksAndNoDuplicateDocs() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = true;
        long autoGeneratedIdTimestamp = 0;
        Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult result = engine.index(firstIndexRequest);
        assertThat(result.getVersion(), equalTo(1L));
        Engine.Index firstIndexRequestReplica = new Engine.Index(newUid(doc), doc, result.getSeqNo(), firstIndexRequest.primaryTerm(), result.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexReplicaResult = replicaEngine.index(firstIndexRequestReplica);
        assertThat(indexReplicaResult.getVersion(), equalTo(1L));
        isRetry = false;
        Engine.Index secondIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(secondIndexRequest);
        assertTrue(indexResult.isCreated());
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        Engine.Index secondIndexRequestReplica = new Engine.Index(newUid(doc), doc, result.getSeqNo(), secondIndexRequest.primaryTerm(), result.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        replicaEngine.index(secondIndexRequestReplica);
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public Engine.Index randomAppendOnly(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        if (randomBoolean()) {
            return appendOnlyPrimary(doc, retry, autoGeneratedIdTimestamp);
        } else {
            return appendOnlyReplica(doc, retry, autoGeneratedIdTimestamp, 0);
        }
    }

    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        return new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, retry);
    }

    public Engine.Index appendOnlyReplica(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp, final long seqNo) {
        return new Engine.Index(newUid(doc), doc, seqNo, 2, 1, VersionType.EXTERNAL, Engine.Operation.Origin.REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, retry);
    }

    public void testRetryConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        List<Engine.Index> docs = new ArrayList<>();
        final boolean primary = randomBoolean();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            final Engine.Index originalIndex;
            final Engine.Index retryIndex;
            if (primary) {
                originalIndex = appendOnlyPrimary(doc, false, i);
                retryIndex = appendOnlyPrimary(doc, true, i);
            } else {
                originalIndex = appendOnlyReplica(doc, false, i, i * 2);
                retryIndex = appendOnlyReplica(doc, true, i, i * 2);
            }
            docs.add(originalIndex);
            docs.add(retryIndex);
        }
        Collections.shuffle(docs, random());
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
                while ((docOffset = offset.incrementAndGet()) < docs.size()) {
                    try {
                        engine.index(docs.get(docOffset));
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
        if (primary) {
            assertEquals(0, engine.getNumVersionLookups());
            assertEquals(0, engine.getNumIndexVersionsLookups());
        } else {
            assertThat(engine.getNumIndexVersionsLookups(), lessThanOrEqualTo(engine.getNumVersionLookups()));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(numDocs, topDocs.totalHits);
        }
        if (primary) {
            assertTrue(engine.indexWriterHasDeletions());
        } else {
            assertFalse(engine.indexWriterHasDeletions());
        }
    }

    public void testEngineMaxTimestampIsInitialized() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final long timestamp1 = Math.abs(randomNonNegativeLong());
        final Path storeDir = createTempDir();
        final Path translogDir = createTempDir();
        final long timestamp2 = randomNonNegativeLong();
        final long maxTimestamp12 = Math.max(timestamp1, timestamp2);
        final Function<Store, EngineConfig> configSupplier = store -> config(defaultSettings, store, translogDir, NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
        try (Store store = createStore(newFSDirectory(storeDir));
            Engine engine = createEngine(configSupplier.apply(store))) {
            assertEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            engine.index(appendOnlyPrimary(doc, true, timestamp1));
            assertEquals(timestamp1, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
        try (Store store = createStore(newFSDirectory(storeDir));
            Engine engine = new InternalEngine(configSupplier.apply(store))) {
            assertEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            engine.recoverFromTranslog();
            assertEquals(timestamp1, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            engine.index(appendOnlyPrimary(doc, true, timestamp2));
            assertEquals(maxTimestamp12, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            globalCheckpoint.set(1);
            engine.flush();
        }
        try (Store store = createStore(newFSDirectory(storeDir))) {
            if (randomBoolean() || true) {
                EngineDiskUtils.createNewTranslog(store.directory(), translogDir, SequenceNumbers.NO_OPS_PERFORMED, shardId);
            }
            try (Engine engine = new InternalEngine(configSupplier.apply(store))) {
                assertEquals(maxTimestamp12, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            }
        }
    }

    public void testAppendConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        assertEquals(0, engine.getNumVersionLookups());
        assertEquals(0, engine.getNumIndexVersionsLookups());
        boolean primary = randomBoolean();
        List<Engine.Index> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            Engine.Index index = primary ? appendOnlyPrimary(doc, false, i) : appendOnlyReplica(doc, false, i, i);
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
                    assertEquals(0, engine.getVersionMapSize());
                    int docOffset;
                    while ((docOffset = offset.incrementAndGet()) < docs.size()) {
                        try {
                            engine.index(docs.get(docOffset));
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            };
            thread[i].start();
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals("unexpected refresh", 0, searcher.reader().maxDoc());
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
            EngineConfig config = config(defaultSettings, store, translogPath, newMergePolicy(), new ReferenceManager.RefreshListener() {

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
                        throw new ElasticsearchException("something completely different");
                    }
                }
            });
            InternalEngine internalEngine = createEngine(config);
            int docId = 0;
            final ParsedDocument doc = testParsedDocument(Integer.toString(docId), null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            Engine.Index index = randomBoolean() ? indexForDoc(doc) : randomAppendOnly(doc, false, docId);
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
                } catch (AlreadyClosedException ex) {
                    if (ex.getCause() != null) {
                        assertTrue(ex.toString(), ex.getCause() instanceof MockDirectoryWrapper.FakeIOException);
                    }
                } catch (RefreshFailedEngineException ex) {
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

    public void testSequenceIDs() throws Exception {
        Tuple<Long, Long> seqID = getSequenceID(engine, new Engine.Get(false, "type", "2", newUid("1")));
        assertThat(seqID.v1(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
        assertThat(seqID.v2(), equalTo(0L));
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(indexForDoc(doc));
        engine.refresh("test");
        seqID = getSequenceID(engine, newGet(false, doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1(), equalTo(0L));
        assertThat(seqID.v2(), equalTo(2L));
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(indexForDoc(doc));
        engine.refresh("test");
        seqID = getSequenceID(engine, newGet(false, doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1(), equalTo(1L));
        assertThat(seqID.v2(), equalTo(2L));
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 3, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false));
        engine.refresh("test");
        seqID = getSequenceID(engine, newGet(false, doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1(), equalTo(2L));
        assertThat(seqID.v2(), equalTo(3L));
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(LongPoint.newExactQuery("_seq_no", 2), 1));
        searchResult.close();
    }

    private ToLongBiFunction<Engine, Engine.Operation> getStallingSeqNoGenerator(final AtomicReference<CountDownLatch> latchReference, final CyclicBarrier barrier, final AtomicBoolean stall, final AtomicLong expectedLocalCheckpoint) {
        return (engine, operation) -> {
            final long seqNo = engine.getLocalCheckpointTracker().generateSeqNo();
            final CountDownLatch latch = latchReference.get();
            if (stall.get()) {
                try {
                    barrier.await();
                    latch.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (expectedLocalCheckpoint.get() + 1 == seqNo) {
                    expectedLocalCheckpoint.set(seqNo);
                }
            }
            return seqNo;
        };
    }

    public void testSequenceNumberAdvancesToMaxSeqOnEngineOpenOnPrimary() throws BrokenBarrierException, InterruptedException, IOException {
        engine.close();
        final int docs = randomIntBetween(1, 32);
        InternalEngine initialEngine = null;
        try {
            final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>(new CountDownLatch(1));
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean stall = new AtomicBoolean();
            final AtomicLong expectedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final List<Thread> threads = new ArrayList<>();
            initialEngine = createEngine(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null, LocalCheckpointTracker::new, null, getStallingSeqNoGenerator(latchReference, barrier, stall, expectedLocalCheckpoint));
            final InternalEngine finalInitialEngine = initialEngine;
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                stall.set(randomBoolean());
                final Thread thread = new Thread(() -> {
                    try {
                        finalInitialEngine.index(indexForDoc(doc));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
                if (stall.get()) {
                    threads.add(thread);
                    barrier.await();
                } else {
                    thread.join();
                }
            }
            assertThat(initialEngine.getLocalCheckpointTracker().getCheckpoint(), equalTo(expectedLocalCheckpoint.get()));
            assertThat(initialEngine.getLocalCheckpointTracker().getMaxSeqNo(), equalTo((long) (docs - 1)));
            initialEngine.flush(true, true);
            latchReference.get().countDown();
            for (final Thread thread : threads) {
                thread.join();
            }
        } finally {
            IOUtils.close(initialEngine);
        }
        try (Engine recoveringEngine = new InternalEngine(initialEngine.config())) {
            recoveringEngine.recoverFromTranslog();
            recoveringEngine.fillSeqNoGaps(2);
            assertThat(recoveringEngine.getLocalCheckpointTracker().getCheckpoint(), greaterThanOrEqualTo((long) (docs - 1)));
        }
    }

    public void testOutOfOrderSequenceNumbersWithVersionConflict() throws IOException {
        final List<Engine.Operation> operations = new ArrayList<>();
        final int numberOfOperations = randomIntBetween(16, 32);
        final Document document = testDocumentWithTextField();
        final AtomicLong sequenceNumber = new AtomicLong();
        final Engine.Operation.Origin origin = randomFrom(LOCAL_TRANSLOG_RECOVERY, PEER_RECOVERY, PRIMARY, REPLICA);
        final LongSupplier sequenceNumberSupplier = origin == PRIMARY ? () -> SequenceNumbers.UNASSIGNED_SEQ_NO : sequenceNumber::getAndIncrement;
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        final ParsedDocument doc = testParsedDocument("1", null, document, B_1, null);
        final Term uid = newUid(doc);
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        for (int i = 0; i < numberOfOperations; i++) {
            if (randomBoolean()) {
                final Engine.Index index = new Engine.Index(uid, doc, sequenceNumberSupplier.getAsLong(), 1, i, VersionType.EXTERNAL, origin, System.nanoTime(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
                operations.add(index);
            } else {
                final Engine.Delete delete = new Engine.Delete("test", "1", uid, sequenceNumberSupplier.getAsLong(), 1, i, VersionType.EXTERNAL, origin, System.nanoTime());
                operations.add(delete);
            }
        }
        final boolean exists = operations.get(operations.size() - 1) instanceof Engine.Index;
        Randomness.shuffle(operations);
        for (final Engine.Operation operation : operations) {
            if (operation instanceof Engine.Index) {
                engine.index((Engine.Index) operation);
            } else {
                engine.delete((Engine.Delete) operation);
            }
        }
        final long expectedLocalCheckpoint;
        if (origin == PRIMARY) {
            int count = 0;
            long version = -1;
            for (int i = 0; i < numberOfOperations; i++) {
                if (operations.get(i).version() >= version) {
                    count++;
                    version = operations.get(i).version();
                }
            }
            expectedLocalCheckpoint = count - 1;
        } else {
            expectedLocalCheckpoint = numberOfOperations - 1;
        }
        assertThat(engine.getLocalCheckpointTracker().getCheckpoint(), equalTo(expectedLocalCheckpoint));
        try (Engine.GetResult result = engine.get(new Engine.Get(true, "type", "2", uid), searcherFactory)) {
            assertThat(result.exists(), equalTo(exists));
        }
    }

    public void testNoOps() throws IOException {
        engine.close();
        InternalEngine noOpEngine = null;
        final int maxSeqNo = randomIntBetween(0, 128);
        final int localCheckpoint = randomIntBetween(0, maxSeqNo);
        try {
            final BiFunction<Long, Long, LocalCheckpointTracker> supplier = (ms, lcp) -> new LocalCheckpointTracker(maxSeqNo, localCheckpoint);
            noOpEngine = new InternalEngine(engine.config(), supplier) {

                @Override
                protected long doGenerateSeqNoForOperation(Operation operation) {
                    throw new UnsupportedOperationException();
                }
            };
            noOpEngine.recoverFromTranslog();
            final long primaryTerm = randomNonNegativeLong();
            final int gapsFilled = noOpEngine.fillSeqNoGaps(primaryTerm);
            final String reason = randomAlphaOfLength(16);
            noOpEngine.noOp(new Engine.NoOp(maxSeqNo + 1, primaryTerm, randomFrom(PRIMARY, REPLICA, PEER_RECOVERY, LOCAL_TRANSLOG_RECOVERY), System.nanoTime(), reason));
            assertThat(noOpEngine.getLocalCheckpointTracker().getCheckpoint(), equalTo((long) (maxSeqNo + 1)));
            assertThat(noOpEngine.getTranslog().uncommittedOperations(), equalTo(1 + gapsFilled));
            Translog.Operation op;
            Translog.Operation last = null;
            try (Translog.Snapshot snapshot = noOpEngine.getTranslog().newSnapshot()) {
                while ((op = snapshot.next()) != null) {
                    last = op;
                }
            }
            assertNotNull(last);
            assertThat(last, instanceOf(Translog.NoOp.class));
            final Translog.NoOp noOp = (Translog.NoOp) last;
            assertThat(noOp.seqNo(), equalTo((long) (maxSeqNo + 1)));
            assertThat(noOp.primaryTerm(), equalTo(primaryTerm));
            assertThat(noOp.reason(), equalTo(reason));
        } finally {
            IOUtils.close(noOpEngine);
        }
    }

    public void testMinGenerationForSeqNo() throws IOException, BrokenBarrierException, InterruptedException {
        engine.close();
        final int numberOfTriplets = randomIntBetween(1, 32);
        InternalEngine actualEngine = null;
        try {
            final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean stall = new AtomicBoolean();
            final AtomicLong expectedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final Map<Thread, CountDownLatch> threads = new LinkedHashMap<>();
            actualEngine = createEngine(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null, LocalCheckpointTracker::new, null, getStallingSeqNoGenerator(latchReference, barrier, stall, expectedLocalCheckpoint));
            final InternalEngine finalActualEngine = actualEngine;
            final Translog translog = finalActualEngine.getTranslog();
            final long generation = finalActualEngine.getTranslog().currentFileGeneration();
            for (int i = 0; i < numberOfTriplets; i++) {
                stall.set(false);
                index(finalActualEngine, 3 * i);
                final CountDownLatch latch = new CountDownLatch(1);
                latchReference.set(latch);
                final int skipId = 3 * i + 1;
                stall.set(true);
                final Thread thread = new Thread(() -> {
                    try {
                        index(finalActualEngine, skipId);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
                threads.put(thread, latch);
                barrier.await();
                stall.set(false);
                index(finalActualEngine, 3 * i + 2);
                finalActualEngine.flush();
                assertThat(translog.getMinGenerationForSeqNo(3 * i + 1).translogFileGeneration, equalTo(i + generation));
            }
            int i = 0;
            for (final Map.Entry<Thread, CountDownLatch> entry : threads.entrySet()) {
                final Map<String, String> userData = finalActualEngine.commitStats().getUserData();
                assertThat(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY), equalTo(Long.toString(3 * i)));
                assertThat(userData.get(Translog.TRANSLOG_GENERATION_KEY), equalTo(Long.toString(i + generation)));
                entry.getValue().countDown();
                entry.getKey().join();
                finalActualEngine.flush();
                i++;
            }
        } finally {
            IOUtils.close(actualEngine);
        }
    }

    private void index(final InternalEngine engine, final int id) throws IOException {
        final String docId = Integer.toString(id);
        final ParsedDocument doc = testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
        engine.index(indexForDoc(doc));
    }

    private Tuple<Long, Long> getSequenceID(Engine engine, Engine.Get get) throws EngineException {
        try (Searcher searcher = engine.acquireSearcher("get")) {
            final long primaryTerm;
            final long seqNo;
            DocIdAndSeqNo docIdAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.reader(), get.uid());
            if (docIdAndSeqNo == null) {
                primaryTerm = 0;
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            } else {
                seqNo = docIdAndSeqNo.seqNo;
                primaryTerm = VersionsAndSeqNoResolver.loadPrimaryTerm(docIdAndSeqNo, get.uid().field());
            }
            return new Tuple<>(seqNo, primaryTerm);
        } catch (Exception e) {
            throw new EngineException(shardId, "unable to retrieve sequence id", e);
        }
    }

    public void testRestoreLocalCheckpointFromTranslog() throws IOException {
        engine.close();
        InternalEngine actualEngine = null;
        try {
            final Set<Long> completedSeqNos = new HashSet<>();
            final BiFunction<Long, Long, LocalCheckpointTracker> supplier = (maxSeqNo, localCheckpoint) -> new LocalCheckpointTracker(maxSeqNo, localCheckpoint) {

                @Override
                public void markSeqNoAsCompleted(long seqNo) {
                    super.markSeqNoAsCompleted(seqNo);
                    completedSeqNos.add(seqNo);
                }
            };
            actualEngine = new InternalEngine(engine.config(), supplier);
            final int operations = randomIntBetween(0, 1024);
            final Set<Long> expectedCompletedSeqNos = new HashSet<>();
            for (int i = 0; i < operations; i++) {
                if (rarely() && i < operations - 1) {
                    continue;
                }
                expectedCompletedSeqNos.add((long) i);
            }
            final ArrayList<Long> seqNos = new ArrayList<>(expectedCompletedSeqNos);
            Randomness.shuffle(seqNos);
            for (final long seqNo : seqNos) {
                final String id = Long.toString(seqNo);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                final Term uid = newUid(doc);
                final long time = System.nanoTime();
                actualEngine.index(new Engine.Index(uid, doc, seqNo, 1, 1, VersionType.EXTERNAL, REPLICA, time, time, false));
                if (rarely()) {
                    actualEngine.rollTranslogGeneration();
                }
            }
            final long currentLocalCheckpoint = actualEngine.getLocalCheckpointTracker().getCheckpoint();
            final long resetLocalCheckpoint = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Math.toIntExact(currentLocalCheckpoint));
            actualEngine.getLocalCheckpointTracker().resetCheckpoint(resetLocalCheckpoint);
            completedSeqNos.clear();
            actualEngine.restoreLocalCheckpointFromTranslog();
            final Set<Long> intersection = new HashSet<>(expectedCompletedSeqNos);
            intersection.retainAll(LongStream.range(resetLocalCheckpoint + 1, operations).boxed().collect(Collectors.toSet()));
            assertThat(completedSeqNos, equalTo(intersection));
            assertThat(actualEngine.getLocalCheckpointTracker().getCheckpoint(), equalTo(currentLocalCheckpoint));
            assertThat(actualEngine.getLocalCheckpointTracker().generateSeqNo(), equalTo((long) operations));
        } finally {
            IOUtils.close(actualEngine);
        }
    }

    public void testFillUpSequenceIdGapsOnRecovery() throws IOException {
        final int docs = randomIntBetween(1, 32);
        int numDocsOnReplica = 0;
        long maxSeqIDOnReplica = -1;
        long checkpointOnReplica;
        try {
            for (int i = 0; i < docs; i++) {
                final String docId = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
                Engine.Index primaryResponse = indexForDoc(doc);
                Engine.IndexResult indexResult = engine.index(primaryResponse);
                if (randomBoolean()) {
                    numDocsOnReplica++;
                    maxSeqIDOnReplica = indexResult.getSeqNo();
                    replicaEngine.index(replicaIndexForDoc(doc, 1, indexResult.getSeqNo(), false));
                }
            }
            checkpointOnReplica = replicaEngine.getLocalCheckpointTracker().getCheckpoint();
        } finally {
            IOUtils.close(replicaEngine);
        }
        boolean flushed = false;
        AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        Engine recoveringEngine = null;
        try {
            assertEquals(docs - 1, engine.getLocalCheckpointTracker().getMaxSeqNo());
            assertEquals(docs - 1, engine.getLocalCheckpointTracker().getCheckpoint());
            assertEquals(maxSeqIDOnReplica, replicaEngine.getLocalCheckpointTracker().getMaxSeqNo());
            assertEquals(checkpointOnReplica, replicaEngine.getLocalCheckpointTracker().getCheckpoint());
            recoveringEngine = new InternalEngine(copy(replicaEngine.config(), globalCheckpoint::get));
            assertEquals(numDocsOnReplica, recoveringEngine.getTranslog().uncommittedOperations());
            recoveringEngine.recoverFromTranslog();
            assertEquals(maxSeqIDOnReplica, recoveringEngine.getLocalCheckpointTracker().getMaxSeqNo());
            assertEquals(checkpointOnReplica, recoveringEngine.getLocalCheckpointTracker().getCheckpoint());
            assertEquals((maxSeqIDOnReplica + 1) - numDocsOnReplica, recoveringEngine.fillSeqNoGaps(2));
            try (Translog.Snapshot snapshot = recoveringEngine.getTranslog().newSnapshot()) {
                assertTrue((maxSeqIDOnReplica + 1) - numDocsOnReplica <= snapshot.totalOperations());
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.opType() == Translog.Operation.Type.NO_OP) {
                        assertEquals(2, operation.primaryTerm());
                    } else {
                        assertEquals(1, operation.primaryTerm());
                    }
                }
                assertEquals(maxSeqIDOnReplica, recoveringEngine.getLocalCheckpointTracker().getMaxSeqNo());
                assertEquals(maxSeqIDOnReplica, recoveringEngine.getLocalCheckpointTracker().getCheckpoint());
                if ((flushed = randomBoolean())) {
                    globalCheckpoint.set(recoveringEngine.getLocalCheckpointTracker().getMaxSeqNo());
                    recoveringEngine.getTranslog().sync();
                    recoveringEngine.flush(true, true);
                }
            }
        } finally {
            IOUtils.close(recoveringEngine);
        }
        try {
            recoveringEngine = new InternalEngine(copy(replicaEngine.config(), globalCheckpoint::get));
            if (flushed) {
                assertEquals(0, recoveringEngine.getTranslog().uncommittedOperations());
            }
            recoveringEngine.recoverFromTranslog();
            assertEquals(maxSeqIDOnReplica, recoveringEngine.getLocalCheckpointTracker().getMaxSeqNo());
            assertEquals(maxSeqIDOnReplica, recoveringEngine.getLocalCheckpointTracker().getCheckpoint());
            assertEquals(0, recoveringEngine.fillSeqNoGaps(3));
            assertEquals(maxSeqIDOnReplica, recoveringEngine.getLocalCheckpointTracker().getMaxSeqNo());
            assertEquals(maxSeqIDOnReplica, recoveringEngine.getLocalCheckpointTracker().getCheckpoint());
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void assertSameReader(Searcher left, Searcher right) {
        List<LeafReaderContext> leftLeaves = ElasticsearchDirectoryReader.unwrap(left.getDirectoryReader()).leaves();
        List<LeafReaderContext> rightLeaves = ElasticsearchDirectoryReader.unwrap(right.getDirectoryReader()).leaves();
        assertEquals(rightLeaves.size(), leftLeaves.size());
        for (int i = 0; i < leftLeaves.size(); i++) {
            assertSame(leftLeaves.get(i).reader(), rightLeaves.get(i).reader());
        }
    }

    public void assertNotSameReader(Searcher left, Searcher right) {
        List<LeafReaderContext> leftLeaves = ElasticsearchDirectoryReader.unwrap(left.getDirectoryReader()).leaves();
        List<LeafReaderContext> rightLeaves = ElasticsearchDirectoryReader.unwrap(right.getDirectoryReader()).leaves();
        if (rightLeaves.size() == leftLeaves.size()) {
            for (int i = 0; i < leftLeaves.size(); i++) {
                if (leftLeaves.get(i).reader() != rightLeaves.get(i).reader()) {
                    return;
                }
            }
            fail("readers are same");
        }
    }

    public void testRefreshScopedSearcher() throws IOException {
        try (Store store = createStore();
            InternalEngine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertSameReader(getSearcher, searchSearcher);
            }
            for (int i = 0; i < 10; i++) {
                final String docId = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
                Engine.Index primaryResponse = indexForDoc(doc);
                engine.index(primaryResponse);
            }
            assertTrue(engine.refreshNeeded());
            engine.refresh("test", Engine.SearcherScope.INTERNAL);
            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertEquals(10, getSearcher.reader().numDocs());
                assertEquals(0, searchSearcher.reader().numDocs());
                assertNotSameReader(getSearcher, searchSearcher);
            }
            engine.refresh("test", Engine.SearcherScope.EXTERNAL);
            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertEquals(10, getSearcher.reader().numDocs());
                assertEquals(10, searchSearcher.reader().numDocs());
                assertSameReader(getSearcher, searchSearcher);
            }
            final String docId = Integer.toString(10);
            final ParsedDocument doc = testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
            Engine.Index primaryResponse = indexForDoc(doc);
            engine.index(primaryResponse);
            engine.refresh("test", Engine.SearcherScope.EXTERNAL);
            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertEquals(11, getSearcher.reader().numDocs());
                assertEquals(11, searchSearcher.reader().numDocs());
                assertSameReader(getSearcher, searchSearcher);
            }
            try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                engine.refresh("test", Engine.SearcherScope.INTERNAL);
                try (Searcher nextSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                    assertSame(searcher.searcher(), nextSearcher.searcher());
                }
            }
            try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                engine.refresh("test", Engine.SearcherScope.EXTERNAL);
                try (Searcher nextSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                    assertSame(searcher.searcher(), nextSearcher.searcher());
                }
            }
        }
    }

    public void testSeqNoGenerator() throws IOException {
        engine.close();
        final long seqNo = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Integer.MAX_VALUE);
        final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier = (ms, lcp) -> new LocalCheckpointTracker(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong seqNoGenerator = new AtomicLong(seqNo);
        try (Engine e = createEngine(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null, localCheckpointTrackerSupplier, null, (engine, operation) -> seqNoGenerator.getAndIncrement())) {
            final String id = "id";
            final Field uidField = new Field("_id", id, IdFieldMapper.Defaults.FIELD_TYPE);
            final String type = "type";
            final Field versionField = new NumericDocValuesField("_version", 0);
            final SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
            final ParseContext.Document document = new ParseContext.Document();
            document.add(uidField);
            document.add(versionField);
            document.add(seqID.seqNo);
            document.add(seqID.seqNoDocValue);
            document.add(seqID.primaryTerm);
            final BytesReference source = new BytesArray(new byte[] { 1 });
            final ParsedDocument parsedDocument = new ParsedDocument(versionField, seqID, id, type, "routing", Collections.singletonList(document), source, XContentType.JSON, null);
            final Engine.Index index = new Engine.Index(new Term("_id", parsedDocument.id()), parsedDocument, SequenceNumbers.UNASSIGNED_SEQ_NO, (long) randomIntBetween(1, 8), Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.currentTimeMillis(), System.currentTimeMillis(), randomBoolean());
            final Engine.IndexResult indexResult = e.index(index);
            assertThat(indexResult.getSeqNo(), equalTo(seqNo));
            assertThat(seqNoGenerator.get(), equalTo(seqNo + 1));
            final Engine.Delete delete = new Engine.Delete(type, id, new Term("_id", parsedDocument.id()), SequenceNumbers.UNASSIGNED_SEQ_NO, (long) randomIntBetween(1, 8), Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.currentTimeMillis());
            final Engine.DeleteResult deleteResult = e.delete(delete);
            assertThat(deleteResult.getSeqNo(), equalTo(seqNo + 1));
            assertThat(seqNoGenerator.get(), equalTo(seqNo + 2));
        }
    }

    public void testKeepTranslogAfterGlobalCheckpoint() throws Exception {
        IOUtils.close(engine, store);
        final IndexSettings indexSettings = new IndexSettings(defaultSettings.getIndexMetaData(), defaultSettings.getNodeSettings(), defaultSettings.getScopedSettings());
        IndexMetaData.Builder builder = IndexMetaData.builder(indexSettings.getIndexMetaData()).settings(Settings.builder().put(indexSettings.getSettings()).put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomFrom("-1", "100micros", "30m")).put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), randomFrom("-1", "512b", "1gb")));
        indexSettings.updateIndexMetaData(builder.build());
        final Path translogPath = createTempDir();
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final EngineConfig engineConfig = config(indexSettings, store, translogPath, NoMergePolicy.INSTANCE, null, null, () -> globalCheckpoint.get());
        EngineDiskUtils.createEmpty(store.directory(), translogPath, shardId);
        try (Engine engine = new InternalEngine(engineConfig) {

            @Override
            protected void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
                if (rarely()) {
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), getLocalCheckpointTracker().getCheckpoint()));
                }
                super.commitIndexWriter(writer, translog, syncId);
            }
        }) {
            engine.recoverFromTranslog();
            int numDocs = scaledRandomIntBetween(10, 100);
            final String translogUUID = engine.getTranslog().getTranslogUUID();
            for (int docId = 0; docId < numDocs; docId++) {
                ParseContext.Document document = testDocumentWithTextField();
                document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
                engine.index(indexForDoc(testParsedDocument(Integer.toString(docId), null, document, B_1, null)));
                if (frequently()) {
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getLocalCheckpointTracker().getCheckpoint()));
                    engine.getTranslog().sync();
                }
                if (frequently()) {
                    final long lastSyncedGlobalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
                    engine.flush(randomBoolean(), true);
                    final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
                    final IndexCommit safeCommit = commits.get(0);
                    if (lastSyncedGlobalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        assertThat(Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
                    } else {
                        assertThat(Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)), lessThanOrEqualTo(lastSyncedGlobalCheckpoint));
                    }
                    for (int i = 1; i < commits.size(); i++) {
                        assertThat(Long.parseLong(commits.get(i).getUserData().get(SequenceNumbers.MAX_SEQ_NO)), greaterThan(lastSyncedGlobalCheckpoint));
                    }
                    long localCheckpointFromSafeCommit = Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
                    try (Translog.Snapshot snapshot = engine.getTranslog().newSnapshot()) {
                        assertThat(snapshot, SnapshotMatchers.containsSeqNoRange(localCheckpointFromSafeCommit + 1, docId));
                    }
                }
            }
        }
    }

    public void testConcurrentAppendUpdateAndRefresh() throws InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicInteger numDeletes = new AtomicInteger();
        Thread thread = new Thread(() -> {
            try {
                latch.countDown();
                latch.await();
                for (int j = 0; j < numDocs; j++) {
                    String docID = Integer.toString(j);
                    ParsedDocument doc = testParsedDocument(docID, null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
                    Engine.Index operation = appendOnlyPrimary(doc, false, 1);
                    engine.index(operation);
                    if (rarely()) {
                        engine.delete(new Engine.Delete(operation.type(), operation.id(), operation.uid()));
                        numDeletes.incrementAndGet();
                    } else {
                        doc = testParsedDocument(docID, null, testDocumentWithTextField("updated"), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
                        Engine.Index update = indexForDoc(doc);
                        engine.index(update);
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                done.set(true);
            }
        });
        thread.start();
        latch.countDown();
        latch.await();
        while (done.get() == false) {
            engine.refresh("test", Engine.SearcherScope.INTERNAL);
        }
        thread.join();
        engine.refresh("test", Engine.SearcherScope.INTERNAL);
        try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), searcher.reader().numDocs());
            for (int i = 0; i < search.scoreDocs.length; i++) {
                org.apache.lucene.document.Document luceneDoc = searcher.searcher().doc(search.scoreDocs[i].doc);
                assertEquals("updated", luceneDoc.get("value"));
            }
            int totalNumDocs = numDocs - numDeletes.get();
            assertEquals(totalNumDocs, searcher.reader().numDocs());
        }
    }

    public void testAcquireIndexCommit() throws Exception {
        IOUtils.close(engine, store);
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (InternalEngine engine = createEngine(store, createTempDir(), globalCheckpoint::get)) {
            int numDocs = between(1, 20);
            for (int i = 0; i < numDocs; i++) {
                index(engine, i);
            }
            if (randomBoolean()) {
                globalCheckpoint.set(numDocs - 1);
            }
            final boolean flushFirst = randomBoolean();
            final boolean safeCommit = randomBoolean();
            final Engine.IndexCommitRef snapshot;
            if (safeCommit) {
                snapshot = engine.acquireSafeIndexCommit();
            } else {
                snapshot = engine.acquireLastIndexCommit(flushFirst);
            }
            int moreDocs = between(1, 20);
            for (int i = 0; i < moreDocs; i++) {
                index(engine, numDocs + i);
            }
            globalCheckpoint.set(numDocs + moreDocs - 1);
            engine.flush();
            try (IndexReader reader = DirectoryReader.open(snapshot.getIndexCommit())) {
                assertThat(reader.numDocs(), equalTo(flushFirst && safeCommit == false ? numDocs : 0));
            }
            assertThat(DirectoryReader.listCommits(engine.store.directory()), hasSize(2));
            snapshot.close();
            engine.flush(true, true);
            assertThat(DirectoryReader.listCommits(engine.store.directory()), hasSize(1));
        }
    }

    public void testOpenIndexAndTranslogKeepOnlySafeCommit() throws Exception {
        IOUtils.close(engine);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final EngineConfig config = copy(engine.config(), globalCheckpoint::get);
        final IndexCommit safeCommit;
        try (InternalEngine engine = createEngine(config)) {
            final int numDocs = between(5, 50);
            for (int i = 0; i < numDocs; i++) {
                index(engine, i);
                if (randomBoolean()) {
                    engine.flush();
                }
            }
            final List<IndexCommit> commits = DirectoryReader.listCommits(engine.store.directory());
            safeCommit = randomFrom(commits);
            globalCheckpoint.set(Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)));
            engine.getTranslog().sync();
        }
        try (InternalEngine engine = new InternalEngine(config)) {
            final List<IndexCommit> existingCommits = DirectoryReader.listCommits(engine.store.directory());
            assertThat("safe commit should be kept", existingCommits, contains(safeCommit));
        }
    }

    public void testCleanUpCommitsWhenGlobalCheckpointAdvanced() throws Exception {
        IOUtils.close(engine, store);
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (InternalEngine engine = createEngine(store, createTempDir(), globalCheckpoint::get)) {
            final int numDocs = scaledRandomIntBetween(10, 100);
            for (int docId = 0; docId < numDocs; docId++) {
                index(engine, docId);
                if (frequently()) {
                    engine.flush(randomBoolean(), randomBoolean());
                }
            }
            engine.flush(false, randomBoolean());
            List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
            globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getLocalCheckpointTracker().getCheckpoint() - 1));
            engine.syncTranslog();
            assertThat(DirectoryReader.listCommits(store.directory()), equalTo(commits));
            globalCheckpoint.set(randomLongBetween(engine.getLocalCheckpointTracker().getCheckpoint(), Long.MAX_VALUE));
            engine.syncTranslog();
            assertThat(DirectoryReader.listCommits(store.directory()), contains(commits.get(commits.size() - 1)));
        }
    }

    public void testCleanupCommitsWhenReleaseSnapshot() throws Exception {
        IOUtils.close(engine, store);
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (InternalEngine engine = createEngine(store, createTempDir(), globalCheckpoint::get)) {
            final int numDocs = scaledRandomIntBetween(10, 100);
            for (int docId = 0; docId < numDocs; docId++) {
                index(engine, docId);
                if (frequently()) {
                    engine.flush(randomBoolean(), randomBoolean());
                }
            }
            engine.flush(false, randomBoolean());
            int numSnapshots = between(1, 10);
            final List<Engine.IndexCommitRef> snapshots = new ArrayList<>();
            for (int i = 0; i < numSnapshots; i++) {
                snapshots.add(engine.acquireSafeIndexCommit());
            }
            globalCheckpoint.set(engine.getLocalCheckpointTracker().getCheckpoint());
            engine.syncTranslog();
            final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
            for (int i = 0; i < numSnapshots - 1; i++) {
                snapshots.get(i).close();
                assertThat(DirectoryReader.listCommits(store.directory()), equalTo(commits));
            }
            snapshots.get(numSnapshots - 1).close();
            assertThat(DirectoryReader.listCommits(store.directory()), hasSize(1));
        }
    }

    public void testShouldPeriodicallyFlush() throws Exception {
        assertThat("Empty engine does not need flushing", engine.shouldPeriodicallyFlush(), equalTo(false));
        int numDocs = between(10, 100);
        for (int id = 0; id < numDocs; id++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(id), null, testDocumentWithTextField(), SOURCE, null);
            engine.index(indexForDoc(doc));
        }
        assertThat("Not exceeded translog flush threshold yet", engine.shouldPeriodicallyFlush(), equalTo(false));
        long flushThreshold = RandomNumbers.randomLongBetween(random(), 100, engine.getTranslog().uncommittedSizeInBytes());
        final IndexSettings indexSettings = engine.config().getIndexSettings();
        final IndexMetaData indexMetaData = IndexMetaData.builder(indexSettings.getIndexMetaData()).settings(Settings.builder().put(indexSettings.getSettings()).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), flushThreshold + "b")).build();
        indexSettings.updateIndexMetaData(indexMetaData);
        engine.onSettingsChanged();
        assertThat(engine.getTranslog().uncommittedOperations(), equalTo(numDocs));
        assertThat(engine.shouldPeriodicallyFlush(), equalTo(true));
        engine.flush();
        assertThat(engine.getTranslog().uncommittedOperations(), equalTo(0));
        for (int id = 0; id < numDocs; id++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(id), null, testDocumentWithTextField(), SOURCE, null);
            final Engine.IndexResult result = engine.index(replicaIndexForDoc(doc, 1L, id, false));
            assertThat(result.isCreated(), equalTo(false));
        }
        SegmentInfos lastCommitInfo = engine.getLastCommittedSegmentInfos();
        assertThat(engine.getTranslog().uncommittedOperations(), equalTo(numDocs));
        assertThat(engine.shouldPeriodicallyFlush(), equalTo(true));
        engine.flush(false, false);
        assertThat(engine.getLastCommittedSegmentInfos(), not(sameInstance(lastCommitInfo)));
        assertThat(engine.getTranslog().uncommittedOperations(), equalTo(0));
    }

    public void testStressUpdateSameDocWhileGettingIt() throws IOException, InterruptedException {
        final int iters = randomIntBetween(1, 15);
        for (int i = 0; i < iters; i++) {
            try (Store store = createStore();
                InternalEngine engine = createEngine(store, createTempDir())) {
                final IndexSettings indexSettings = engine.config().getIndexSettings();
                final IndexMetaData indexMetaData = IndexMetaData.builder(indexSettings.getIndexMetaData()).settings(Settings.builder().put(indexSettings.getSettings()).put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), TimeValue.timeValueMillis(1))).build();
                engine.engineConfig.getIndexSettings().updateIndexMetaData(indexMetaData);
                engine.onSettingsChanged();
                ParsedDocument document = testParsedDocument(Integer.toString(0), null, testDocumentWithTextField(), SOURCE, null);
                final Engine.Index doc = new Engine.Index(newUid(document), document, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), 0, false);
                engine.index(doc);
                engine.delete(new Engine.Delete(doc.type(), doc.id(), doc.uid()));
                ParsedDocument document1 = testParsedDocument(Integer.toString(1), null, testDocumentWithTextField(), SOURCE, null);
                engine.index(new Engine.Index(newUid(document1), document1, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), 0, false));
                engine.refresh("test");
                ParsedDocument document2 = testParsedDocument(Integer.toString(2), null, testDocumentWithTextField(), SOURCE, null);
                engine.index(new Engine.Index(newUid(document2), document2, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), 0, false));
                engine.refresh("test");
                ParsedDocument document3 = testParsedDocument(Integer.toString(3), null, testDocumentWithTextField(), SOURCE, null);
                final Engine.Index doc3 = new Engine.Index(newUid(document3), document3, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), 0, false);
                engine.index(doc3);
                engine.engineConfig.setEnableGcDeletes(true);
                CountDownLatch awaitStarted = new CountDownLatch(1);
                Thread thread = new Thread(() -> {
                    awaitStarted.countDown();
                    try (Engine.GetResult getResult = engine.get(new Engine.Get(true, doc3.type(), doc3.id(), doc3.uid()), engine::acquireSearcher)) {
                        assertTrue(getResult.exists());
                    }
                });
                thread.start();
                awaitStarted.await();
                try (Engine.GetResult getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), doc.uid()), engine::acquireSearcher)) {
                    assertFalse(getResult.exists());
                }
                thread.join();
            }
        }
    }
}