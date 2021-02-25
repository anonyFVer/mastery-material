package org.elasticsearch.common.lucene;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.ESTestCase;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.hamcrest.Matchers.equalTo;

public class LuceneTests extends ESTestCase {

    public void testWaitForIndex() throws Exception {
        final MockDirectoryWrapper dir = newMockDirectory();
        final AtomicBoolean succeeded = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    latch.await();
                    if (Lucene.waitForIndex(dir, 5000)) {
                        succeeded.set(true);
                    } else {
                        fail("index should have eventually existed!");
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    fail("should have been able to create the engine! " + e.getMessage());
                }
            }
        });
        t.start();
        latch.countDown();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        t.join();
        writer.close();
        dir.close();
        assertTrue("index should have eventually existed", succeeded.get());
    }

    public void testCleanIndex() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        doc = new Document();
        doc.add(new TextField("id", "4", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.deleteDocuments(new Term("id", "2"));
        writer.commit();
        try (DirectoryReader open = DirectoryReader.open(writer)) {
            assertEquals(3, open.numDocs());
            assertEquals(1, open.numDeletedDocs());
            assertEquals(4, open.maxDoc());
        }
        writer.close();
        if (random().nextBoolean()) {
            for (String file : dir.listAll()) {
                if (file.startsWith("_1")) {
                    dir.deleteFile(file);
                    break;
                }
            }
        }
        Lucene.cleanLuceneIndex(dir);
        if (dir.listAll().length > 0) {
            for (String file : dir.listAll()) {
                if (file.startsWith("extra") == false) {
                    assertEquals(file, "write.lock");
                }
            }
        }
        dir.close();
    }

    public void testPruneUnreferencedFiles() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(dir);
        doc = new Document();
        doc.add(new TextField("id", "4", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.deleteDocuments(new Term("id", "2"));
        writer.commit();
        DirectoryReader open = DirectoryReader.open(writer);
        assertEquals(3, open.numDocs());
        assertEquals(1, open.numDeletedDocs());
        assertEquals(4, open.maxDoc());
        open.close();
        writer.close();
        SegmentInfos si = Lucene.pruneUnreferencedFiles(segmentCommitInfos.getSegmentsFileName(), dir);
        assertEquals(si.getSegmentsFileName(), segmentCommitInfos.getSegmentsFileName());
        open = DirectoryReader.open(dir);
        assertEquals(3, open.numDocs());
        assertEquals(0, open.numDeletedDocs());
        assertEquals(3, open.maxDoc());
        IndexSearcher s = new IndexSearcher(open);
        assertEquals(s.search(new TermQuery(new Term("id", "1")), 1).totalHits, 1);
        assertEquals(s.search(new TermQuery(new Term("id", "2")), 1).totalHits, 1);
        assertEquals(s.search(new TermQuery(new Term("id", "3")), 1).totalHits, 1);
        assertEquals(s.search(new TermQuery(new Term("id", "4")), 1).totalHits, 0);
        for (String file : dir.listAll()) {
            assertFalse("unexpected file: " + file, file.equals("segments_3") || file.startsWith("_2"));
        }
        open.close();
        dir.close();
    }

    public void testFiles() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(2);
        iwc.setUseCompoundFile(true);
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        Set<String> files = new HashSet<>();
        for (String f : Lucene.files(Lucene.readSegmentInfos(dir))) {
            files.add(f);
        }
        final boolean simpleTextCFS = files.contains("_0.scf");
        assertTrue(files.toString(), files.contains("segments_1"));
        if (simpleTextCFS) {
            assertFalse(files.toString(), files.contains("_0.cfs"));
            assertFalse(files.toString(), files.contains("_0.cfe"));
        } else {
            assertTrue(files.toString(), files.contains("_0.cfs"));
            assertTrue(files.toString(), files.contains("_0.cfe"));
        }
        assertTrue(files.toString(), files.contains("_0.si"));
        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        files.clear();
        for (String f : Lucene.files(Lucene.readSegmentInfos(dir))) {
            files.add(f);
        }
        assertFalse(files.toString(), files.contains("segments_1"));
        assertTrue(files.toString(), files.contains("segments_2"));
        if (simpleTextCFS) {
            assertFalse(files.toString(), files.contains("_0.cfs"));
            assertFalse(files.toString(), files.contains("_0.cfe"));
        } else {
            assertTrue(files.toString(), files.contains("_0.cfs"));
            assertTrue(files.toString(), files.contains("_0.cfe"));
        }
        assertTrue(files.toString(), files.contains("_0.si"));
        if (simpleTextCFS) {
            assertFalse(files.toString(), files.contains("_1.cfs"));
            assertFalse(files.toString(), files.contains("_1.cfe"));
        } else {
            assertTrue(files.toString(), files.contains("_1.cfs"));
            assertTrue(files.toString(), files.contains("_1.cfe"));
        }
        assertTrue(files.toString(), files.contains("_1.si"));
        writer.close();
        dir.close();
    }

    public void testNumDocs() throws IOException {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new TextField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        SegmentInfos segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(1, Lucene.getNumDocs(segmentCommitInfos));
        doc = new Document();
        doc.add(new TextField("id", "2", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new TextField("id", "3", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(1, Lucene.getNumDocs(segmentCommitInfos));
        writer.commit();
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(3, Lucene.getNumDocs(segmentCommitInfos));
        writer.deleteDocuments(new Term("id", "2"));
        writer.commit();
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(2, Lucene.getNumDocs(segmentCommitInfos));
        int numDocsToIndex = randomIntBetween(10, 50);
        List<Term> deleteTerms = new ArrayList<>();
        for (int i = 0; i < numDocsToIndex; i++) {
            doc = new Document();
            doc.add(new TextField("id", "extra_" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            deleteTerms.add(new Term("id", "extra_" + i));
            writer.addDocument(doc);
        }
        int numDocsToDelete = randomIntBetween(0, numDocsToIndex);
        Collections.shuffle(deleteTerms, random());
        for (int i = 0; i < numDocsToDelete; i++) {
            Term remove = deleteTerms.remove(0);
            writer.deleteDocuments(remove);
        }
        writer.commit();
        segmentCommitInfos = Lucene.readSegmentInfos(dir);
        assertEquals(2 + deleteTerms.size(), Lucene.getNumDocs(segmentCommitInfos));
        writer.close();
        dir.close();
    }

    public void testCount() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        try (DirectoryReader reader = w.getReader()) {
            IndexSearcher searcher = newSearcher(reader);
            assertFalse(Lucene.exists(searcher, new MatchAllDocsQuery()));
        }
        Document doc = new Document();
        w.addDocument(doc);
        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);
        try (DirectoryReader reader = w.getReader()) {
            IndexSearcher searcher = newSearcher(reader);
            assertTrue(Lucene.exists(searcher, new MatchAllDocsQuery()));
            assertFalse(Lucene.exists(searcher, new TermQuery(new Term("baz", "bar"))));
            assertTrue(Lucene.exists(searcher, new TermQuery(new Term("foo", "bar"))));
        }
        w.deleteDocuments(new Term("foo", "bar"));
        try (DirectoryReader reader = w.getReader()) {
            IndexSearcher searcher = newSearcher(reader);
            assertFalse(Lucene.exists(searcher, new TermQuery(new Term("foo", "bar"))));
        }
        w.close();
        dir.close();
    }

    public void testAsSequentialAccessBits() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);
        doc = new Document();
        w.addDocument(doc);
        doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
            IndexSearcher searcher = newSearcher(reader);
            Weight termWeight = new TermQuery(new Term("foo", "bar")).createWeight(searcher, false, 1f);
            assertEquals(1, reader.leaves().size());
            LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
            Bits bits = Lucene.asSequentialAccessBits(leafReaderContext.reader().maxDoc(), termWeight.scorerSupplier(leafReaderContext));
            expectThrows(IndexOutOfBoundsException.class, () -> bits.get(-1));
            expectThrows(IndexOutOfBoundsException.class, () -> bits.get(leafReaderContext.reader().maxDoc()));
            assertTrue(bits.get(0));
            assertTrue(bits.get(0));
            assertFalse(bits.get(1));
            assertFalse(bits.get(1));
            expectThrows(IllegalArgumentException.class, () -> bits.get(0));
            assertTrue(bits.get(2));
            assertTrue(bits.get(2));
            expectThrows(IllegalArgumentException.class, () -> bits.get(1));
        }
        w.close();
        dir.close();
    }

    public void testMMapHackSupported() throws Exception {
        assertTrue("MMapDirectory does not support unmapping: " + MMapDirectory.UNMAP_NOT_SUPPORTED_REASON, MMapDirectory.UNMAP_SUPPORTED);
    }

    public void testWrapAllDocsLive() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD).setMergePolicy(new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, MatchAllDocsQuery::new, newMergePolicy()));
        IndexWriter writer = new IndexWriter(dir, config);
        int numDocs = between(1, 10);
        Set<String> liveDocs = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            Document doc = new Document();
            doc.add(new StringField("id", id, Store.YES));
            writer.addDocument(doc);
            liveDocs.add(id);
        }
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                String id = Integer.toString(i);
                Document doc = new Document();
                doc.add(new StringField("id", "v2-" + id, Store.YES));
                if (randomBoolean()) {
                    doc.add(Lucene.newSoftDeletesField());
                }
                writer.softUpdateDocument(new Term("id", id), doc, Lucene.newSoftDeletesField());
                liveDocs.add("v2-" + id);
            }
        }
        try (DirectoryReader unwrapped = DirectoryReader.open(writer)) {
            DirectoryReader reader = Lucene.wrapAllDocsLive(unwrapped);
            assertThat(reader.numDocs(), equalTo(liveDocs.size()));
            IndexSearcher searcher = new IndexSearcher(reader);
            Set<String> actualDocs = new HashSet<>();
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                actualDocs.add(reader.document(scoreDoc.doc).get("id"));
            }
            assertThat(actualDocs, equalTo(liveDocs));
        }
        IOUtils.close(writer, dir);
    }

    public void testWrapLiveDocsNotExposeAbortedDocuments() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD).setMergePolicy(new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, MatchAllDocsQuery::new, newMergePolicy()));
        IndexWriter writer = new IndexWriter(dir, config);
        int numDocs = between(1, 10);
        List<String> liveDocs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            Document doc = new Document();
            doc.add(new StringField("id", id, Store.YES));
            if (randomBoolean()) {
                doc.add(Lucene.newSoftDeletesField());
            }
            writer.addDocument(doc);
            liveDocs.add(id);
        }
        int abortedDocs = between(1, 10);
        for (int i = 0; i < abortedDocs; i++) {
            try {
                Document doc = new Document();
                doc.add(new StringField("id", "aborted-" + i, Store.YES));
                StringReader reader = new StringReader("");
                doc.add(new TextField("other", reader));
                reader.close();
                writer.addDocument(doc);
                fail("index should have failed");
            } catch (Exception ignored) {
            }
        }
        try (DirectoryReader unwrapped = DirectoryReader.open(writer)) {
            DirectoryReader reader = Lucene.wrapAllDocsLive(unwrapped);
            assertThat(reader.maxDoc(), equalTo(numDocs + abortedDocs));
            assertThat(reader.numDocs(), equalTo(liveDocs.size()));
            IndexSearcher searcher = new IndexSearcher(reader);
            List<String> actualDocs = new ArrayList<>();
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                actualDocs.add(reader.document(scoreDoc.doc).get("id"));
            }
            assertThat(actualDocs, equalTo(liveDocs));
        }
        IOUtils.close(writer, dir);
    }
}