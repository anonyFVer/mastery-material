package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import java.io.IOException;

final class PerThreadIDVersionAndSeqNoLookup {

    final String uidField;

    private final TermsEnum termsEnum;

    private PostingsEnum docsEnum;

    private final Object readerKey;

    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, String uidField) throws IOException {
        this.uidField = uidField;
        final Terms terms = reader.terms(uidField);
        if (terms == null) {
            final NumericDocValues softDeletesDV = reader.getNumericDocValues(Lucene.SOFT_DELETE_FIELD);
            final NumericDocValues tombstoneDV = reader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
            if (softDeletesDV == null || tombstoneDV == null) {
                throw new IllegalArgumentException("reader does not have _uid terms but not a no-op segment; " + "_soft_deletes [" + softDeletesDV + "], _tombstone [" + tombstoneDV + "]");
            }
            termsEnum = null;
        } else {
            termsEnum = terms.iterator();
        }
        if (reader.getNumericDocValues(VersionFieldMapper.NAME) == null) {
            throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field; _uid terms [" + terms + "]");
        }
        Object readerKey = null;
        assert (readerKey = reader.getCoreCacheHelper().getKey()) != null;
        this.readerKey = readerKey;
    }

    public DocIdAndVersion lookupVersion(BytesRef id, LeafReaderContext context) throws IOException {
        assert context.reader().getCoreCacheHelper().getKey().equals(readerKey) : "context's reader is not the same as the reader class was initialized on.";
        int docID = getDocID(id, context.reader().getLiveDocs());
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final NumericDocValues versions = context.reader().getNumericDocValues(VersionFieldMapper.NAME);
            if (versions == null) {
                throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field");
            }
            if (versions.advanceExact(docID) == false) {
                throw new IllegalArgumentException("Document [" + docID + "] misses the [" + VersionFieldMapper.NAME + "] field");
            }
            return new DocIdAndVersion(docID, versions.longValue(), context.reader(), context.docBase);
        } else {
            return null;
        }
    }

    private int getDocID(BytesRef id, Bits liveDocs) throws IOException {
        if (termsEnum != null && termsEnum.seekExact(id)) {
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            docsEnum = termsEnum.postings(docsEnum, 0);
            for (int d = docsEnum.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.nextDoc()) {
                if (liveDocs != null && liveDocs.get(d) == false) {
                    continue;
                }
                docID = d;
            }
            return docID;
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    DocIdAndSeqNo lookupSeqNo(BytesRef id, LeafReaderContext context) throws IOException {
        assert context.reader().getCoreCacheHelper().getKey().equals(readerKey) : "context's reader is not the same as the reader class was initialized on.";
        int docID = getDocID(id, context.reader().getLiveDocs());
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            NumericDocValues seqNos = context.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
            long seqNo;
            if (seqNos != null && seqNos.advanceExact(docID)) {
                seqNo = seqNos.longValue();
            } else {
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            }
            return new DocIdAndSeqNo(docID, seqNo, context);
        } else {
            return null;
        }
    }
}