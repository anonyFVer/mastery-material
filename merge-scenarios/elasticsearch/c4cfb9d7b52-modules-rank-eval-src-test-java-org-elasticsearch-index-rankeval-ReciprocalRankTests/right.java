package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.rankeval.PrecisionAtN.Rating;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.test.ESTestCase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import static java.util.Collections.emptyList;

public class ReciprocalRankTests extends ESTestCase {

    public void testMaxAcceptableRank() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        assertEquals(ReciprocalRank.DEFAULT_MAX_ACCEPTABLE_RANK, reciprocalRank.getMaxAcceptableRank());
        int maxRank = randomIntBetween(1, 100);
        reciprocalRank.setMaxAcceptableRank(maxRank);
        assertEquals(maxRank, reciprocalRank.getMaxAcceptableRank());
        InternalSearchHit[] hits = new InternalSearchHit[10];
        for (int i = 0; i < 10; i++) {
            hits[i] = new InternalSearchHit(i, Integer.toString(i), new Text("type"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        List<RatedDocument> ratedDocs = new ArrayList<>();
        int relevantAt = 5;
        for (int i = 0; i < 10; i++) {
            if (i == relevantAt) {
                ratedDocs.add(new RatedDocument(new RatedDocumentKey("test", "type", Integer.toString(i)), Rating.RELEVANT.ordinal()));
            } else {
                ratedDocs.add(new RatedDocument(new RatedDocumentKey("test", "type", Integer.toString(i)), Rating.IRRELEVANT.ordinal()));
            }
        }
        int rankAtFirstRelevant = relevantAt + 1;
        EvalQueryQuality evaluation = reciprocalRank.evaluate(hits, ratedDocs);
        if (rankAtFirstRelevant <= maxRank) {
            assertEquals(1.0 / rankAtFirstRelevant, evaluation.getQualityLevel(), Double.MIN_VALUE);
            reciprocalRank = new ReciprocalRank(rankAtFirstRelevant - 1);
            evaluation = reciprocalRank.evaluate(hits, ratedDocs);
            assertEquals(0.0, evaluation.getQualityLevel(), Double.MIN_VALUE);
        } else {
            assertEquals(0.0, evaluation.getQualityLevel(), Double.MIN_VALUE);
        }
    }

    public void testEvaluationOneRelevantInResults() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        InternalSearchHit[] hits = new InternalSearchHit[10];
        for (int i = 0; i < 10; i++) {
            hits[i] = new InternalSearchHit(i, Integer.toString(i), new Text("type"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        List<RatedDocument> ratedDocs = new ArrayList<>();
        int relevantAt = randomIntBetween(0, 9);
        for (int i = 0; i <= 20; i++) {
            if (i == relevantAt) {
                ratedDocs.add(new RatedDocument(new RatedDocumentKey("test", "type", Integer.toString(i)), Rating.RELEVANT.ordinal()));
            } else {
                ratedDocs.add(new RatedDocument(new RatedDocumentKey("test", "type", Integer.toString(i)), Rating.IRRELEVANT.ordinal()));
            }
        }
        EvalQueryQuality evaluation = reciprocalRank.evaluate(hits, ratedDocs);
        assertEquals(1.0 / (relevantAt + 1), evaluation.getQualityLevel(), Double.MIN_VALUE);
    }

    public void testPrecisionAtFiveRelevanceThreshold() throws IOException, InterruptedException, ExecutionException {
        List<RatedDocument> rated = new ArrayList<>();
        rated.add(new RatedDocument(new RatedDocumentKey("test", "testtype", "0"), 0));
        rated.add(new RatedDocument(new RatedDocumentKey("test", "testtype", "1"), 1));
        rated.add(new RatedDocument(new RatedDocumentKey("test", "testtype", "2"), 2));
        rated.add(new RatedDocument(new RatedDocumentKey("test", "testtype", "3"), 3));
        rated.add(new RatedDocument(new RatedDocumentKey("test", "testtype", "4"), 4));
        InternalSearchHit[] hits = new InternalSearchHit[5];
        for (int i = 0; i < 5; i++) {
            hits[i] = new InternalSearchHit(i, i + "", new Text("testtype"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        reciprocalRank.setRelevantRatingThreshhold(2);
        assertEquals((double) 1 / 3, reciprocalRank.evaluate(hits, rated).getQualityLevel(), 0.00001);
    }

    public void testCombine() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        Vector<EvalQueryQuality> partialResults = new Vector<>(3);
        partialResults.add(new EvalQueryQuality(0.5, emptyList()));
        partialResults.add(new EvalQueryQuality(1.0, emptyList()));
        partialResults.add(new EvalQueryQuality(0.75, emptyList()));
        assertEquals(0.75, reciprocalRank.combine(partialResults), Double.MIN_VALUE);
    }

    public void testEvaluationNoRelevantInResults() {
        ReciprocalRank reciprocalRank = new ReciprocalRank();
        InternalSearchHit[] hits = new InternalSearchHit[10];
        for (int i = 0; i < 10; i++) {
            hits[i] = new InternalSearchHit(i, Integer.toString(i), new Text("type"), Collections.emptyMap());
            hits[i].shard(new SearchShardTarget("testnode", new Index("test", "uuid"), 0));
        }
        List<RatedDocument> ratedDocs = new ArrayList<>();
        EvalQueryQuality evaluation = reciprocalRank.evaluate(hits, ratedDocs);
        assertEquals(0.0, evaluation.getQualityLevel(), Double.MIN_VALUE);
    }
}