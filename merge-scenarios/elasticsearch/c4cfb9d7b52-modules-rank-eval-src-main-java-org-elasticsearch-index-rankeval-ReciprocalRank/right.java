package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.naming.directory.SearchResult;

public class ReciprocalRank extends RankedListQualityMetric {

    public static final String NAME = "reciprocal_rank";

    public static final int DEFAULT_MAX_ACCEPTABLE_RANK = 10;

    private int maxAcceptableRank = DEFAULT_MAX_ACCEPTABLE_RANK;

    private int relevantRatingThreshhold = 1;

    public ReciprocalRank() {
    }

    public ReciprocalRank(int maxAcceptableRank) {
        if (maxAcceptableRank <= 0) {
            throw new IllegalArgumentException("maximal acceptable rank needs to be positive but was [" + maxAcceptableRank + "]");
        }
        this.maxAcceptableRank = maxAcceptableRank;
    }

    public ReciprocalRank(StreamInput in) throws IOException {
        this.maxAcceptableRank = in.readInt();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public void setMaxAcceptableRank(int maxAcceptableRank) {
        if (maxAcceptableRank <= 0) {
            throw new IllegalArgumentException("maximal acceptable rank needs to be positive but was [" + maxAcceptableRank + "]");
        }
        this.maxAcceptableRank = maxAcceptableRank;
    }

    public int getMaxAcceptableRank() {
        return this.maxAcceptableRank;
    }

    public void setRelevantRatingThreshhold(int threshold) {
        this.relevantRatingThreshhold = threshold;
    }

    public int getRelevantRatingThreshold() {
        return relevantRatingThreshhold;
    }

    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Set<RatedDocumentKey> relevantDocIds = new HashSet<>();
        Set<RatedDocumentKey> irrelevantDocIds = new HashSet<>();
        for (RatedDocument doc : ratedDocs) {
            if (doc.getRating() >= this.relevantRatingThreshhold) {
                relevantDocIds.add(doc.getKey());
            } else {
                irrelevantDocIds.add(doc.getKey());
            }
        }
        Collection<RatedDocumentKey> unknownDocIds = new ArrayList<>();
        int firstRelevant = -1;
        boolean found = false;
        for (int i = 0; i < hits.length; i++) {
            RatedDocumentKey key = new RatedDocumentKey(hits[i].getIndex(), hits[i].getType(), hits[i].getId());
            if (relevantDocIds.contains(key)) {
                if (found == false && i < maxAcceptableRank) {
                    firstRelevant = i + 1;
                    found = true;
                }
            } else {
                unknownDocIds.add(key);
            }
        }
        double reciprocalRank = (firstRelevant == -1) ? 0 : 1.0d / firstRelevant;
        return new EvalQueryQuality(reciprocalRank, unknownDocIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxAcceptableRank);
    }

    private static final ParseField MAX_RANK_FIELD = new ParseField("max_acceptable_rank");

    private static final ParseField RELEVANT_RATING_FIELD = new ParseField("relevant_rating_threshold");

    private static final ObjectParser<ReciprocalRank, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("reciprocal_rank", () -> new ReciprocalRank());

    static {
        PARSER.declareInt(ReciprocalRank::setMaxAcceptableRank, MAX_RANK_FIELD);
        PARSER.declareInt(ReciprocalRank::setRelevantRatingThreshhold, RELEVANT_RATING_FIELD);
    }

    public static ReciprocalRank fromXContent(XContentParser parser, ParseFieldMatcherSupplier matcher) {
        return PARSER.apply(parser, matcher);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(MAX_RANK_FIELD.getPreferredName(), this.maxAcceptableRank);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}