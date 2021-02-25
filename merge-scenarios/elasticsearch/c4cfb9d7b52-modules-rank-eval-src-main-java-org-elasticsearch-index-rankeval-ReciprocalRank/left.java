package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.rankeval.PrecisionAtN.Rating;
import org.elasticsearch.index.rankeval.PrecisionAtN.RatingMapping;
import org.elasticsearch.search.SearchHit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.naming.directory.SearchResult;

public class ReciprocalRank extends RankedListQualityMetric<ReciprocalRank> {

    public static final String NAME = "reciprocal_rank";

    public static final int DEFAULT_MAX_ACCEPTABLE_RANK = 10;

    private int maxAcceptableRank = DEFAULT_MAX_ACCEPTABLE_RANK;

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

    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Set<RatedDocumentKey> relevantDocIds = new HashSet<>();
        Set<RatedDocumentKey> irrelevantDocIds = new HashSet<>();
        for (RatedDocument doc : ratedDocs) {
            if (Rating.RELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                relevantDocIds.add(doc.getKey());
            } else if (Rating.IRRELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                irrelevantDocIds.add(doc.getKey());
            }
        }
        Collection<RatedDocumentKey> unknownDocIds = new ArrayList<>();
        int firstRelevant = -1;
        boolean found = false;
        for (int i = 0; i < hits.length; i++) {
            RatedDocumentKey id = new RatedDocumentKey(hits[i].getIndex(), hits[i].getType(), hits[i].getId());
            if (relevantDocIds.contains(id)) {
                if (found == false && i < maxAcceptableRank) {
                    firstRelevant = i + 1;
                    found = true;
                }
            } else {
                unknownDocIds.add(id);
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

    private static final ObjectParser<ReciprocalRank, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("reciprocal_rank", () -> new ReciprocalRank());

    static {
        PARSER.declareInt(ReciprocalRank::setMaxAcceptableRank, MAX_RANK_FIELD);
    }

    @Override
    public ReciprocalRank fromXContent(XContentParser parser, ParseFieldMatcher matcher) {
        return ReciprocalRank.fromXContent(parser, new ParseFieldMatcherSupplier() {

            @Override
            public ParseFieldMatcher getParseFieldMatcher() {
                return matcher;
            }
        });
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

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ReciprocalRank other = (ReciprocalRank) obj;
        return Objects.equals(maxAcceptableRank, other.maxAcceptableRank);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), maxAcceptableRank);
    }
}