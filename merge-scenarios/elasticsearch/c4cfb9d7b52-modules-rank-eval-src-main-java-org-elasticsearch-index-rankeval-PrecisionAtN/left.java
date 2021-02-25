package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import javax.naming.directory.SearchResult;

public class PrecisionAtN extends RankedListQualityMetric<PrecisionAtN> {

    private int n;

    public static final String NAME = "precisionatn";

    public PrecisionAtN(StreamInput in) throws IOException {
        n = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(n);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public PrecisionAtN() {
        this.n = 10;
    }

    public PrecisionAtN(int n) {
        this.n = n;
    }

    public int getN() {
        return n;
    }

    private static final ParseField SIZE_FIELD = new ParseField("size");

    private static final ConstructingObjectParser<PrecisionAtN, ParseFieldMatcherSupplier> PARSER = new ConstructingObjectParser<>("precision_at", a -> new PrecisionAtN((Integer) a[0]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SIZE_FIELD);
    }

    @Override
    public PrecisionAtN fromXContent(XContentParser parser, ParseFieldMatcher matcher) {
        return PrecisionAtN.fromXContent(parser, new ParseFieldMatcherSupplier() {

            @Override
            public ParseFieldMatcher getParseFieldMatcher() {
                return matcher;
            }
        });
    }

    public static PrecisionAtN fromXContent(XContentParser parser, ParseFieldMatcherSupplier matcher) {
        return PARSER.apply(parser, matcher);
    }

    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Collection<RatedDocumentKey> relevantDocIds = new ArrayList<>();
        Collection<RatedDocumentKey> irrelevantDocIds = new ArrayList<>();
        for (RatedDocument doc : ratedDocs) {
            if (Rating.RELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                relevantDocIds.add(doc.getKey());
            } else if (Rating.IRRELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                irrelevantDocIds.add(doc.getKey());
            }
        }
        int good = 0;
        int bad = 0;
        Collection<RatedDocumentKey> unknownDocIds = new ArrayList<>();
        for (int i = 0; (i < n && i < hits.length); i++) {
            RatedDocumentKey hitKey = new RatedDocumentKey(hits[i].getIndex(), hits[i].getType(), hits[i].getId());
            if (relevantDocIds.contains(hitKey)) {
                good++;
            } else if (irrelevantDocIds.contains(hitKey)) {
                bad++;
            } else {
                unknownDocIds.add(hitKey);
            }
        }
        double precision = (double) good / (good + bad);
        return new EvalQueryQuality(precision, unknownDocIds);
    }

    public enum Rating {

        IRRELEVANT, RELEVANT
    }

    public static class RatingMapping {

        public static Integer mapFrom(Rating rating) {
            if (Rating.RELEVANT.equals(rating)) {
                return 1;
            }
            return 0;
        }

        public static Rating mapTo(Integer rating) {
            if (rating == 1) {
                return Rating.RELEVANT;
            }
            return Rating.IRRELEVANT;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(SIZE_FIELD.getPreferredName(), this.n);
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
        PrecisionAtN other = (PrecisionAtN) obj;
        return Objects.equals(n, other.n);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), n);
    }
}