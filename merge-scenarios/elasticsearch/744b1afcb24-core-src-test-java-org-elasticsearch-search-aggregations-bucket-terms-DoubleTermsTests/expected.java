package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DoubleTermsTests extends InternalTermsTestCase {

    @Override
    protected InternalTerms<?, ?> createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, InternalAggregations aggregations, boolean showTermDocCountError, long docCountError) {
        BucketOrder order = BucketOrder.count(false);
        long minDocCount = 1;
        int requiredSize = 3;
        int shardSize = requiredSize + 2;
        DocValueFormat format = randomNumericDocValueFormat();
        long otherDocCount = 0;
        List<DoubleTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomInt(shardSize);
        Set<Double> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            double term = randomValueOtherThanMany(d -> terms.add(d) == false, random()::nextDouble);
            int docCount = randomIntBetween(1, 100);
            buckets.add(new DoubleTerms.Bucket(term, docCount, aggregations, showTermDocCountError, docCountError, format));
        }
        return new DoubleTerms(name, order, requiredSize, minDocCount, pipelineAggregators, metaData, format, shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Reader<InternalTerms<?, ?>> instanceReader() {
        return DoubleTerms::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedDoubleTerms.class;
    }
}