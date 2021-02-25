package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StringTermsTests extends InternalTermsTestCase {

    @Override
    protected InternalTerms<?, ?> createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        Terms.Order order = Terms.Order.count(false);
        long minDocCount = 1;
        int requiredSize = 3;
        int shardSize = requiredSize + 2;
        DocValueFormat format = DocValueFormat.RAW;
        boolean showTermDocCountError = false;
        long docCountError = -1;
        long otherDocCount = 0;
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomInt(shardSize);
        Set<BytesRef> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            BytesRef term = randomValueOtherThanMany(b -> terms.add(b) == false, () -> new BytesRef(randomAlphaOfLength(10)));
            int docCount = randomIntBetween(1, 100);
            buckets.add(new StringTerms.Bucket(term, docCount, InternalAggregations.EMPTY, showTermDocCountError, docCountError, format));
        }
        return new StringTerms(name, order, requiredSize, minDocCount, pipelineAggregators, metaData, format, shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Reader<InternalTerms<?, ?>> instanceReader() {
        return StringTerms::new;
    }
}