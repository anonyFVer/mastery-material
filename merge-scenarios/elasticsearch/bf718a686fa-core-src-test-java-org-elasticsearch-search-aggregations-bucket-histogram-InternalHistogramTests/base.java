package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InternalHistogramTests extends InternalAggregationTestCase<InternalHistogram> {

    @Override
    protected InternalHistogram createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        final boolean keyed = randomBoolean();
        final DocValueFormat format = DocValueFormat.RAW;
        final int base = randomInt(50) - 30;
        final int numBuckets = randomInt(10);
        final int interval = randomIntBetween(1, 3);
        List<InternalHistogram.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < numBuckets; ++i) {
            final int docCount = TestUtil.nextInt(random(), 1, 50);
            buckets.add(new InternalHistogram.Bucket(base + i * interval, docCount, keyed, format, InternalAggregations.EMPTY));
        }
        return new InternalHistogram(name, buckets, (InternalOrder) InternalHistogram.Order.KEY_ASC, 1, null, format, keyed, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalHistogram reduced, List<InternalHistogram> inputs) {
        Map<Double, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute((Double) bucket.getKey(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
            }
        }
        Map<Double, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute((Double) bucket.getKey(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Reader<InternalHistogram> instanceReader() {
        return InternalHistogram::new;
    }
}