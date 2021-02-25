package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.joda.time.DateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class InternalDateHistogramTests extends InternalMultiBucketAggregationTestCase<InternalDateHistogram> {

    private boolean keyed;

    private DocValueFormat format;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keyed = randomBoolean();
        format = randomNumericDocValueFormat();
    }

    @Override
    protected InternalDateHistogram createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, InternalAggregations aggregations) {
        int nbBuckets = randomInt(10);
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(nbBuckets);
        long startingDate = System.currentTimeMillis();
        long interval = randomIntBetween(1, 3);
        long intervalMillis = randomFrom(timeValueSeconds(interval), timeValueMinutes(interval), timeValueHours(interval)).getMillis();
        for (int i = 0; i < nbBuckets; i++) {
            long key = startingDate + (intervalMillis * i);
            buckets.add(i, new InternalDateHistogram.Bucket(key, randomIntBetween(1, 100), keyed, format, aggregations));
        }
        InternalOrder order = (InternalOrder) randomFrom(InternalHistogram.Order.KEY_ASC, InternalHistogram.Order.KEY_DESC);
        return new InternalDateHistogram(name, buckets, order, 1, 0L, null, format, keyed, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalDateHistogram reduced, List<InternalDateHistogram> inputs) {
        Map<Long, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute(((DateTime) bucket.getKey()).getMillis(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
            }
        }
        Map<Long, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute(((DateTime) bucket.getKey()).getMillis(), (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Writeable.Reader<InternalDateHistogram> instanceReader() {
        return InternalDateHistogram::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedDateHistogram.class;
    }
}