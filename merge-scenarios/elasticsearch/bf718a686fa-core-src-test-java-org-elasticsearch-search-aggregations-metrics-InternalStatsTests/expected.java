package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import java.util.List;
import java.util.Map;

public class InternalStatsTests extends InternalAggregationTestCase<InternalStats> {

    @Override
    protected InternalStats createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        long count = frequently() ? randomIntBetween(1, Integer.MAX_VALUE) : 0;
        double min = randomDoubleBetween(-1000000, 1000000, true);
        double max = randomDoubleBetween(-1000000, 1000000, true);
        double sum = randomDoubleBetween(-1000000, 1000000, true);
        DocValueFormat format = randomNumericDocValueFormat();
        return createInstance(name, count, sum, min, max, format, pipelineAggregators, metaData);
    }

    protected InternalStats createInstance(String name, long count, double sum, double min, double max, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalStats(name, count, sum, min, max, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalStats reduced, List<InternalStats> inputs) {
        long expectedCount = 0;
        double expectedSum = 0;
        double expectedMin = Double.POSITIVE_INFINITY;
        double expectedMax = Double.NEGATIVE_INFINITY;
        for (InternalStats stats : inputs) {
            expectedCount += stats.getCount();
            if (Double.compare(stats.getMin(), expectedMin) < 0) {
                expectedMin = stats.getMin();
            }
            if (Double.compare(stats.getMax(), expectedMax) > 0) {
                expectedMax = stats.getMax();
            }
            expectedSum += stats.getSum();
        }
        assertEquals(expectedCount, reduced.getCount());
        assertEquals(expectedSum, reduced.getSum(), 1e-7);
        assertEquals(expectedMin, reduced.getMin(), 0d);
        assertEquals(expectedMax, reduced.getMax(), 0d);
    }

    @Override
    protected void assertFromXContent(InternalStats aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedStats);
        ParsedStats parsed = (ParsedStats) parsedAggregation;
        assertStats(aggregation, parsed);
    }

    static void assertStats(InternalStats aggregation, ParsedStats parsed) {
        long count = aggregation.getCount();
        assertEquals(count, parsed.getCount());
        assertEquals(count > 0 ? aggregation.getMin() : Double.POSITIVE_INFINITY, parsed.getMin(), 0);
        assertEquals(count > 0 ? aggregation.getMax() : Double.NEGATIVE_INFINITY, parsed.getMax(), 0);
        assertEquals(count > 0 ? aggregation.getSum() : 0, parsed.getSum(), 0);
        assertEquals(count > 0 ? aggregation.getAvg() : 0, parsed.getAvg(), 0);
        if (count > 0) {
            assertEquals(aggregation.getMinAsString(), parsed.getMinAsString());
            assertEquals(aggregation.getMaxAsString(), parsed.getMaxAsString());
            assertEquals(aggregation.getSumAsString(), parsed.getSumAsString());
            assertEquals(aggregation.getAvgAsString(), parsed.getAvgAsString());
        }
    }

    @Override
    protected Writeable.Reader<InternalStats> instanceReader() {
        return InternalStats::new;
    }
}