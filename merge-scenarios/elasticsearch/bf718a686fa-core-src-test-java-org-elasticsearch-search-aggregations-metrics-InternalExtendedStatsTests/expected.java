package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats.Bounds;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.junit.Before;
import java.util.List;
import java.util.Map;

public class InternalExtendedStatsTests extends InternalAggregationTestCase<InternalExtendedStats> {

    protected double sigma;

    @Before
    public void randomSigma() {
        this.sigma = randomDoubleBetween(0, 10, true);
    }

    @Override
    protected InternalExtendedStats createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        long count = frequently() ? randomIntBetween(1, Integer.MAX_VALUE) : 0;
        double min = randomDoubleBetween(-1000000, 1000000, true);
        double max = randomDoubleBetween(-1000000, 1000000, true);
        double sum = randomDoubleBetween(-1000000, 1000000, true);
        DocValueFormat format = randomNumericDocValueFormat();
        return createInstance(name, count, sum, min, max, randomDoubleBetween(0, 1000000, true), sigma, format, pipelineAggregators, metaData);
    }

    protected InternalExtendedStats createInstance(String name, long count, double sum, double min, double max, double sumOfSqrs, double sigma, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs, sigma, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalExtendedStats reduced, List<InternalExtendedStats> inputs) {
        long expectedCount = 0;
        double expectedSum = 0;
        double expectedSumOfSquare = 0;
        double expectedMin = Double.POSITIVE_INFINITY;
        double expectedMax = Double.NEGATIVE_INFINITY;
        for (InternalExtendedStats stats : inputs) {
            assertEquals(sigma, stats.getSigma(), 0);
            expectedCount += stats.getCount();
            if (Double.compare(stats.getMin(), expectedMin) < 0) {
                expectedMin = stats.getMin();
            }
            if (Double.compare(stats.getMax(), expectedMax) > 0) {
                expectedMax = stats.getMax();
            }
            expectedSum += stats.getSum();
            expectedSumOfSquare += stats.getSumOfSquares();
        }
        assertEquals(sigma, reduced.getSigma(), 0);
        assertEquals(expectedCount, reduced.getCount());
        assertEquals(expectedSum, reduced.getSum(), 1e-07);
        assertEquals(expectedMin, reduced.getMin(), 0d);
        assertEquals(expectedMax, reduced.getMax(), 0d);
        assertEquals(expectedSumOfSquare, reduced.getSumOfSquares(), 1e-07);
    }

    @Override
    protected void assertFromXContent(InternalExtendedStats aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedExtendedStats);
        ParsedExtendedStats parsed = (ParsedExtendedStats) parsedAggregation;
        InternalStatsTests.assertStats(aggregation, parsed);
        long count = aggregation.getCount();
        assertEquals(count > 0 ? aggregation.getSumOfSquares() : 0, parsed.getSumOfSquares(), 0);
        assertEquals(count > 0 ? aggregation.getVariance() : 0, parsed.getVariance(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviation() : 0, parsed.getStdDeviation(), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationBound(Bounds.LOWER) : 0, parsed.getStdDeviationBound(Bounds.LOWER), 0);
        assertEquals(count > 0 ? aggregation.getStdDeviationBound(Bounds.UPPER) : 0, parsed.getStdDeviationBound(Bounds.UPPER), 0);
        if (count > 0) {
            assertEquals(aggregation.getSumOfSquaresAsString(), parsed.getSumOfSquaresAsString());
            assertEquals(aggregation.getVarianceAsString(), parsed.getVarianceAsString());
            assertEquals(aggregation.getStdDeviationAsString(), parsed.getStdDeviationAsString());
            assertEquals(aggregation.getStdDeviationBoundAsString(Bounds.LOWER), parsed.getStdDeviationBoundAsString(Bounds.LOWER));
            assertEquals(aggregation.getStdDeviationBoundAsString(Bounds.UPPER), parsed.getStdDeviationBoundAsString(Bounds.UPPER));
        }
    }

    @Override
    protected Writeable.Reader<InternalExtendedStats> instanceReader() {
        return InternalExtendedStats::new;
    }
}