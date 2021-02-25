package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.percentiles.ParsedPercentiles;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InternalTDigestPercentilesRanksTests extends InternalPercentilesRanksTestCase<InternalTDigestPercentileRanks> {

    @Override
    protected InternalTDigestPercentileRanks createTestInstance(String name, List<PipelineAggregator> aggregators, Map<String, Object> metadata, boolean keyed, DocValueFormat format, double[] percents, double[] values) {
        final TDigestState state = new TDigestState(100);
        Arrays.stream(values).forEach(state::add);
        assertEquals(state.centroidCount(), values.length);
        return new InternalTDigestPercentileRanks(name, percents, state, keyed, format, aggregators, metadata);
    }

    @Override
    protected void assertReduced(InternalTDigestPercentileRanks reduced, List<InternalTDigestPercentileRanks> inputs) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long totalCount = 0;
        for (InternalTDigestPercentileRanks ranks : inputs) {
            if (ranks.state.centroidCount() == 0) {
                continue;
            }
            totalCount += ranks.state.size();
            min = Math.min(ranks.state.quantile(0), min);
            max = Math.max(ranks.state.quantile(1), max);
        }
        assertEquals(totalCount, reduced.state.size());
        if (totalCount > 0) {
            assertEquals(reduced.state.quantile(0), min, 0d);
            assertEquals(reduced.state.quantile(1), max, 0d);
        }
    }

    @Override
    protected Reader<InternalTDigestPercentileRanks> instanceReader() {
        return InternalTDigestPercentileRanks::new;
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedTDigestPercentileRanks.class;
    }
}