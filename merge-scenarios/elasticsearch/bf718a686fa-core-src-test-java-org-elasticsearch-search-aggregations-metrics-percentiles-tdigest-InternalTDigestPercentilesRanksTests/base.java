package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalTDigestPercentilesRanksTests extends InternalAggregationTestCase<InternalTDigestPercentileRanks> {

    @Override
    protected InternalTDigestPercentileRanks createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        double[] cdfValues = new double[] { 0.5 };
        TDigestState state = new TDigestState(100);
        int numValues = randomInt(100);
        for (int i = 0; i < numValues; ++i) {
            state.add(randomDouble());
        }
        boolean keyed = false;
        DocValueFormat format = DocValueFormat.RAW;
        return new InternalTDigestPercentileRanks(name, cdfValues, state, keyed, format, pipelineAggregators, metaData);
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
}