package org.elasticsearch.search.aggregations.metrics.percentiles.hdr;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentilesRanksTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.ParsedPercentiles;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InternalHDRPercentilesRanksTests extends InternalPercentilesRanksTestCase<InternalHDRPercentileRanks> {

    @Override
    protected InternalHDRPercentileRanks createTestInstance(String name, List<PipelineAggregator> aggregators, Map<String, Object> metadata, boolean keyed, DocValueFormat format, double[] percents, double[] values) {
        final DoubleHistogram state = new DoubleHistogram(3);
        Arrays.stream(values).forEach(state::recordValue);
        return new InternalHDRPercentileRanks(name, percents, state, keyed, format, aggregators, metadata);
    }

    @Override
    protected void assertReduced(InternalHDRPercentileRanks reduced, List<InternalHDRPercentileRanks> inputs) {
        long totalCount = 0;
        for (InternalHDRPercentileRanks ranks : inputs) {
            totalCount += ranks.state.getTotalCount();
        }
        assertEquals(totalCount, reduced.state.getTotalCount());
    }

    @Override
    protected Reader<InternalHDRPercentileRanks> instanceReader() {
        return InternalHDRPercentileRanks::new;
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedHDRPercentileRanks.class;
    }
}