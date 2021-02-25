package org.elasticsearch.search.aggregations.metrics.percentiles.hdr;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalHDRPercentilesRanksTests extends InternalAggregationTestCase<InternalHDRPercentileRanks> {

    @Override
    protected InternalHDRPercentileRanks createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        double[] cdfValues = new double[] { 0.5 };
        int numberOfSignificantValueDigits = 3;
        DoubleHistogram state = new DoubleHistogram(numberOfSignificantValueDigits);
        int numValues = randomInt(100);
        for (int i = 0; i < numValues; ++i) {
            state.recordValue(randomDouble());
        }
        boolean keyed = false;
        DocValueFormat format = DocValueFormat.RAW;
        return new InternalHDRPercentileRanks(name, cdfValues, state, keyed, format, pipelineAggregators, metaData);
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
}