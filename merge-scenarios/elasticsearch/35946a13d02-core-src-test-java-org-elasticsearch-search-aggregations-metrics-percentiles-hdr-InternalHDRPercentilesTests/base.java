package org.elasticsearch.search.aggregations.metrics.percentiles.hdr;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentilesTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InternalHDRPercentilesTests extends InternalPercentilesTestCase<InternalHDRPercentiles> {

    @Override
    protected InternalHDRPercentiles createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, boolean keyed, DocValueFormat format, double[] percents, double[] values) {
        final DoubleHistogram state = new DoubleHistogram(3);
        Arrays.stream(values).forEach(state::recordValue);
        return new InternalHDRPercentiles(name, percents, state, keyed, format, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalHDRPercentiles reduced, List<InternalHDRPercentiles> inputs) {
        long totalCount = 0;
        for (InternalHDRPercentiles ranks : inputs) {
            totalCount += ranks.state.getTotalCount();
        }
        assertEquals(totalCount, reduced.state.getTotalCount());
    }

    @Override
    protected Writeable.Reader<InternalHDRPercentiles> instanceReader() {
        return InternalHDRPercentiles::new;
    }
}