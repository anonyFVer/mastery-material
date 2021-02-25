package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalMaxTests extends InternalAggregationTestCase<InternalMax> {

    @Override
    protected InternalMax createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalMax(name, randomDouble(), randomNumericDocValueFormat(), pipelineAggregators, metaData);
    }

    @Override
    protected Reader<InternalMax> instanceReader() {
        return InternalMax::new;
    }

    @Override
    protected void assertReduced(InternalMax reduced, List<InternalMax> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMax::value).max().getAsDouble(), reduced.value(), 0);
    }
}