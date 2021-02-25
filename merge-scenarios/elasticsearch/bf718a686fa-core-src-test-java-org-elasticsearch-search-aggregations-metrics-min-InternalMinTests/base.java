package org.elasticsearch.search.aggregations.metrics.min;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalMinTests extends InternalAggregationTestCase<InternalMin> {

    @Override
    protected InternalMin createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalMin(name, randomDouble(), randomNumericDocValueFormat(), pipelineAggregators, metaData);
    }

    @Override
    protected Reader<InternalMin> instanceReader() {
        return InternalMin::new;
    }

    @Override
    protected void assertReduced(InternalMin reduced, List<InternalMin> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMin::value).min().getAsDouble(), reduced.value(), 0);
    }
}