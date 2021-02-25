package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import java.util.List;
import java.util.Map;

public class InternalValueCountTests extends InternalAggregationTestCase<InternalValueCount> {

    @Override
    protected InternalValueCount createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalValueCount(name, randomIntBetween(0, 100), pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalValueCount reduced, List<InternalValueCount> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalValueCount::getValue).sum(), reduced.getValue(), 0);
    }

    @Override
    protected Writeable.Reader<InternalValueCount> instanceReader() {
        return InternalValueCount::new;
    }

    @Override
    protected void assertFromXContent(InternalValueCount valueCount, ParsedAggregation parsedAggregation) {
        assertEquals(valueCount.getValue(), ((ParsedValueCount) parsedAggregation).getValue(), 0);
        assertEquals(valueCount.getValueAsString(), ((ParsedValueCount) parsedAggregation).getValueAsString());
    }
}