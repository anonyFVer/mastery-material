package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalSimpleValueTests extends InternalAggregationTestCase<InternalSimpleValue> {

    @Override
    protected InternalSimpleValue createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        double value = randomDoubleBetween(0, 100000, true);
        return new InternalSimpleValue(name, value, formatter, pipelineAggregators, metaData);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", Collections.emptyList(), null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalSimpleValue reduced, List<InternalSimpleValue> inputs) {
    }

    @Override
    protected Reader<InternalSimpleValue> instanceReader() {
        return InternalSimpleValue::new;
    }
}