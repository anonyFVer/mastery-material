package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalDerivativeTests extends InternalAggregationTestCase<InternalDerivative> {

    @Override
    protected InternalDerivative createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        double value = randomDoubleBetween(0, 100000, true);
        double normalizationFactor = randomDoubleBetween(0, 100000, true);
        return new InternalDerivative(name, value, normalizationFactor, formatter, pipelineAggregators, metaData);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", Collections.emptyList(), null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalDerivative reduced, List<InternalDerivative> inputs) {
    }

    @Override
    protected Reader<InternalDerivative> instanceReader() {
        return InternalDerivative::new;
    }
}