package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalDerivativeTests extends InternalAggregationTestCase<InternalDerivative> {

    @Override
    protected InternalDerivative createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        double value = frequently() ? randomDoubleBetween(-100000, 100000, true) : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN });
        double normalizationFactor = frequently() ? randomDoubleBetween(0, 100000, true) : 0;
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

    @Override
    protected void assertFromXContent(InternalDerivative derivative, ParsedAggregation parsedAggregation) {
        ParsedDerivative parsed = ((ParsedDerivative) parsedAggregation);
        if (Double.isInfinite(derivative.getValue()) == false && Double.isNaN(derivative.getValue()) == false) {
            assertEquals(derivative.getValue(), parsed.value(), Double.MIN_VALUE);
            assertEquals(derivative.getValueAsString(), parsed.getValueAsString());
        } else {
            assertEquals(parsed.value(), Double.NaN, Double.MIN_VALUE);
        }
    }
}