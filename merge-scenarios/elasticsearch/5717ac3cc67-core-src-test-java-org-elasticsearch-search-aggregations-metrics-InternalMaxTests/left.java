package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalMaxTests extends InternalAggregationTestCase<InternalMax> {

    @Override
    protected InternalMax createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        double value = frequently() ? randomDouble() : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY });
        DocValueFormat formatter = randomFrom(new DocValueFormat.Decimal("###.##"), DocValueFormat.BOOLEAN, DocValueFormat.RAW);
        return new InternalMax(name, value, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected Reader<InternalMax> instanceReader() {
        return InternalMax::new;
    }

    @Override
    protected void assertReduced(InternalMax reduced, List<InternalMax> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMax::value).max().getAsDouble(), reduced.value(), 0);
    }

    @Override
    protected void assertFromXContent(InternalMax max, ParsedAggregation parsedAggregation) {
        ParsedMax parsed = ((ParsedMax) parsedAggregation);
        if (Double.isInfinite(max.getValue()) == false) {
            assertEquals(max.getValue(), parsed.getValue(), Double.MIN_VALUE);
            assertEquals(max.getValueAsString(), parsed.getValueAsString());
        } else {
            assertEquals(parsed.getValue(), Double.NEGATIVE_INFINITY, 0);
        }
    }
}