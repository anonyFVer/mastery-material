package org.elasticsearch.search.aggregations.metrics.min;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalMinTests extends InternalAggregationTestCase<InternalMin> {

    @Override
    protected InternalMin createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        double value = frequently() ? randomDouble() : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY });
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalMin(name, value, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected Reader<InternalMin> instanceReader() {
        return InternalMin::new;
    }

    @Override
    protected void assertReduced(InternalMin reduced, List<InternalMin> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMin::value).min().getAsDouble(), reduced.value(), 0);
    }

    @Override
    protected void assertFromXContent(InternalMin min, ParsedAggregation parsedAggregation) {
        ParsedMin parsed = ((ParsedMin) parsedAggregation);
        if (Double.isInfinite(min.getValue()) == false) {
            assertEquals(min.getValue(), parsed.getValue(), Double.MIN_VALUE);
            assertEquals(min.getValueAsString(), parsed.getValueAsString());
        } else {
            assertEquals(parsed.getValue(), Double.POSITIVE_INFINITY, 0);
        }
    }
}