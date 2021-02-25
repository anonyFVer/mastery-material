package org.elasticsearch.search.aggregations.metrics.sum;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import java.util.List;
import java.util.Map;

public class InternalSumTests extends InternalAggregationTestCase<InternalSum> {

    @Override
    protected InternalSum createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        double value = frequently() ? randomDouble() : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY });
        DocValueFormat formatter = randomFrom(new DocValueFormat.Decimal("###.##"), DocValueFormat.BOOLEAN, DocValueFormat.RAW);
        return new InternalSum(name, value, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected Writeable.Reader<InternalSum> instanceReader() {
        return InternalSum::new;
    }

    @Override
    protected void assertReduced(InternalSum reduced, List<InternalSum> inputs) {
        double expectedSum = inputs.stream().mapToDouble(InternalSum::getValue).sum();
        assertEquals(expectedSum, reduced.getValue(), 0.0001d);
    }

    @Override
    protected void assertFromXContent(InternalSum sum, ParsedAggregation parsedAggregation) {
        ParsedSum parsed = ((ParsedSum) parsedAggregation);
        assertEquals(sum.getValue(), parsed.getValue(), Double.MIN_VALUE);
        assertEquals(sum.getValueAsString(), parsed.getValueAsString());
    }
}