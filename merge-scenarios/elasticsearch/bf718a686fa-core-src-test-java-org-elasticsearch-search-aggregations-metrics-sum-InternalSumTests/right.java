package org.elasticsearch.search.aggregations.metrics.sum;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;
import java.util.List;
import java.util.Map;

public class InternalSumTests extends InternalAggregationTestCase<InternalSum> {

    @Override
    protected InternalSum createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalSum(name, randomDouble(), DocValueFormat.RAW, pipelineAggregators, metaData);
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
}