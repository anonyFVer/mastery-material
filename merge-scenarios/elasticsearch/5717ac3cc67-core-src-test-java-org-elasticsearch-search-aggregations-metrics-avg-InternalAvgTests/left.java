package org.elasticsearch.search.aggregations.metrics.avg;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalAvgTests extends InternalAggregationTestCase<InternalAvg> {

    @Override
    protected InternalAvg createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        DocValueFormat formatter = randomFrom(new DocValueFormat.Decimal("###.##"), DocValueFormat.BOOLEAN, DocValueFormat.RAW);
        long count = frequently() ? randomNonNegativeLong() % 100000 : 0;
        return new InternalAvg(name, randomDoubleBetween(0, 100000, true), count, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected Reader<InternalAvg> instanceReader() {
        return InternalAvg::new;
    }

    @Override
    protected void assertReduced(InternalAvg reduced, List<InternalAvg> inputs) {
        double sum = 0;
        long counts = 0;
        for (InternalAvg in : inputs) {
            sum += in.getSum();
            counts += in.getCount();
        }
        assertEquals(counts, reduced.getCount());
        assertEquals(sum, reduced.getSum(), 0.0000001);
        assertEquals(sum / counts, reduced.value(), 0.0000001);
    }

    @Override
    protected void assertFromXContent(InternalAvg avg, ParsedAggregation parsedAggregation) {
        ParsedAvg parsed = ((ParsedAvg) parsedAggregation);
        assertEquals(avg.getValue(), parsed.getValue(), Double.MIN_VALUE);
        if (avg.getCount() != 0) {
            assertEquals(avg.getValueAsString(), parsed.getValueAsString());
        }
    }
}