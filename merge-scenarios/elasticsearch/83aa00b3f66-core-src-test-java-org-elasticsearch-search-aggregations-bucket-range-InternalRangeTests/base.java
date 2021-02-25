package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.Before;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalRangeTests extends InternalRangeTestCase<InternalRange> {

    private DocValueFormat format;

    private List<Tuple<Double, Double>> ranges;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        format = randomNumericDocValueFormat();
        final int interval = randomFrom(1, 5, 10, 25, 50, 100);
        final int numRanges = 1;
        List<Tuple<Double, Double>> listOfRanges = new ArrayList<>(numRanges);
        for (int i = 0; i < numRanges; i++) {
            double from = i * interval;
            double to = from + interval;
            listOfRanges.add(Tuple.tuple(from, to));
        }
        if (randomBoolean()) {
            double max = (double) numRanges * interval;
            listOfRanges.add(Tuple.tuple(0.0, max));
            listOfRanges.add(Tuple.tuple(0.0, max / 2));
            listOfRanges.add(Tuple.tuple(max / 3, max / 3 * 2));
        }
        ranges = Collections.unmodifiableList(listOfRanges);
    }

    @Override
    protected InternalRange createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, boolean keyed) {
        final List<InternalRange.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < ranges.size(); ++i) {
            Tuple<Double, Double> range = ranges.get(i);
            int docCount = randomIntBetween(0, 1000);
            double from = range.v1();
            double to = range.v2();
            buckets.add(new InternalRange.Bucket("range_" + i, from, to, docCount, InternalAggregations.EMPTY, keyed, format));
        }
        return new InternalRange<>(name, buckets, format, keyed, pipelineAggregators, Collections.emptyMap());
    }

    @Override
    protected Writeable.Reader<InternalRange> instanceReader() {
        return InternalRange::new;
    }
}