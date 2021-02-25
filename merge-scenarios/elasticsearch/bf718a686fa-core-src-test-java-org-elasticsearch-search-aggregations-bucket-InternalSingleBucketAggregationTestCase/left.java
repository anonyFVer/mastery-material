package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class InternalSingleBucketAggregationTestCase<T extends InternalSingleBucketAggregation> extends InternalAggregationTestCase<T> {

    private final boolean hasInternalMax = randomBoolean();

    private final boolean hasInternalMin = randomBoolean();

    public Supplier<InternalAggregations> subAggregationsSupplier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        subAggregationsSupplier = () -> {
            List<InternalAggregation> aggs = new ArrayList<>();
            if (hasInternalMax) {
                aggs.add(new InternalMax("max", randomDouble(), randomNumericDocValueFormat(), emptyList(), emptyMap()));
            }
            if (hasInternalMin) {
                aggs.add(new InternalMin("min", randomDouble(), randomNumericDocValueFormat(), emptyList(), emptyMap()));
            }
            return new InternalAggregations(aggs);
        };
    }

    protected abstract T createTestInstance(String name, long docCount, InternalAggregations aggregations, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    protected abstract void extraAssertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        long docCount = between(0, Integer.MAX_VALUE);
        return createTestInstance(name, docCount, subAggregationsSupplier.get(), pipelineAggregators, metaData);
    }

    @Override
    protected final void assertReduced(T reduced, List<T> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalSingleBucketAggregation::getDocCount).sum(), reduced.getDocCount());
        if (hasInternalMax) {
            double expected = inputs.stream().mapToDouble(i -> {
                InternalMax max = i.getAggregations().get("max");
                return max.getValue();
            }).max().getAsDouble();
            InternalMax reducedMax = reduced.getAggregations().get("max");
            assertEquals(expected, reducedMax.getValue(), 0);
        }
        if (hasInternalMin) {
            double expected = inputs.stream().mapToDouble(i -> {
                InternalMin min = i.getAggregations().get("min");
                return min.getValue();
            }).min().getAsDouble();
            InternalMin reducedMin = reduced.getAggregations().get("min");
            assertEquals(expected, reducedMin.getValue(), 0);
        }
        extraAssertReduced(reduced, inputs);
    }
}