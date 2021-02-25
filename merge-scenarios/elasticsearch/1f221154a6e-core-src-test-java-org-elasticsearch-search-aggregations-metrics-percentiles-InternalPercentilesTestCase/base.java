package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.Before;
import java.util.List;
import java.util.Map;

public abstract class InternalPercentilesTestCase<T extends InternalAggregation> extends InternalAggregationTestCase<T> {

    private double[] percents;

    @Before
    public void init() {
        percents = randomPercents();
    }

    @Override
    protected T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        int numValues = randomInt(100);
        double[] values = new double[numValues];
        for (int i = 0; i < numValues; ++i) {
            values[i] = randomDouble();
        }
        return createTestInstance(name, pipelineAggregators, metaData, randomBoolean(), DocValueFormat.RAW, percents, values);
    }

    protected abstract T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, boolean keyed, DocValueFormat format, double[] percents, double[] values);

    protected static double[] randomPercents() {
        List<Double> randomCdfValues = randomSubsetOf(randomIntBetween(1, 7), 0.01d, 0.05d, 0.25d, 0.50d, 0.75d, 0.95d, 0.99d);
        double[] percents = new double[randomCdfValues.size()];
        for (int i = 0; i < randomCdfValues.size(); i++) {
            percents[i] = randomCdfValues.get(i);
        }
        return percents;
    }
}