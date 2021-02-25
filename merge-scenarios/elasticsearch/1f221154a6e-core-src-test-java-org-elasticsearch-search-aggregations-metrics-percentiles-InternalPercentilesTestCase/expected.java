package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import java.util.List;

public abstract class InternalPercentilesTestCase<T extends InternalAggregation & Percentiles> extends AbstractPercentilesTestCase<T> {

    @Override
    protected final void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof Percentiles);
        Percentiles parsedPercentiles = (Percentiles) parsedAggregation;
        for (Percentile percentile : aggregation) {
            Double percent = percentile.getPercent();
            assertEquals(aggregation.percentile(percent), parsedPercentiles.percentile(percent), 0);
            assertEquals(aggregation.percentileAsString(percent), parsedPercentiles.percentileAsString(percent));
        }
        Class<? extends ParsedPercentiles> parsedClass = implementationClass();
        assertTrue(parsedClass != null && parsedClass.isInstance(parsedAggregation));
    }

    public static double[] randomPercents() {
        List<Double> randomCdfValues = randomSubsetOf(randomIntBetween(1, 7), 0.01d, 0.05d, 0.25d, 0.50d, 0.75d, 0.95d, 0.99d);
        double[] percents = new double[randomCdfValues.size()];
        for (int i = 0; i < randomCdfValues.size(); i++) {
            percents[i] = randomCdfValues.get(i);
        }
        return percents;
    }
}