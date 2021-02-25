package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;

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
}