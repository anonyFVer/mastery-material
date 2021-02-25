package org.elasticsearch.search.aggregations.metrics.geocentroid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.geo.RandomGeoGenerator;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalGeoCentroidTests extends InternalAggregationTestCase<InternalGeoCentroid> {

    @Override
    protected InternalGeoCentroid createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        GeoPoint centroid = RandomGeoGenerator.randomPoint(random());
        int encodedLon = GeoEncodingUtils.encodeLongitude(centroid.lon());
        centroid.resetLon(GeoEncodingUtils.decodeLongitude(encodedLon));
        int encodedLat = GeoEncodingUtils.encodeLatitude(centroid.lat());
        centroid.resetLat(GeoEncodingUtils.decodeLatitude(encodedLat));
        long count = randomIntBetween(0, 1000);
        if (count == 0) {
            centroid = null;
        }
        return new InternalGeoCentroid("_name", centroid, count, Collections.emptyList(), Collections.emptyMap());
    }

    @Override
    protected Writeable.Reader<InternalGeoCentroid> instanceReader() {
        return InternalGeoCentroid::new;
    }

    @Override
    protected void assertReduced(InternalGeoCentroid reduced, List<InternalGeoCentroid> inputs) {
        double lonSum = 0;
        double latSum = 0;
        int totalCount = 0;
        for (InternalGeoCentroid input : inputs) {
            if (input.count() > 0) {
                lonSum += (input.count() * input.centroid().getLon());
                latSum += (input.count() * input.centroid().getLat());
            }
            totalCount += input.count();
        }
        assertEquals(latSum / totalCount, reduced.centroid().getLat(), 1E-5D);
        assertEquals(lonSum / totalCount, reduced.centroid().getLon(), 1E-5D);
        assertEquals(totalCount, reduced.count());
    }
}