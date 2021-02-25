package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalMissingTests extends InternalSingleBucketAggregationTestCase<InternalMissing> {

    @Override
    protected InternalMissing createTestInstance(String name, long docCount, InternalAggregations aggregations, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalMissing(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalMissing reduced, List<InternalMissing> inputs) {
    }

    @Override
    protected Reader<InternalMissing> instanceReader() {
        return InternalMissing::new;
    }
}