package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalFilterTests extends InternalSingleBucketAggregationTestCase<InternalFilter> {

    @Override
    protected InternalFilter createTestInstance(String name, long docCount, InternalAggregations aggregations, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalFilter(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalFilter reduced, List<InternalFilter> inputs) {
    }

    @Override
    protected Reader<InternalFilter> instanceReader() {
        return InternalFilter::new;
    }
}