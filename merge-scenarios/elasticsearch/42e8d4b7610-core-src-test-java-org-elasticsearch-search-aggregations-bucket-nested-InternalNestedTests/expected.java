package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalNestedTests extends InternalSingleBucketAggregationTestCase<InternalNested> {

    @Override
    protected InternalNested createTestInstance(String name, long docCount, InternalAggregations aggregations, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalNested(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalNested reduced, List<InternalNested> inputs) {
    }

    @Override
    protected Reader<InternalNested> instanceReader() {
        return InternalNested::new;
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedNested.class;
    }
}