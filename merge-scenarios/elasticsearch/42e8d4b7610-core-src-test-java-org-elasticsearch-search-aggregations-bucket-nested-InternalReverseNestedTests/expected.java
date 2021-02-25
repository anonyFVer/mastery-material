package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalReverseNestedTests extends InternalSingleBucketAggregationTestCase<InternalReverseNested> {

    @Override
    protected InternalReverseNested createTestInstance(String name, long docCount, InternalAggregations aggregations, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalReverseNested(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalReverseNested reduced, List<InternalReverseNested> inputs) {
    }

    @Override
    protected Reader<InternalReverseNested> instanceReader() {
        return InternalReverseNested::new;
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedReverseNested.class;
    }
}