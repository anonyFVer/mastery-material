package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalSamplerTests extends InternalSingleBucketAggregationTestCase<InternalSampler> {

    @Override
    protected InternalSampler createTestInstance(String name, long docCount, InternalAggregations aggregations, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalSampler(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalSampler reduced, List<InternalSampler> inputs) {
    }

    @Override
    protected Writeable.Reader<InternalSampler> instanceReader() {
        return InternalSampler::new;
    }
}