package org.elasticsearch.search.aggregations.bucket.global;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import java.util.List;
import java.util.Map;

public class InternalGlobalTests extends InternalSingleBucketAggregationTestCase<InternalGlobal> {

    @Override
    protected InternalGlobal createTestInstance(String name, long docCount, InternalAggregations aggregations, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalGlobal(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalGlobal reduced, List<InternalGlobal> inputs) {
    }

    @Override
    protected Reader<InternalGlobal> instanceReader() {
        return InternalGlobal::new;
    }
}