package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public abstract class InternalAggregationTestCase<T extends InternalAggregation> extends AbstractWireSerializingTestCase<T> {

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, false, emptyList()).getNamedWriteables());

    private final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(getNamedXContents());

    static List<NamedXContentRegistry.Entry> getNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> namedXContents = new HashMap<>();
        namedXContents.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        namedXContents.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        namedXContents.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        namedXContents.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        namedXContents.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        namedXContents.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        namedXContents.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        namedXContents.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        return namedXContents.entrySet().stream().map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue())).collect(Collectors.toList());
    }

    protected abstract T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    protected T createUnmappedInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    public void testReduceRandom() {
        String name = randomAlphaOfLength(5);
        List<T> inputs = new ArrayList<>();
        List<InternalAggregation> toReduce = new ArrayList<>();
        int toReduceSize = between(1, 200);
        for (int i = 0; i < toReduceSize; i++) {
            T t = randomBoolean() ? createUnmappedInstance(name) : createTestInstance(name);
            inputs.add(t);
            toReduce.add(t);
        }
        ScriptService mockScriptService = mockScriptService();
        MockBigArrays bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
        if (randomBoolean() && toReduce.size() > 1) {
            Collections.shuffle(toReduce, random());
            int r = randomIntBetween(1, toReduceSize);
            List<InternalAggregation> internalAggregations = toReduce.subList(0, r);
            InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(bigArrays, mockScriptService, false);
            @SuppressWarnings("unchecked")
            T reduced = (T) inputs.get(0).reduce(internalAggregations, context);
            toReduce = new ArrayList<>(toReduce.subList(r, toReduceSize));
            toReduce.add(reduced);
        }
        InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(bigArrays, mockScriptService, true);
        @SuppressWarnings("unchecked")
        T reduced = (T) inputs.get(0).reduce(toReduce, context);
        assertReduced(reduced, inputs);
    }

    protected ScriptService mockScriptService() {
        return null;
    }

    protected abstract void assertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance() {
        return createTestInstance(randomAlphaOfLength(5));
    }

    private T createTestInstance(String name) {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        Map<String, Object> metaData = null;
        if (randomBoolean()) {
            metaData = new HashMap<>();
            int metaDataCount = between(0, 10);
            while (metaData.size() < metaDataCount) {
                metaData.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
        }
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    protected final T createUnmappedInstance(String name) {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        Map<String, Object> metaData = new HashMap<>();
        int metaDataCount = randomBoolean() ? 0 : between(1, 10);
        while (metaData.size() < metaDataCount) {
            metaData.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return createUnmappedInstance(name, pipelineAggregators, metaData);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    public final void testFromXContent() throws IOException {
        final NamedXContentRegistry xContentRegistry = xContentRegistry();
        final T aggregation = createTestInstance();
        assumeTrue("This test does not support the aggregation type yet", getNamedXContents().stream().filter(entry -> entry.name.match(aggregation.getType())).count() > 0);
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        final boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference originalBytes = toShuffledXContent(aggregation, xContentType, params, humanReadable);
        Aggregation parsedAggregation;
        try (XContentParser parser = xContentType.xContent().createParser(xContentRegistry, originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            String currentName = parser.currentName();
            int i = currentName.indexOf(InternalAggregation.TYPED_KEYS_DELIMITER);
            String aggType = currentName.substring(0, i);
            String aggName = currentName.substring(i + 1);
            parsedAggregation = parser.namedObject(Aggregation.class, aggType, aggName);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
            assertEquals(aggregation.getName(), parsedAggregation.getName());
            assertEquals(aggregation.getMetaData(), parsedAggregation.getMetaData());
            assertTrue(parsedAggregation instanceof ParsedAggregation);
            assertEquals(aggregation.getType(), ((ParsedAggregation) parsedAggregation).getType());
            final BytesReference parsedBytes = toXContent((ToXContent) parsedAggregation, xContentType, params, humanReadable);
            assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
            assertFromXContent(aggregation, (ParsedAggregation) parsedAggregation);
        }
    }

    protected void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
    }
}