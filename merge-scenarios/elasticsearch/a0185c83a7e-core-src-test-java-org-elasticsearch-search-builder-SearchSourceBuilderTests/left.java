package org.elasticsearch.search.builder;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import java.io.IOException;
import java.util.Map;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class SearchSourceBuilderTests extends AbstractSearchTestCase {

    public void testFromXContent() throws IOException {
        SearchSourceBuilder testSearchSourceBuilder = createSearchSourceBuilder();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        testSearchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertParseSearchSource(testSearchSourceBuilder, createParser(builder));
    }

    private void assertParseSearchSource(SearchSourceBuilder testBuilder, XContentParser parser) throws IOException {
        assertParseSearchSource(testBuilder, parser, ParseFieldMatcher.STRICT);
    }

    private void assertParseSearchSource(SearchSourceBuilder testBuilder, XContentParser parser, ParseFieldMatcher pfm) throws IOException {
        QueryParseContext parseContext = new QueryParseContext(searchRequestParsers.queryParsers, parser, pfm);
        if (randomBoolean()) {
            parser.nextToken();
        }
        SearchSourceBuilder newBuilder = SearchSourceBuilder.fromXContent(parseContext, searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
        assertNull(parser.nextToken());
        assertEquals(testBuilder, newBuilder);
        assertEquals(testBuilder.hashCode(), newBuilder.hashCode());
    }

    private QueryParseContext createParseContext(XContentParser parser) {
        return new QueryParseContext(searchRequestParsers.queryParsers, parser, ParseFieldMatcher.STRICT);
    }

    public void testSerialization() throws IOException {
        SearchSourceBuilder testBuilder = createSearchSourceBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testBuilder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                SearchSourceBuilder deserializedBuilder = new SearchSourceBuilder(in);
                assertEquals(deserializedBuilder, testBuilder);
                assertEquals(deserializedBuilder.hashCode(), testBuilder.hashCode());
                assertNotSame(deserializedBuilder, testBuilder);
            }
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createSearchSourceBuilder(), this::copyBuilder);
    }

    private SearchSourceBuilder copyBuilder(SearchSourceBuilder original) throws IOException {
        return ESTestCase.copyWriteable(original, namedWriteableRegistry, SearchSourceBuilder::new);
    }

    public void testParseIncludeExclude() throws IOException {
        {
            String restContent = " { \"_source\": { \"includes\": \"include\", \"excludes\": \"*.field2\"}}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertArrayEquals(new String[] { "*.field2" }, searchSourceBuilder.fetchSource().excludes());
                assertArrayEquals(new String[] { "include" }, searchSourceBuilder.fetchSource().includes());
            }
        }
        {
            String restContent = " { \"_source\": false}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertArrayEquals(new String[] {}, searchSourceBuilder.fetchSource().excludes());
                assertArrayEquals(new String[] {}, searchSourceBuilder.fetchSource().includes());
                assertFalse(searchSourceBuilder.fetchSource().fetchSource());
            }
        }
    }

    public void testMultipleQueryObjectsAreRejected() throws Exception {
        String restContent = " { \"query\": {\n" + "    \"multi_match\": {\n" + "      \"query\": \"workd\",\n" + "      \"fields\": [\"title^5\", \"plain_body\"]\n" + "    },\n" + "    \"filters\": {\n" + "      \"terms\": {\n" + "        \"status\": [ 3 ]\n" + "      }\n" + "    }\n" + "  } }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            ParsingException e = expectThrows(ParsingException.class, () -> SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers));
            assertEquals("[multi_match] malformed query, expected [END_OBJECT] but found [FIELD_NAME]", e.getMessage());
        }
    }

    public void testParseSort() throws IOException {
        {
            String restContent = " { \"sort\": \"foo\"}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertEquals(1, searchSourceBuilder.sorts().size());
                assertEquals(new FieldSortBuilder("foo"), searchSourceBuilder.sorts().get(0));
            }
        }
        {
            String restContent = "{\"sort\" : [\n" + "        { \"post_date\" : {\"order\" : \"asc\"}},\n" + "        \"user\",\n" + "        { \"name\" : \"desc\" },\n" + "        { \"age\" : \"desc\" },\n" + "        \"_score\"\n" + "    ]}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertEquals(5, searchSourceBuilder.sorts().size());
                assertEquals(new FieldSortBuilder("post_date"), searchSourceBuilder.sorts().get(0));
                assertEquals(new FieldSortBuilder("user"), searchSourceBuilder.sorts().get(1));
                assertEquals(new FieldSortBuilder("name").order(SortOrder.DESC), searchSourceBuilder.sorts().get(2));
                assertEquals(new FieldSortBuilder("age").order(SortOrder.DESC), searchSourceBuilder.sorts().get(3));
                assertEquals(new ScoreSortBuilder(), searchSourceBuilder.sorts().get(4));
            }
        }
    }

    public void testAggsParsing() throws IOException {
        {
            String restContent = "{\n" + "    " + "\"aggs\": {" + "        \"test_agg\": {\n" + "            " + "\"terms\" : {\n" + "                \"field\": \"foo\"\n" + "            }\n" + "        }\n" + "    }\n" + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertEquals(1, searchSourceBuilder.aggregations().count());
            }
        }
        {
            String restContent = "{\n" + "    \"aggregations\": {" + "        \"test_agg\": {\n" + "            \"terms\" : {\n" + "                \"field\": \"foo\"\n" + "            }\n" + "        }\n" + "    }\n" + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertEquals(1, searchSourceBuilder.aggregations().count());
            }
        }
    }

    public void testParseRescore() throws IOException {
        {
            String restContent = "{\n" + "    \"query\" : {\n" + "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n" + "     },\n" + "    \"rescore\": {" + "        \"window_size\": 50,\n" + "        \"query\": {\n" + "            \"rescore_query\" : {\n" + "                \"match\": { \"content\": { \"query\": \"baz\" } }\n" + "            }\n" + "        }\n" + "    }\n" + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertEquals(1, searchSourceBuilder.rescores().size());
                assertEquals(new QueryRescorerBuilder(QueryBuilders.matchQuery("content", "baz")).windowSize(50), searchSourceBuilder.rescores().get(0));
            }
        }
        {
            String restContent = "{\n" + "    \"query\" : {\n" + "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n" + "     },\n" + "    \"rescore\": [ {" + "        \"window_size\": 50,\n" + "        \"query\": {\n" + "            \"rescore_query\" : {\n" + "                \"match\": { \"content\": { \"query\": \"baz\" } }\n" + "            }\n" + "        }\n" + "    } ]\n" + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                assertEquals(1, searchSourceBuilder.rescores().size());
                assertEquals(new QueryRescorerBuilder(QueryBuilders.matchQuery("content", "baz")).windowSize(50), searchSourceBuilder.rescores().get(0));
            }
        }
    }

    public void testTimeoutWithUnits() throws IOException {
        final String timeout = randomTimeValue();
        final String query = "{ \"query\": { \"match_all\": {}}, \"timeout\": \"" + timeout + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, query)) {
            final SearchSourceBuilder builder = SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
            assertThat(builder.timeout(), equalTo(TimeValue.parseTimeValue(timeout, null, "timeout")));
        }
    }

    public void testTimeoutWithoutUnits() throws IOException {
        final int timeout = randomIntBetween(1, 1024);
        final String query = "{ \"query\": { \"match_all\": {}}, \"timeout\": \"" + timeout + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, query)) {
            final ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> SearchSourceBuilder.fromXContent(createParseContext(parser), searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers));
            assertThat(e, hasToString(containsString("unit is missing or unrecognized")));
        }
    }

    public void testToXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
            searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference source = builder.bytes();
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false).v2();
            assertEquals(0, sourceAsMap.size());
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(RandomQueryBuilder.createQuery(random()));
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
            searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference source = builder.bytes();
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false).v2();
            assertEquals(1, sourceAsMap.size());
            assertEquals("query", sourceAsMap.keySet().iterator().next());
        }
    }
}