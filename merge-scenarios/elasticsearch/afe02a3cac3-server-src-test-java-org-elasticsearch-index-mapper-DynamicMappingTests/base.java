package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.BooleanFieldMapper.BooleanFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DynamicMappingTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testDynamicTrue() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").field("dynamic", "true").startObject("properties").startObject("field1").field("type", "text").endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(jsonBuilder().startObject().field("field1", "value1").field("field2", "value2").endObject()), XContentType.JSON));
        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));
    }

    public void testDynamicFalse() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").field("dynamic", "false").startObject("properties").startObject("field1").field("type", "text").endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(jsonBuilder().startObject().field("field1", "value1").field("field2", "value2").endObject()), XContentType.JSON));
        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), nullValue());
    }

    public void testDynamicStrict() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").field("dynamic", "strict").startObject("properties").startObject("field1").field("type", "text").endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(jsonBuilder().startObject().field("field1", "value1").field("field2", "value2").endObject()), XContentType.JSON)));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [type] is not allowed"));
        e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field1", "value1").field("field2", (String) null).endObject()), XContentType.JSON)));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [type] is not allowed"));
    }

    public void testDynamicFalseWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").field("dynamic", "false").startObject("properties").startObject("obj1").startObject("properties").startObject("field1").field("type", "text").endObject().endObject().endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(jsonBuilder().startObject().startObject("obj1").field("field1", "value1").field("field2", "value2").endObject().endObject()), XContentType.JSON));
        assertThat(doc.rootDoc().get("obj1.field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("obj1.field2"), nullValue());
    }

    public void testDynamicStrictWithInnerObjectButDynamicSetOnRoot() throws IOException {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").field("dynamic", "strict").startObject("properties").startObject("obj1").startObject("properties").startObject("field1").field("type", "text").endObject().endObject().endObject().endObject().endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(jsonBuilder().startObject().startObject("obj1").field("field1", "value1").field("field2", "value2").endObject().endObject()), XContentType.JSON)));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [obj1] is not allowed"));
    }

    public void testDynamicMappingOnEmptyString() throws Exception {
        IndexService service = createIndex("test");
        client().prepareIndex("test", "type").setSource("empty_field", "").get();
        MappedFieldType fieldType = service.mapperService().fullName("empty_field");
        assertNotNull(fieldType);
    }

    public void testTypeNotCreatedOnIndexFailure() throws IOException, InterruptedException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_default_").field("dynamic", "strict").endObject().endObject();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_3_0).build();
        createIndex("test", settings, "_default_", mapping);
        try {
            client().prepareIndex().setIndex("test").setType("type").setSource(jsonBuilder().startObject().field("test", "test").endObject()).get();
            fail();
        } catch (StrictDynamicMappingException e) {
        }
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertNull(getMappingsResponse.getMappings().get("test").get("type"));
    }

    private String serialize(ToXContent mapper) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, new ToXContent.MapParams(emptyMap()));
        return Strings.toString(builder.endObject());
    }

    private Mapper parse(DocumentMapper mapper, DocumentMapperParser parser, XContentBuilder builder) throws Exception {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        SourceToParse source = SourceToParse.source("test", mapper.type(), "some_id", BytesReference.bytes(builder), builder.contentType());
        try (XContentParser xContentParser = createParser(JsonXContent.jsonXContent, source.source())) {
            ParseContext.InternalParseContext ctx = new ParseContext.InternalParseContext(settings, parser, mapper, source, xContentParser);
            assertEquals(XContentParser.Token.START_OBJECT, ctx.parser().nextToken());
            ctx.parser().nextToken();
            DocumentParser.parseObjectOrNested(ctx, mapper.root());
            Mapping mapping = DocumentParser.createDynamicUpdate(mapper.mapping(), mapper, ctx.getDynamicMappers());
            return mapping == null ? null : mapping.root();
        }
    }

    public void testDynamicMappingsNotNeeded() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").field("type", "text").endObject().endObject().endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        assertNull(update);
    }

    public void testField() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject());
        assertNotNull(update);
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().endObject().endObject().endObject()), serialize(update));
    }

    public void testIncremental() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").field("type", "text").endObject().endObject().endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").field("bar", "baz").endObject());
        assertNotNull(update);
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("bar").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().endObject().endObject().endObject()), serialize(update));
    }

    public void testIntroduceTwoFields() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().field("foo", "bar").field("bar", "baz").endObject());
        assertNotNull(update);
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("bar").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().startObject("foo").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().endObject().endObject().endObject()), serialize(update));
    }

    public void testObject() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo").startObject("bar").field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").startObject("properties").startObject("bar").startObject("properties").startObject("baz").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject()), serialize(update));
    }

    public void testArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startArray("foo").value("bar").value("baz").endArray().endObject());
        assertNotNull(update);
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().endObject().endObject().endObject()), serialize(update));
    }

    public void testInnerDynamicMapping() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").field("type", "object").endObject().endObject().endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startObject("foo").startObject("bar").field("baz", "foo").endObject().endObject().endObject());
        assertNotNull(update);
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").startObject("properties").startObject("bar").startObject("properties").startObject("baz").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject()), serialize(update));
    }

    public void testComplexArray() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, serialize(mapper));
        Mapper update = parse(mapper, parser, XContentFactory.jsonBuilder().startObject().startArray("foo").startObject().field("bar", "baz").endObject().startObject().field("baz", 3).endObject().endArray().endObject());
        assertEquals(mapping, serialize(mapper));
        assertEquals(Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("foo").startObject("properties").startObject("bar").field("type", "text").startObject("fields").startObject("keyword").field("type", "keyword").field("ignore_above", 256).endObject().endObject().endObject().startObject("baz").field("type", "long").endObject().endObject().endObject().endObject().endObject().endObject()), serialize(update));
    }

    public void testReuseExistingMappings() throws IOException, Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY, "type", "my_field1", "type=text,store=true", "my_field2", "type=integer,store=false", "my_field3", "type=long,doc_values=false", "my_field4", "type=float,index=false", "my_field5", "type=double,store=true", "my_field6", "type=date,doc_values=false", "my_field7", "type=boolean,doc_values=false");
        DocumentMapper newMapper = indexService.mapperService().documentMapperWithAutoCreate("type2").getDocumentMapper();
        Mapper update = parse(newMapper, indexService.mapperService().documentMapperParser(), XContentFactory.jsonBuilder().startObject().field("my_field1", 42).field("my_field2", 43).field("my_field3", 44).field("my_field4", 45).field("my_field5", 46).field("my_field6", 47).field("my_field7", true).endObject());
        Mapper myField1Mapper = null;
        Mapper myField2Mapper = null;
        Mapper myField3Mapper = null;
        Mapper myField4Mapper = null;
        Mapper myField5Mapper = null;
        Mapper myField6Mapper = null;
        Mapper myField7Mapper = null;
        for (Mapper m : update) {
            switch(m.name()) {
                case "my_field1":
                    myField1Mapper = m;
                    break;
                case "my_field2":
                    myField2Mapper = m;
                    break;
                case "my_field3":
                    myField3Mapper = m;
                    break;
                case "my_field4":
                    myField4Mapper = m;
                    break;
                case "my_field5":
                    myField5Mapper = m;
                    break;
                case "my_field6":
                    myField6Mapper = m;
                    break;
                case "my_field7":
                    myField7Mapper = m;
                    break;
            }
        }
        assertNotNull(myField1Mapper);
        assertTrue(myField1Mapper instanceof TextFieldMapper);
        assertTrue(((TextFieldMapper) myField1Mapper).fieldType().stored());
        assertNotNull(myField2Mapper);
        assertEquals("integer", ((FieldMapper) myField2Mapper).fieldType().typeName());
        assertFalse(((FieldMapper) myField2Mapper).fieldType().stored());
        assertNotNull(myField3Mapper);
        assertTrue(myField3Mapper instanceof NumberFieldMapper);
        assertFalse(((NumberFieldType) ((NumberFieldMapper) myField3Mapper).fieldType()).hasDocValues());
        assertNotNull(myField4Mapper);
        assertTrue(myField4Mapper instanceof NumberFieldMapper);
        assertEquals(IndexOptions.NONE, ((FieldMapper) myField4Mapper).fieldType().indexOptions());
        assertNotNull(myField5Mapper);
        assertTrue(myField5Mapper instanceof NumberFieldMapper);
        assertTrue(((NumberFieldMapper) myField5Mapper).fieldType().stored());
        assertNotNull(myField6Mapper);
        assertTrue(myField6Mapper instanceof DateFieldMapper);
        assertFalse(((DateFieldType) ((DateFieldMapper) myField6Mapper).fieldType()).hasDocValues());
        assertNotNull(myField7Mapper);
        assertTrue(myField7Mapper instanceof BooleanFieldMapper);
        assertFalse(((BooleanFieldType) ((BooleanFieldMapper) myField7Mapper).fieldType()).hasDocValues());
        try {
            parse(newMapper, indexService.mapperService().documentMapperParser(), XContentFactory.jsonBuilder().startObject().field("my_field2", "foobar").endObject());
            fail("Cannot succeed, incompatible types");
        } catch (MapperParsingException e) {
        }
    }

    public void testMixTemplateMultiFieldAndMappingReuse() throws Exception {
        IndexService indexService = createIndex("test");
        XContentBuilder mappings1 = jsonBuilder().startObject().startObject("_doc").startArray("dynamic_templates").startObject().startObject("template1").field("match_mapping_type", "string").startObject("mapping").field("type", "text").startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject().endObject().endObject().endObject().endArray().endObject().endObject();
        indexService.mapperService().merge("_doc", new CompressedXContent(BytesReference.bytes(mappings1)), MapperService.MergeReason.MAPPING_UPDATE);
        XContentBuilder json = XContentFactory.jsonBuilder().startObject().field("field", "foo").endObject();
        SourceToParse source = SourceToParse.source("test", "_doc", "1", BytesReference.bytes(json), json.contentType());
        DocumentMapper mapper = indexService.mapperService().documentMapper("_doc");
        assertNull(mapper.mappers().getMapper("field.raw"));
        ParsedDocument parsed = mapper.parse(source);
        assertNotNull(parsed.dynamicMappingsUpdate());
        indexService.mapperService().merge("_doc", new CompressedXContent(parsed.dynamicMappingsUpdate().toString()), MapperService.MergeReason.MAPPING_UPDATE);
        mapper = indexService.mapperService().documentMapper("_doc");
        assertNotNull(mapper.mappers().getMapper("field.raw"));
        parsed = mapper.parse(source);
        assertNull(parsed.dynamicMappingsUpdate());
    }

    public void testDefaultFloatingPointMappings() throws IOException {
        MapperService mapperService = createIndex("test").mapperService();
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").field("numeric_detection", true).endObject().endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        DocumentMapper mapper = mapperService.documentMapper("type");
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.jsonBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.yamlBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.smileBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.cborBuilder());
    }

    private void doTestDefaultFloatingPointMappings(DocumentMapper mapper, XContentBuilder builder) throws IOException {
        BytesReference source = BytesReference.bytes(builder.startObject().field("foo", 3.2f).field("bar", 3.2d).field("baz", (double) 3.2f).field("quux", "3.2").endObject());
        ParsedDocument parsedDocument = mapper.parse(SourceToParse.source("index", "type", "id", source, builder.contentType()));
        Mapping update = parsedDocument.dynamicMappingsUpdate();
        assertNotNull(update);
        assertThat(((FieldMapper) update.root().getMapper("foo")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("bar")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("baz")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.root().getMapper("quux")).fieldType().typeName(), equalTo("float"));
    }

    public void testNumericDetectionEnabled() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").field("numeric_detection", true).endObject().endObject());
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping, XContentType.JSON).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper("type");
        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("s_long", "100").field("s_double", "100.0").endObject()), XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test").setType("type").setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();
        defaultMapper = index.mapperService().documentMapper("type");
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper.fieldType().typeName(), equalTo("long"));
        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper.fieldType().typeName(), equalTo("float"));
    }

    public void testNumericDetectionDefault() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping, XContentType.JSON).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper("type");
        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("s_long", "100").field("s_double", "100.0").endObject()), XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertAcked(client().admin().indices().preparePutMapping("test").setType("type").setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get());
        defaultMapper = index.mapperService().documentMapper("type");
        FieldMapper mapper = defaultMapper.mappers().smartNameFieldMapper("s_long");
        assertThat(mapper, instanceOf(TextFieldMapper.class));
        mapper = defaultMapper.mappers().smartNameFieldMapper("s_double");
        assertThat(mapper, instanceOf(TextFieldMapper.class));
    }

    public void testDateDetectionInheritsFormat() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startArray("dynamic_date_formats").value("yyyy-MM-dd").endArray().startArray("dynamic_templates").startObject().startObject("dates").field("match_mapping_type", "date").field("match", "*2").startObject("mapping").endObject().endObject().endObject().startObject().startObject("dates").field("match_mapping_type", "date").field("match", "*3").startObject("mapping").field("format", "yyyy-MM-dd||epoch_millis").endObject().endObject().endObject().endArray().endObject().endObject());
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("type").setSource(mapping, XContentType.JSON).get();
        DocumentMapper defaultMapper = index.mapperService().documentMapper("type");
        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("date1", "2016-11-20").field("date2", "2016-11-20").field("date3", "2016-11-20").endObject()), XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertAcked(client().admin().indices().preparePutMapping("test").setType("type").setSource(doc.dynamicMappingsUpdate().toString(), XContentType.JSON).get());
        defaultMapper = index.mapperService().documentMapper("type");
        DateFieldMapper dateMapper1 = (DateFieldMapper) defaultMapper.mappers().smartNameFieldMapper("date1");
        DateFieldMapper dateMapper2 = (DateFieldMapper) defaultMapper.mappers().smartNameFieldMapper("date2");
        DateFieldMapper dateMapper3 = (DateFieldMapper) defaultMapper.mappers().smartNameFieldMapper("date3");
        assertEquals("yyyy-MM-dd", dateMapper1.fieldType().dateTimeFormatter().format());
        assertEquals("yyyy-MM-dd", dateMapper2.fieldType().dateTimeFormatter().format());
        assertEquals("yyyy-MM-dd||epoch_millis", dateMapper3.fieldType().dateTimeFormatter().format());
    }

    public void testDynamicTemplateOrder() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startArray("dynamic_templates").startObject().startObject("type-based").field("match_mapping_type", "string").startObject("mapping").field("type", "keyword").endObject().endObject().endObject().startObject().startObject("path-based").field("path_match", "foo").startObject("mapping").field("type", "long").endObject().endObject().endObject().endArray().endObject().endObject();
        IndexService index = createIndex("test", Settings.EMPTY, "type", mapping);
        client().prepareIndex("test", "type", "1").setSource("foo", "abc").get();
        assertThat(index.mapperService().fullName("foo"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
    }
}