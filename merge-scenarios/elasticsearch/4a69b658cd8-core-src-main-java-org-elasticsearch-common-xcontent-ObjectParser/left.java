package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_BOOLEAN;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_EMBEDDED_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NULL;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NUMBER;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_STRING;

public final class ObjectParser<Value, Context> extends AbstractObjectParser<Value, Context> {

    public static <Value, ElementValue> BiConsumer<Value, List<ElementValue>> fromList(Class<ElementValue> c, BiConsumer<Value, ElementValue[]> consumer) {
        return (Value v, List<ElementValue> l) -> {
            @SuppressWarnings("unchecked")
            ElementValue[] array = (ElementValue[]) Array.newInstance(c, l.size());
            consumer.accept(v, l.toArray(array));
        };
    }

    private final Map<String, FieldParser> fieldParserMap = new HashMap<>();

    private final String name;

    private final Supplier<Value> valueSupplier;

    private final boolean ignoreUnknownFields;

    public ObjectParser(String name) {
        this(name, null);
    }

    public ObjectParser(String name, @Nullable Supplier<Value> valueSupplier) {
        this(name, false, valueSupplier);
    }

    public ObjectParser(String name, boolean ignoreUnknownFields, @Nullable Supplier<Value> valueSupplier) {
        this.name = name;
        this.valueSupplier = valueSupplier;
        this.ignoreUnknownFields = ignoreUnknownFields;
    }

    @Override
    public Value parse(XContentParser parser, Context context) throws IOException {
        if (valueSupplier == null) {
            throw new NullPointerException("valueSupplier is not set");
        }
        return parse(parser, valueSupplier.get(), context);
    }

    public Value parse(XContentParser parser, Value value, Context context) throws IOException {
        XContentParser.Token token;
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            token = parser.currentToken();
        } else {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException("[" + name + "] Expected START_OBJECT but was: " + token);
            }
        }
        FieldParser fieldParser = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                fieldParser = getParser(currentFieldName);
            } else {
                if (currentFieldName == null) {
                    throw new IllegalStateException("[" + name + "] no field found");
                }
                if (fieldParser == null) {
                    assert ignoreUnknownFields : "this should only be possible if configured to ignore known fields";
                    parser.skipChildren();
                } else {
                    fieldParser.assertSupports(name, token, currentFieldName);
                    parseSub(parser, fieldParser, currentFieldName, value, context);
                }
                fieldParser = null;
            }
        }
        return value;
    }

    @Override
    public Value apply(XContentParser parser, Context context) {
        if (valueSupplier == null) {
            throw new NullPointerException("valueSupplier is not set");
        }
        try {
            return parse(parser, valueSupplier.get(), context);
        } catch (IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "[" + name + "] failed to parse object", e);
        }
    }

    public interface Parser<Value, Context> {

        void parse(XContentParser parser, Value value, Context context) throws IOException;
    }

    public void declareField(Parser<Value, Context> p, ParseField parseField, ValueType type) {
        if (parseField == null) {
            throw new IllegalArgumentException("[parseField] is required");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] is required");
        }
        FieldParser fieldParser = new FieldParser(p, type.supportedTokens(), parseField, type);
        for (String fieldValue : parseField.getAllNamesIncludedDeprecated()) {
            fieldParserMap.putIfAbsent(fieldValue, fieldParser);
        }
    }

    @Override
    public <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, ParseField parseField, ValueType type) {
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        declareField((p, v, c) -> consumer.accept(v, parser.parse(p, c)), parseField, type);
    }

    public <T> void declareObjectOrDefault(BiConsumer<Value, T> consumer, BiFunction<XContentParser, Context, T> objectParser, Supplier<T> defaultValue, ParseField field) {
        declareField((p, v, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                if (p.booleanValue()) {
                    consumer.accept(v, defaultValue.get());
                }
            } else {
                consumer.accept(v, objectParser.apply(p, c));
            }
        }, field, ValueType.OBJECT_OR_BOOLEAN);
    }

    public <T> void declareNamedObjects(BiConsumer<Value, List<T>> consumer, NamedObjectParser<T, Context> namedObjectParser, Consumer<Value> orderedModeCallback, ParseField field) {
        BiFunction<XContentParser, Context, T> objectParser = (XContentParser p, Context c) -> {
            if (p.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(p.getTokenLocation(), "[" + field + "] can be a single object with any number of " + "fields or an array where each entry is an object with a single field");
            }
            try {
                String name = p.currentName();
                try {
                    return namedObjectParser.parse(p, c, name);
                } catch (Exception e) {
                    throw new ParsingException(p.getTokenLocation(), "[" + field + "] failed to parse field [" + name + "]", e);
                }
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "[" + field + "] error while parsing", e);
            }
        };
        declareField((XContentParser p, Value v, Context c) -> {
            List<T> fields = new ArrayList<>();
            XContentParser.Token token;
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    fields.add(objectParser.apply(p, c));
                }
            } else if (p.currentToken() == XContentParser.Token.START_ARRAY) {
                orderedModeCallback.accept(v);
                while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new ParsingException(p.getTokenLocation(), "[" + field + "] can be a single object with any number of " + "fields or an array where each entry is an object with a single field");
                    }
                    p.nextToken();
                    fields.add(objectParser.apply(p, c));
                    p.nextToken();
                    if (p.currentToken() != XContentParser.Token.END_OBJECT) {
                        throw new ParsingException(p.getTokenLocation(), "[" + field + "] can be a single object with any number of " + "fields or an array where each entry is an object with a single field");
                    }
                }
            }
            consumer.accept(v, fields);
        }, field, ValueType.OBJECT_ARRAY);
    }

    public <T> void declareNamedObjects(BiConsumer<Value, List<T>> consumer, NamedObjectParser<T, Context> namedObjectParser, ParseField field) {
        Consumer<Value> orderedModeCallback = (v) -> {
            throw new IllegalArgumentException("[" + field + "] doesn't support arrays. Use a single object with multiple fields.");
        };
        declareNamedObjects(consumer, namedObjectParser, orderedModeCallback, field);
    }

    @FunctionalInterface
    public interface NamedObjectParser<T, Context> {

        T parse(XContentParser p, Context c, String name) throws IOException;
    }

    public String getName() {
        return name;
    }

    private void parseArray(XContentParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_ARRAY : "Token was: " + parser.currentToken();
        parseValue(parser, fieldParser, currentFieldName, value, context);
    }

    private void parseValue(XContentParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) throws IOException {
        try {
            fieldParser.parser.parse(parser, value, context);
        } catch (Exception ex) {
            throw new ParsingException(parser.getTokenLocation(), "[" + name + "] failed to parse field [" + currentFieldName + "]", ex);
        }
    }

    private void parseSub(XContentParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) throws IOException {
        final XContentParser.Token token = parser.currentToken();
        switch(token) {
            case START_OBJECT:
                parseValue(parser, fieldParser, currentFieldName, value, context);
                break;
            case START_ARRAY:
                parseArray(parser, fieldParser, currentFieldName, value, context);
                break;
            case END_OBJECT:
            case END_ARRAY:
            case FIELD_NAME:
                throw new IllegalStateException("[" + name + "]" + token + " is unexpected");
            case VALUE_STRING:
            case VALUE_NUMBER:
            case VALUE_BOOLEAN:
            case VALUE_EMBEDDED_OBJECT:
            case VALUE_NULL:
                parseValue(parser, fieldParser, currentFieldName, value, context);
        }
    }

    private FieldParser getParser(String fieldName) {
        FieldParser parser = fieldParserMap.get(fieldName);
        if (parser == null && false == ignoreUnknownFields) {
            throw new IllegalArgumentException("[" + name + "] unknown field [" + fieldName + "], parser not found");
        }
        return parser;
    }

    private class FieldParser {

        private final Parser<Value, Context> parser;

        private final EnumSet<XContentParser.Token> supportedTokens;

        private final ParseField parseField;

        private final ValueType type;

        FieldParser(Parser<Value, Context> parser, EnumSet<XContentParser.Token> supportedTokens, ParseField parseField, ValueType type) {
            this.parser = parser;
            this.supportedTokens = supportedTokens;
            this.parseField = parseField;
            this.type = type;
        }

        void assertSupports(String parserName, XContentParser.Token token, String currentFieldName) {
            if (parseField.match(currentFieldName) == false) {
                throw new IllegalStateException("[" + parserName + "] parsefield doesn't accept: " + currentFieldName);
            }
            if (supportedTokens.contains(token) == false) {
                throw new IllegalArgumentException("[" + parserName + "] " + currentFieldName + " doesn't support values of type: " + token);
            }
        }

        @Override
        public String toString() {
            String[] deprecatedNames = parseField.getDeprecatedNames();
            String allReplacedWith = parseField.getAllReplacedWith();
            String deprecated = "";
            if (deprecatedNames != null && deprecatedNames.length > 0) {
                deprecated = ", deprecated_names=" + Arrays.toString(deprecatedNames);
            }
            return "FieldParser{" + "preferred_name=" + parseField.getPreferredName() + ", supportedTokens=" + supportedTokens + deprecated + (allReplacedWith == null ? "" : ", replaced_with=" + allReplacedWith) + ", type=" + type.name() + '}';
        }
    }

    public enum ValueType {

        STRING(VALUE_STRING),
        STRING_OR_NULL(VALUE_STRING, VALUE_NULL),
        FLOAT(VALUE_NUMBER, VALUE_STRING),
        FLOAT_OR_NULL(VALUE_NUMBER, VALUE_STRING, VALUE_NULL),
        DOUBLE(VALUE_NUMBER, VALUE_STRING),
        DOUBLE_OR_NULL(VALUE_NUMBER, VALUE_STRING, VALUE_NULL),
        LONG(VALUE_NUMBER, VALUE_STRING),
        LONG_OR_NULL(VALUE_NUMBER, VALUE_STRING, VALUE_NULL),
        INT(VALUE_NUMBER, VALUE_STRING),
        BOOLEAN(VALUE_BOOLEAN),
        STRING_ARRAY(START_ARRAY, VALUE_STRING),
        FLOAT_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        DOUBLE_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        LONG_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        INT_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        BOOLEAN_ARRAY(START_ARRAY, VALUE_BOOLEAN),
        OBJECT(START_OBJECT),
        OBJECT_ARRAY(START_OBJECT, START_ARRAY),
        OBJECT_OR_BOOLEAN(START_OBJECT, VALUE_BOOLEAN),
        OBJECT_OR_STRING(START_OBJECT, VALUE_STRING),
        OBJECT_ARRAY_OR_STRING(START_OBJECT, START_ARRAY, VALUE_STRING),
        VALUE(VALUE_BOOLEAN, VALUE_NULL, VALUE_EMBEDDED_OBJECT, VALUE_NUMBER, VALUE_STRING);

        private final EnumSet<XContentParser.Token> tokens;

        ValueType(XContentParser.Token first, XContentParser.Token... rest) {
            this.tokens = EnumSet.of(first, rest);
        }

        public EnumSet<XContentParser.Token> supportedTokens() {
            return this.tokens;
        }
    }

    @Override
    public String toString() {
        return "ObjectParser{" + "name='" + name + '\'' + ", fields=" + fieldParserMap.values() + '}';
    }
}