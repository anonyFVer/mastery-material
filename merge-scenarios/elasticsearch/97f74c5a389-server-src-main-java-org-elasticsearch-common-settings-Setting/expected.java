package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Setting<T> implements ToXContentObject {

    public enum Property {

        Filtered,
        Dynamic,
        Final,
        Deprecated,
        NodeScope,
        IndexScope,
        NotCopyableOnResize,
        InternalIndex,
        PrivateIndex
    }

    private final Key key;

    protected final Function<Settings, String> defaultValue;

    @Nullable
    private final Setting<T> fallbackSetting;

    private final Function<String, T> parser;

    private final Validator<T> validator;

    private final EnumSet<Property> properties;

    private static final EnumSet<Property> EMPTY_PROPERTIES = EnumSet.noneOf(Property.class);

    private Setting(Key key, @Nullable Setting<T> fallbackSetting, Function<Settings, String> defaultValue, Function<String, T> parser, Validator<T> validator, Property... properties) {
        assert this instanceof SecureSetting || this.isGroupSetting() || parser.apply(defaultValue.apply(Settings.EMPTY)) != null : "parser returned null";
        this.key = key;
        this.fallbackSetting = fallbackSetting;
        this.defaultValue = defaultValue;
        this.parser = parser;
        this.validator = validator;
        if (properties == null) {
            throw new IllegalArgumentException("properties cannot be null for setting [" + key + "]");
        }
        if (properties.length == 0) {
            this.properties = EMPTY_PROPERTIES;
        } else {
            final EnumSet<Property> propertiesAsSet = EnumSet.copyOf(Arrays.asList(properties));
            if (propertiesAsSet.contains(Property.Dynamic) && propertiesAsSet.contains(Property.Final)) {
                throw new IllegalArgumentException("final setting [" + key + "] cannot be dynamic");
            }
            checkPropertyRequiresIndexScope(propertiesAsSet, Property.NotCopyableOnResize);
            checkPropertyRequiresIndexScope(propertiesAsSet, Property.InternalIndex);
            checkPropertyRequiresIndexScope(propertiesAsSet, Property.PrivateIndex);
            this.properties = propertiesAsSet;
        }
    }

    private void checkPropertyRequiresIndexScope(final EnumSet<Property> properties, final Property property) {
        if (properties.contains(property) && properties.contains(Property.IndexScope) == false) {
            throw new IllegalArgumentException("non-index-scoped setting [" + key + "] can not have property [" + property + "]");
        }
    }

    public Setting(Key key, Function<Settings, String> defaultValue, Function<String, T> parser, Property... properties) {
        this(key, defaultValue, parser, (v, s) -> {
        }, properties);
    }

    public Setting(Key key, Function<Settings, String> defaultValue, Function<String, T> parser, Validator<T> validator, Property... properties) {
        this(key, null, defaultValue, parser, validator, properties);
    }

    public Setting(String key, String defaultValue, Function<String, T> parser, Property... properties) {
        this(key, s -> defaultValue, parser, properties);
    }

    public Setting(String key, String defaultValue, Function<String, T> parser, Validator<T> validator, Property... properties) {
        this(new SimpleKey(key), s -> defaultValue, parser, validator, properties);
    }

    public Setting(String key, Function<Settings, String> defaultValue, Function<String, T> parser, Property... properties) {
        this(new SimpleKey(key), defaultValue, parser, properties);
    }

    public Setting(Key key, Setting<T> fallbackSetting, Function<String, T> parser, Property... properties) {
        this(key, fallbackSetting, fallbackSetting::getRaw, parser, (v, m) -> {
        }, properties);
    }

    public Setting(String key, Setting<T> fallBackSetting, Function<String, T> parser, Property... properties) {
        this(new SimpleKey(key), fallBackSetting, parser, properties);
    }

    public final String getKey() {
        return key.toString();
    }

    public final Key getRawKey() {
        return key;
    }

    public final boolean isDynamic() {
        return properties.contains(Property.Dynamic);
    }

    public final boolean isFinal() {
        return properties.contains(Property.Final);
    }

    public final boolean isInternalIndex() {
        return properties.contains(Property.InternalIndex);
    }

    public final boolean isPrivateIndex() {
        return properties.contains(Property.PrivateIndex);
    }

    public EnumSet<Property> getProperties() {
        return properties;
    }

    public boolean isFiltered() {
        return properties.contains(Property.Filtered);
    }

    public boolean hasNodeScope() {
        return properties.contains(Property.NodeScope);
    }

    public boolean hasIndexScope() {
        return properties.contains(Property.IndexScope);
    }

    public boolean isDeprecated() {
        return properties.contains(Property.Deprecated);
    }

    boolean isGroupSetting() {
        return false;
    }

    final boolean isListSetting() {
        return this instanceof ListSetting;
    }

    boolean hasComplexMatcher() {
        return isGroupSetting();
    }

    public String getDefaultRaw(Settings settings) {
        return defaultValue.apply(settings);
    }

    public T getDefault(Settings settings) {
        return parser.apply(getDefaultRaw(settings));
    }

    public boolean exists(final Settings settings) {
        return settings.keySet().contains(getKey());
    }

    public boolean existsOrFallbackExists(final Settings settings) {
        return settings.keySet().contains(getKey()) || (fallbackSetting != null && fallbackSetting.existsOrFallbackExists(settings));
    }

    public T get(Settings settings) {
        return get(settings, true);
    }

    private T get(Settings settings, boolean validate) {
        String value = getRaw(settings);
        try {
            T parsed = parser.apply(value);
            if (validate) {
                final Iterator<Setting<T>> it = validator.settings();
                final Map<Setting<T>, T> map;
                if (it.hasNext()) {
                    map = new HashMap<>();
                    while (it.hasNext()) {
                        final Setting<T> setting = it.next();
                        map.put(setting, setting.get(settings, false));
                    }
                } else {
                    map = Collections.emptyMap();
                }
                validator.validate(parsed, map);
            }
            return parsed;
        } catch (ElasticsearchParseException ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Failed to parse value [" + value + "] for setting [" + getKey() + "]", ex);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception t) {
            throw new IllegalArgumentException("Failed to parse value [" + value + "] for setting [" + getKey() + "]", t);
        }
    }

    public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
        if (exists(source) == false) {
            builder.put(getKey(), getRaw(defaultSettings));
        }
    }

    public final String getRaw(final Settings settings) {
        checkDeprecation(settings);
        return innerGetRaw(settings);
    }

    String innerGetRaw(final Settings settings) {
        return settings.get(getKey(), defaultValue.apply(settings));
    }

    void checkDeprecation(Settings settings) {
        if (this.isDeprecated() && this.exists(settings)) {
            final String key = getKey();
            Settings.DeprecationLoggerHolder.deprecationLogger.deprecatedAndMaybeLog(key, "[{}] setting was deprecated in Elasticsearch and will be removed in a future release! " + "See the breaking changes documentation for the next major version.", key);
        }
    }

    public final boolean match(String toTest) {
        return key.match(toTest);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("key", key.toString());
        builder.field("properties", properties);
        builder.field("is_group_setting", isGroupSetting());
        builder.field("default", defaultValue.apply(Settings.EMPTY));
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public final T get(Settings primary, Settings secondary) {
        if (exists(primary)) {
            return get(primary);
        }
        if (exists(secondary)) {
            return get(secondary);
        }
        if (fallbackSetting == null) {
            return get(primary);
        }
        if (fallbackSetting.exists(primary)) {
            return fallbackSetting.get(primary);
        }
        return fallbackSetting.get(secondary);
    }

    public Setting<T> getConcreteSetting(String key) {
        assert key.startsWith(this.getKey()) : "was " + key + " expected: " + getKey();
        return this;
    }

    public Set<Setting<?>> getSettingsDependencies(String key) {
        return Collections.emptySet();
    }

    final AbstractScopedSettings.SettingUpdater<T> newUpdater(Consumer<T> consumer, Logger logger) {
        return newUpdater(consumer, logger, (s) -> {
        });
    }

    AbstractScopedSettings.SettingUpdater<T> newUpdater(Consumer<T> consumer, Logger logger, Consumer<T> validator) {
        if (isDynamic()) {
            return new Updater(consumer, logger, validator);
        } else {
            throw new IllegalStateException("setting [" + getKey() + "] is not dynamic");
        }
    }

    static <A, B> AbstractScopedSettings.SettingUpdater<Tuple<A, B>> compoundUpdater(final BiConsumer<A, B> consumer, final BiConsumer<A, B> validator, final Setting<A> aSetting, final Setting<B> bSetting, Logger logger) {
        final AbstractScopedSettings.SettingUpdater<A> aSettingUpdater = aSetting.newUpdater(null, logger);
        final AbstractScopedSettings.SettingUpdater<B> bSettingUpdater = bSetting.newUpdater(null, logger);
        return new AbstractScopedSettings.SettingUpdater<Tuple<A, B>>() {

            @Override
            public boolean hasChanged(Settings current, Settings previous) {
                return aSettingUpdater.hasChanged(current, previous) || bSettingUpdater.hasChanged(current, previous);
            }

            @Override
            public Tuple<A, B> getValue(Settings current, Settings previous) {
                A valueA = aSettingUpdater.getValue(current, previous);
                B valueB = bSettingUpdater.getValue(current, previous);
                validator.accept(valueA, valueB);
                return new Tuple<>(valueA, valueB);
            }

            @Override
            public void apply(Tuple<A, B> value, Settings current, Settings previous) {
                if (aSettingUpdater.hasChanged(current, previous)) {
                    logSettingUpdate(aSetting, current, previous, logger);
                }
                if (bSettingUpdater.hasChanged(current, previous)) {
                    logSettingUpdate(bSetting, current, previous, logger);
                }
                consumer.accept(value.v1(), value.v2());
            }

            @Override
            public String toString() {
                return "CompoundUpdater for: " + aSettingUpdater + " and " + bSettingUpdater;
            }
        };
    }

    static AbstractScopedSettings.SettingUpdater<Settings> groupedSettingsUpdater(Consumer<Settings> consumer, final List<? extends Setting<?>> configuredSettings) {
        return new AbstractScopedSettings.SettingUpdater<Settings>() {

            private Settings get(Settings settings) {
                return settings.filter(s -> {
                    for (Setting<?> setting : configuredSettings) {
                        if (setting.key.match(s)) {
                            return true;
                        }
                    }
                    return false;
                });
            }

            @Override
            public boolean hasChanged(Settings current, Settings previous) {
                Settings currentSettings = get(current);
                Settings previousSettings = get(previous);
                return currentSettings.equals(previousSettings) == false;
            }

            @Override
            public Settings getValue(Settings current, Settings previous) {
                return get(current);
            }

            @Override
            public void apply(Settings value, Settings current, Settings previous) {
                consumer.accept(value);
            }

            @Override
            public String toString() {
                return "Updater grouped: " + configuredSettings.stream().map(Setting::getKey).collect(Collectors.joining(", "));
            }
        };
    }

    public static class AffixSetting<T> extends Setting<T> {

        private final AffixKey key;

        private final Function<String, Setting<T>> delegateFactory;

        private final Set<AffixSetting> dependencies;

        public AffixSetting(AffixKey key, Setting<T> delegate, Function<String, Setting<T>> delegateFactory, AffixSetting... dependencies) {
            super(key, delegate.defaultValue, delegate.parser, delegate.properties.toArray(new Property[0]));
            this.key = key;
            this.delegateFactory = delegateFactory;
            this.dependencies = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(dependencies)));
        }

        boolean isGroupSetting() {
            return true;
        }

        private Stream<String> matchStream(Settings settings) {
            return settings.keySet().stream().filter(this::match).map(key::getConcreteString);
        }

        public Set<Setting<?>> getSettingsDependencies(String settingsKey) {
            if (dependencies.isEmpty()) {
                return Collections.emptySet();
            } else {
                String namespace = key.getNamespace(settingsKey);
                return dependencies.stream().map(s -> (Setting<?>) s.getConcreteSettingForNamespace(namespace)).collect(Collectors.toSet());
            }
        }

        AbstractScopedSettings.SettingUpdater<Map<AbstractScopedSettings.SettingUpdater<T>, T>> newAffixUpdater(BiConsumer<String, T> consumer, Logger logger, BiConsumer<String, T> validator) {
            return new AbstractScopedSettings.SettingUpdater<Map<AbstractScopedSettings.SettingUpdater<T>, T>>() {

                @Override
                public boolean hasChanged(Settings current, Settings previous) {
                    return Stream.concat(matchStream(current), matchStream(previous)).findAny().isPresent();
                }

                @Override
                public Map<AbstractScopedSettings.SettingUpdater<T>, T> getValue(Settings current, Settings previous) {
                    final Map<AbstractScopedSettings.SettingUpdater<T>, T> result = new IdentityHashMap<>();
                    Stream.concat(matchStream(current), matchStream(previous)).distinct().forEach(aKey -> {
                        String namespace = key.getNamespace(aKey);
                        Setting<T> concreteSetting = getConcreteSetting(aKey);
                        AbstractScopedSettings.SettingUpdater<T> updater = concreteSetting.newUpdater((v) -> consumer.accept(namespace, v), logger, (v) -> validator.accept(namespace, v));
                        if (updater.hasChanged(current, previous)) {
                            T value = updater.getValue(current, previous);
                            result.put(updater, value);
                        }
                    });
                    return result;
                }

                @Override
                public void apply(Map<AbstractScopedSettings.SettingUpdater<T>, T> value, Settings current, Settings previous) {
                    for (Map.Entry<AbstractScopedSettings.SettingUpdater<T>, T> entry : value.entrySet()) {
                        entry.getKey().apply(entry.getValue(), current, previous);
                    }
                }
            };
        }

        AbstractScopedSettings.SettingUpdater<Map<String, T>> newAffixMapUpdater(Consumer<Map<String, T>> consumer, Logger logger, BiConsumer<String, T> validator) {
            return new AbstractScopedSettings.SettingUpdater<Map<String, T>>() {

                @Override
                public boolean hasChanged(Settings current, Settings previous) {
                    return current.filter(k -> match(k)).equals(previous.filter(k -> match(k))) == false;
                }

                @Override
                public Map<String, T> getValue(Settings current, Settings previous) {
                    final Map<String, T> result = new IdentityHashMap<>();
                    Stream.concat(matchStream(current), matchStream(previous)).distinct().forEach(aKey -> {
                        String namespace = key.getNamespace(aKey);
                        Setting<T> concreteSetting = getConcreteSetting(aKey);
                        AbstractScopedSettings.SettingUpdater<T> updater = concreteSetting.newUpdater((v) -> {
                        }, logger, (v) -> validator.accept(namespace, v));
                        if (updater.hasChanged(current, previous)) {
                            T value = updater.getValue(current, previous);
                            result.put(namespace, value);
                        }
                    });
                    return result;
                }

                @Override
                public void apply(Map<String, T> value, Settings current, Settings previous) {
                    consumer.accept(value);
                }
            };
        }

        @Override
        public T get(Settings settings) {
            throw new UnsupportedOperationException("affix settings can't return values" + " use #getConcreteSetting to obtain a concrete setting");
        }

        @Override
        public String innerGetRaw(final Settings settings) {
            throw new UnsupportedOperationException("affix settings can't return values" + " use #getConcreteSetting to obtain a concrete setting");
        }

        @Override
        public Setting<T> getConcreteSetting(String key) {
            if (match(key)) {
                return delegateFactory.apply(key);
            } else {
                throw new IllegalArgumentException("key [" + key + "] must match [" + getKey() + "] but didn't.");
            }
        }

        public Setting<T> getConcreteSettingForNamespace(String namespace) {
            String fullKey = key.toConcreteKey(namespace).toString();
            return getConcreteSetting(fullKey);
        }

        @Override
        public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
            matchStream(defaultSettings).forEach((key) -> getConcreteSetting(key).diff(builder, source, defaultSettings));
        }

        public String getNamespace(Setting<T> concreteSetting) {
            return key.getNamespace(concreteSetting.getKey());
        }

        public Stream<Setting<T>> getAllConcreteSettings(Settings settings) {
            return matchStream(settings).distinct().map(this::getConcreteSetting);
        }

        public Set<String> getNamespaces(Settings settings) {
            return settings.keySet().stream().filter(this::match).map(key::getNamespace).collect(Collectors.toSet());
        }

        public Map<String, T> getAsMap(Settings settings) {
            Map<String, T> map = new HashMap<>();
            matchStream(settings).distinct().forEach(key -> {
                Setting<T> concreteSetting = getConcreteSetting(key);
                map.put(getNamespace(concreteSetting), concreteSetting.get(settings));
            });
            return Collections.unmodifiableMap(map);
        }
    }

    @FunctionalInterface
    public interface Validator<T> {

        void validate(T value, Map<Setting<T>, T> settings);

        default Iterator<Setting<T>> settings() {
            return Collections.emptyIterator();
        }
    }

    private static class GroupSetting extends Setting<Settings> {

        private final String key;

        private final Consumer<Settings> validator;

        private GroupSetting(String key, Consumer<Settings> validator, Property... properties) {
            super(new GroupKey(key), (s) -> "", (s) -> null, properties);
            this.key = key;
            this.validator = validator;
        }

        @Override
        public boolean isGroupSetting() {
            return true;
        }

        @Override
        public String innerGetRaw(final Settings settings) {
            Settings subSettings = get(settings);
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                subSettings.toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return Strings.toString(builder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Settings get(Settings settings) {
            Settings byPrefix = settings.getByPrefix(getKey());
            validator.accept(byPrefix);
            return byPrefix;
        }

        @Override
        public boolean exists(Settings settings) {
            for (String settingsKey : settings.keySet()) {
                if (settingsKey.startsWith(key)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
            Set<String> leftGroup = get(source).keySet();
            Settings defaultGroup = get(defaultSettings);
            builder.put(Settings.builder().put(defaultGroup.filter(k -> leftGroup.contains(k) == false), false).normalizePrefix(getKey()).build(), false);
        }

        @Override
        public AbstractScopedSettings.SettingUpdater<Settings> newUpdater(Consumer<Settings> consumer, Logger logger, Consumer<Settings> validator) {
            if (isDynamic() == false) {
                throw new IllegalStateException("setting [" + getKey() + "] is not dynamic");
            }
            final Setting<?> setting = this;
            return new AbstractScopedSettings.SettingUpdater<Settings>() {

                @Override
                public boolean hasChanged(Settings current, Settings previous) {
                    Settings currentSettings = get(current);
                    Settings previousSettings = get(previous);
                    return currentSettings.equals(previousSettings) == false;
                }

                @Override
                public Settings getValue(Settings current, Settings previous) {
                    Settings currentSettings = get(current);
                    Settings previousSettings = get(previous);
                    try {
                        validator.accept(currentSettings);
                    } catch (Exception | AssertionError e) {
                        throw new IllegalArgumentException("illegal value can't update [" + key + "] from [" + previousSettings + "] to [" + currentSettings + "]", e);
                    }
                    return currentSettings;
                }

                @Override
                public void apply(Settings value, Settings current, Settings previous) {
                    Setting.logSettingUpdate(GroupSetting.this, current, previous, logger);
                    consumer.accept(value);
                }

                @Override
                public String toString() {
                    return "Updater for: " + setting.toString();
                }
            };
        }
    }

    private final class Updater implements AbstractScopedSettings.SettingUpdater<T> {

        private final Consumer<T> consumer;

        private final Logger logger;

        private final Consumer<T> accept;

        Updater(Consumer<T> consumer, Logger logger, Consumer<T> accept) {
            this.consumer = consumer;
            this.logger = logger;
            this.accept = accept;
        }

        @Override
        public String toString() {
            return "Updater for: " + Setting.this.toString();
        }

        @Override
        public boolean hasChanged(Settings current, Settings previous) {
            final String newValue = getRaw(current);
            final String value = getRaw(previous);
            assert isGroupSetting() == false : "group settings must override this method";
            assert value != null : "value was null but can't be unless default is null which is invalid";
            return value.equals(newValue) == false;
        }

        @Override
        public T getValue(Settings current, Settings previous) {
            final String newValue = getRaw(current);
            final String value = getRaw(previous);
            try {
                T inst = get(current);
                accept.accept(inst);
                return inst;
            } catch (Exception | AssertionError e) {
                throw new IllegalArgumentException("illegal value can't update [" + key + "] from [" + value + "] to [" + newValue + "]", e);
            }
        }

        @Override
        public void apply(T value, Settings current, Settings previous) {
            logSettingUpdate(Setting.this, current, previous, logger);
            consumer.accept(value);
        }
    }

    public static Setting<Version> versionSetting(final String key, final Version defaultValue, Property... properties) {
        return new Setting<>(key, s -> Integer.toString(defaultValue.id), s -> Version.fromId(Integer.parseInt(s)), properties);
    }

    public static Setting<Float> floatSetting(String key, float defaultValue, Property... properties) {
        return new Setting<>(key, (s) -> Float.toString(defaultValue), Float::parseFloat, properties);
    }

    public static Setting<Float> floatSetting(String key, float defaultValue, float minValue, Property... properties) {
        return new Setting<>(key, (s) -> Float.toString(defaultValue), (s) -> {
            float value = Float.parseFloat(s);
            if (value < minValue) {
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
            }
            return value;
        }, properties);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, int maxValue, Property... properties) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue), (s) -> parseInt(s, minValue, maxValue, key), properties);
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, int minValue, Property... properties) {
        return new Setting<>(key, (s) -> Integer.toString(defaultValue), (s) -> parseInt(s, minValue, key), properties);
    }

    public static Setting<Integer> intSetting(String key, Setting<Integer> fallbackSetting, int minValue, Property... properties) {
        return new Setting<>(key, fallbackSetting, (s) -> parseInt(s, minValue, key), properties);
    }

    public static Setting<Integer> intSetting(String key, Setting<Integer> fallbackSetting, int minValue, Validator<Integer> validator, Property... properties) {
        return new Setting<>(new SimpleKey(key), fallbackSetting, fallbackSetting::getRaw, (s) -> parseInt(s, minValue, key), validator, properties);
    }

    public static Setting<Long> longSetting(String key, long defaultValue, long minValue, Property... properties) {
        return new Setting<>(key, (s) -> Long.toString(defaultValue), (s) -> parseLong(s, minValue, key), properties);
    }

    public static Setting<String> simpleString(String key, Property... properties) {
        return new Setting<>(key, s -> "", Function.identity(), properties);
    }

    public static Setting<String> simpleString(String key, Function<String, String> parser, Property... properties) {
        return new Setting<>(key, s -> "", parser, properties);
    }

    public static Setting<String> simpleString(String key, Setting<String> fallback, Property... properties) {
        return simpleString(key, fallback, Function.identity(), properties);
    }

    public static Setting<String> simpleString(final String key, final Setting<String> fallback, final Function<String, String> parser, final Property... properties) {
        return new Setting<>(key, fallback, parser, properties);
    }

    public static Setting<String> simpleString(String key, Validator<String> validator, Property... properties) {
        return new Setting<>(new SimpleKey(key), null, s -> "", Function.identity(), validator, properties);
    }

    public static Setting<String> simpleString(String key, String defaultValue, Property... properties) {
        return new Setting<>(key, s -> defaultValue, Function.identity(), properties);
    }

    public static int parseInt(String s, int minValue, String key) {
        return parseInt(s, minValue, Integer.MAX_VALUE, key);
    }

    public static int parseInt(String s, int minValue, int maxValue, String key) {
        int value = Integer.parseInt(s);
        if (value < minValue) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
        }
        if (value > maxValue) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be <= " + maxValue);
        }
        return value;
    }

    public static long parseLong(String s, long minValue, String key) {
        long value = Long.parseLong(s);
        if (value < minValue) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
        }
        return value;
    }

    public static TimeValue parseTimeValue(String s, TimeValue minValue, String key) {
        TimeValue timeValue = TimeValue.parseTimeValue(s, null, key);
        if (timeValue.millis() < minValue.millis()) {
            throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
        }
        return timeValue;
    }

    public static Setting<Integer> intSetting(String key, int defaultValue, Property... properties) {
        return intSetting(key, defaultValue, Integer.MIN_VALUE, properties);
    }

    public static Setting<Boolean> boolSetting(String key, boolean defaultValue, Property... properties) {
        return new Setting<>(key, (s) -> Boolean.toString(defaultValue), Booleans::parseBoolean, properties);
    }

    public static Setting<Boolean> boolSetting(String key, Setting<Boolean> fallbackSetting, Property... properties) {
        return new Setting<>(key, fallbackSetting, Booleans::parseBoolean, properties);
    }

    public static Setting<Boolean> boolSetting(String key, Function<Settings, String> defaultValueFn, Property... properties) {
        return new Setting<>(key, defaultValueFn, Booleans::parseBoolean, properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, ByteSizeValue value, Property... properties) {
        return byteSizeSetting(key, (s) -> value.toString(), properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Setting<ByteSizeValue> fallbackSetting, Property... properties) {
        return new Setting<>(key, fallbackSetting, (s) -> ByteSizeValue.parseBytesSizeValue(s, key), properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Function<Settings, String> defaultValue, Property... properties) {
        return new Setting<>(key, defaultValue, (s) -> ByteSizeValue.parseBytesSizeValue(s, key), properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, ByteSizeValue defaultValue, ByteSizeValue minValue, ByteSizeValue maxValue, Property... properties) {
        return byteSizeSetting(key, (s) -> defaultValue.getStringRep(), minValue, maxValue, properties);
    }

    public static Setting<ByteSizeValue> byteSizeSetting(String key, Function<Settings, String> defaultValue, ByteSizeValue minValue, ByteSizeValue maxValue, Property... properties) {
        return new Setting<>(key, defaultValue, (s) -> parseByteSize(s, minValue, maxValue, key), properties);
    }

    public static ByteSizeValue parseByteSize(String s, ByteSizeValue minValue, ByteSizeValue maxValue, String key) {
        ByteSizeValue value = ByteSizeValue.parseBytesSizeValue(s, key);
        if (value.getBytes() < minValue.getBytes()) {
            final String message = String.format(Locale.ROOT, "failed to parse value [%s] for setting [%s], must be >= [%s]", s, key, minValue.getStringRep());
            throw new IllegalArgumentException(message);
        }
        if (value.getBytes() > maxValue.getBytes()) {
            final String message = String.format(Locale.ROOT, "failed to parse value [%s] for setting [%s], must be <= [%s]", s, key, maxValue.getStringRep());
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public static Setting<ByteSizeValue> memorySizeSetting(String key, ByteSizeValue defaultValue, Property... properties) {
        return memorySizeSetting(key, (s) -> defaultValue.toString(), properties);
    }

    public static Setting<ByteSizeValue> memorySizeSetting(String key, Function<Settings, String> defaultValue, Property... properties) {
        return new Setting<>(key, defaultValue, (s) -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, key), properties);
    }

    public static Setting<ByteSizeValue> memorySizeSetting(String key, String defaultPercentage, Property... properties) {
        return new Setting<>(key, (s) -> defaultPercentage, (s) -> MemorySizeValue.parseBytesSizeValueOrHeapRatio(s, key), properties);
    }

    public static <T> Setting<List<T>> listSetting(final String key, final List<String> defaultStringValue, final Function<String, T> singleValueParser, final Property... properties) {
        return listSetting(key, null, singleValueParser, (s) -> defaultStringValue, properties);
    }

    public static <T> Setting<List<T>> listSetting(final String key, final Setting<List<T>> fallbackSetting, final Function<String, T> singleValueParser, final Property... properties) {
        return listSetting(key, fallbackSetting, singleValueParser, (s) -> parseableStringToList(fallbackSetting.getRaw(s)), properties);
    }

    public static <T> Setting<List<T>> listSetting(final String key, final Function<String, T> singleValueParser, final Function<Settings, List<String>> defaultStringValue, final Property... properties) {
        return listSetting(key, null, singleValueParser, defaultStringValue, properties);
    }

    public static <T> Setting<List<T>> listSetting(final String key, @Nullable final Setting<List<T>> fallbackSetting, final Function<String, T> singleValueParser, final Function<Settings, List<String>> defaultStringValue, final Property... properties) {
        if (defaultStringValue.apply(Settings.EMPTY) == null) {
            throw new IllegalArgumentException("default value function must not return null");
        }
        Function<String, List<T>> parser = (s) -> parseableStringToList(s).stream().map(singleValueParser).collect(Collectors.toList());
        return new ListSetting<>(key, fallbackSetting, defaultStringValue, parser, properties);
    }

    private static List<String> parseableStringToList(String parsableString) {
        try (XContentParser xContentParser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, parsableString)) {
            XContentParser.Token token = xContentParser.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("expected START_ARRAY but got " + token);
            }
            ArrayList<String> list = new ArrayList<>();
            while ((token = xContentParser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token != XContentParser.Token.VALUE_STRING) {
                    throw new IllegalArgumentException("expected VALUE_STRING but got " + token);
                }
                list.add(xContentParser.text());
            }
            return list;
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse array", e);
        }
    }

    private static String arrayToParsableString(List<String> array) {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startArray();
            for (String element : array) {
                builder.value(element);
            }
            builder.endArray();
            return Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException(ex);
        }
    }

    private static class ListSetting<T> extends Setting<List<T>> {

        private final Function<Settings, List<String>> defaultStringValue;

        private ListSetting(final String key, @Nullable final Setting<List<T>> fallbackSetting, final Function<Settings, List<String>> defaultStringValue, final Function<String, List<T>> parser, final Property... properties) {
            super(new ListKey(key), fallbackSetting, (s) -> Setting.arrayToParsableString(defaultStringValue.apply(s)), parser, (v, s) -> {
            }, properties);
            this.defaultStringValue = defaultStringValue;
        }

        @Override
        String innerGetRaw(final Settings settings) {
            List<String> array = settings.getAsList(getKey(), null);
            return array == null ? defaultValue.apply(settings) : arrayToParsableString(array);
        }

        @Override
        boolean hasComplexMatcher() {
            return true;
        }

        @Override
        public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
            if (exists(source) == false) {
                List<String> asList = defaultSettings.getAsList(getKey(), null);
                if (asList == null) {
                    builder.putList(getKey(), defaultStringValue.apply(defaultSettings));
                } else {
                    builder.putList(getKey(), asList);
                }
            }
        }
    }

    static void logSettingUpdate(Setting setting, Settings current, Settings previous, Logger logger) {
        if (logger.isInfoEnabled()) {
            if (setting.isFiltered()) {
                logger.info("updating [{}]", setting.key);
            } else {
                logger.info("updating [{}] from [{}] to [{}]", setting.key, setting.getRaw(previous), setting.getRaw(current));
            }
        }
    }

    public static Setting<Settings> groupSetting(String key, Property... properties) {
        return groupSetting(key, (s) -> {
        }, properties);
    }

    public static Setting<Settings> groupSetting(String key, Consumer<Settings> validator, Property... properties) {
        return new GroupSetting(key, validator, properties);
    }

    public static Setting<TimeValue> timeSetting(final String key, final Setting<TimeValue> fallbackSetting, final TimeValue minValue, final Property... properties) {
        final SimpleKey simpleKey = new SimpleKey(key);
        return new Setting<>(simpleKey, fallbackSetting, fallbackSetting::getRaw, minTimeValueParser(key, minValue), (v, s) -> {
        }, properties);
    }

    public static Setting<TimeValue> timeSetting(final String key, Function<Settings, TimeValue> defaultValue, final TimeValue minValue, final Property... properties) {
        final SimpleKey simpleKey = new SimpleKey(key);
        return new Setting<>(simpleKey, s -> defaultValue.apply(s).getStringRep(), minTimeValueParser(key, minValue), properties);
    }

    public static Setting<TimeValue> timeSetting(final String key, TimeValue defaultValue, final TimeValue minValue, final TimeValue maxValue, final Property... properties) {
        final SimpleKey simpleKey = new SimpleKey(key);
        return new Setting<>(simpleKey, s -> defaultValue.getStringRep(), minMaxTimeValueParser(key, minValue, maxValue), properties);
    }

    private static Function<String, TimeValue> minTimeValueParser(final String key, final TimeValue minValue) {
        return s -> {
            final TimeValue value = TimeValue.parseTimeValue(s, null, key);
            if (value.millis() < minValue.millis()) {
                final String message = String.format(Locale.ROOT, "failed to parse value [%s] for setting [%s], must be >= [%s]", s, key, minValue.getStringRep());
                throw new IllegalArgumentException(message);
            }
            return value;
        };
    }

    private static Function<String, TimeValue> minMaxTimeValueParser(final String key, final TimeValue minValue, final TimeValue maxValue) {
        return s -> {
            final TimeValue value = minTimeValueParser(key, minValue).apply(s);
            if (value.millis() > maxValue.millis()) {
                final String message = String.format(Locale.ROOT, "failed to parse value [%s] for setting [%s], must be <= [%s]", s, key, maxValue.getStringRep());
                throw new IllegalArgumentException(message);
            }
            return value;
        };
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, TimeValue minValue, Property... properties) {
        return timeSetting(key, (s) -> defaultValue, minValue, properties);
    }

    public static Setting<TimeValue> timeSetting(String key, TimeValue defaultValue, Property... properties) {
        return new Setting<>(key, (s) -> defaultValue.getStringRep(), (s) -> TimeValue.parseTimeValue(s, key), properties);
    }

    public static Setting<TimeValue> timeSetting(String key, Setting<TimeValue> fallbackSetting, Property... properties) {
        return new Setting<>(key, fallbackSetting, (s) -> TimeValue.parseTimeValue(s, key), properties);
    }

    public static Setting<TimeValue> positiveTimeSetting(String key, TimeValue defaultValue, Property... properties) {
        return timeSetting(key, defaultValue, TimeValue.timeValueMillis(0), properties);
    }

    public static Setting<TimeValue> positiveTimeSetting(final String key, final Setting<TimeValue> fallbackSetting, final TimeValue minValue, final Property... properties) {
        return timeSetting(key, fallbackSetting, minValue, properties);
    }

    public static Setting<Double> doubleSetting(String key, double defaultValue, double minValue, Property... properties) {
        return doubleSetting(key, defaultValue, minValue, Double.POSITIVE_INFINITY, properties);
    }

    public static Setting<Double> doubleSetting(String key, double defaultValue, double minValue, double maxValue, Property... properties) {
        return new Setting<>(key, (s) -> Double.toString(defaultValue), (s) -> {
            final double d = Double.parseDouble(s);
            if (d < minValue) {
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be >= " + minValue);
            }
            if (d > maxValue) {
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + key + "] must be <= " + maxValue);
            }
            return d;
        }, properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Setting<?> setting = (Setting<?>) o;
        return Objects.equals(key, setting.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    public static <T> AffixSetting<T> prefixKeySetting(String prefix, Function<String, Setting<T>> delegateFactory) {
        return affixKeySetting(new AffixKey(prefix), delegateFactory);
    }

    public static <T> AffixSetting<T> affixKeySetting(String prefix, String suffix, Function<String, Setting<T>> delegateFactory, AffixSetting... dependencies) {
        return affixKeySetting(new AffixKey(prefix, suffix), delegateFactory, dependencies);
    }

    private static <T> AffixSetting<T> affixKeySetting(AffixKey key, Function<String, Setting<T>> delegateFactory, AffixSetting... dependencies) {
        Setting<T> delegate = delegateFactory.apply("_na_");
        return new AffixSetting<>(key, delegate, delegateFactory, dependencies);
    }

    public interface Key {

        boolean match(String key);
    }

    public static class SimpleKey implements Key {

        protected final String key;

        public SimpleKey(String key) {
            this.key = key;
        }

        @Override
        public boolean match(String key) {
            return this.key.equals(key);
        }

        @Override
        public String toString() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            SimpleKey simpleKey = (SimpleKey) o;
            return Objects.equals(key, simpleKey.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    public static final class GroupKey extends SimpleKey {

        public GroupKey(String key) {
            super(key);
            if (key.endsWith(".") == false) {
                throw new IllegalArgumentException("key must end with a '.'");
            }
        }

        @Override
        public boolean match(String toTest) {
            return Regex.simpleMatch(key + "*", toTest);
        }
    }

    public static final class ListKey extends SimpleKey {

        private final Pattern pattern;

        public ListKey(String key) {
            super(key);
            this.pattern = Pattern.compile(Pattern.quote(key) + "(\\.\\d+)?");
        }

        @Override
        public boolean match(String toTest) {
            return pattern.matcher(toTest).matches();
        }
    }

    public static final class AffixKey implements Key {

        private final Pattern pattern;

        private final String prefix;

        private final String suffix;

        AffixKey(String prefix) {
            this(prefix, null);
        }

        AffixKey(String prefix, String suffix) {
            assert prefix != null || suffix != null : "Either prefix or suffix must be non-null";
            this.prefix = prefix;
            if (prefix.endsWith(".") == false) {
                throw new IllegalArgumentException("prefix must end with a '.'");
            }
            this.suffix = suffix;
            if (suffix == null) {
                pattern = Pattern.compile("(" + Pattern.quote(prefix) + "((?:[-\\w]+[.])*[-\\w]+$))");
            } else {
                pattern = Pattern.compile("(" + Pattern.quote(prefix) + "([-\\w]+)\\." + Pattern.quote(suffix) + ")(?:\\..*)?");
            }
        }

        @Override
        public boolean match(String key) {
            return pattern.matcher(key).matches();
        }

        String getConcreteString(String key) {
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches() == false) {
                throw new IllegalStateException("can't get concrete string for key " + key + " key doesn't match");
            }
            return matcher.group(1);
        }

        String getNamespace(String key) {
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches() == false) {
                throw new IllegalStateException("can't get concrete string for key " + key + " key doesn't match");
            }
            return matcher.group(2);
        }

        public SimpleKey toConcreteKey(String missingPart) {
            StringBuilder key = new StringBuilder();
            if (prefix != null) {
                key.append(prefix);
            }
            key.append(missingPart);
            if (suffix != null) {
                key.append(".");
                key.append(suffix);
            }
            return new SimpleKey(key.toString());
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (prefix != null) {
                sb.append(prefix);
            }
            if (suffix != null) {
                sb.append('*');
                sb.append('.');
                sb.append(suffix);
            }
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AffixKey that = (AffixKey) o;
            return Objects.equals(prefix, that.prefix) && Objects.equals(suffix, that.suffix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, suffix);
        }
    }
}