package org.elasticsearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractScopedSettings {

    public static final String ARCHIVED_SETTINGS_PREFIX = "archived.";

    private static final Pattern KEY_PATTERN = Pattern.compile("^(?:[-\\w]+[.])*[-\\w]+$");

    private static final Pattern GROUP_KEY_PATTERN = Pattern.compile("^(?:[-\\w]+[.])+$");

    private static final Pattern AFFIX_KEY_PATTERN = Pattern.compile("^(?:[-\\w]+[.])+[*](?:[.][-\\w]+)+$");

    protected final Logger logger = LogManager.getLogger(this.getClass());

    private final Settings settings;

    private final List<SettingUpdater<?>> settingUpdaters = new CopyOnWriteArrayList<>();

    private final Map<String, Setting<?>> complexMatchers;

    private final Map<String, Setting<?>> keySettings;

    private final Map<Setting<?>, SettingUpgrader<?>> settingUpgraders;

    private final Setting.Property scope;

    private Settings lastSettingsApplied;

    protected AbstractScopedSettings(final Settings settings, final Set<Setting<?>> settingsSet, final Set<SettingUpgrader<?>> settingUpgraders, final Setting.Property scope) {
        this.settings = settings;
        this.lastSettingsApplied = Settings.EMPTY;
        this.settingUpgraders = Collections.unmodifiableMap(settingUpgraders.stream().collect(Collectors.toMap(SettingUpgrader::getSetting, Function.identity())));
        this.scope = scope;
        Map<String, Setting<?>> complexMatchers = new HashMap<>();
        Map<String, Setting<?>> keySettings = new HashMap<>();
        for (Setting<?> setting : settingsSet) {
            if (setting.getProperties().contains(scope) == false) {
                throw new IllegalArgumentException("Setting " + setting + " must be a " + scope + " setting but has: " + setting.getProperties());
            }
            validateSettingKey(setting);
            if (setting.hasComplexMatcher()) {
                Setting<?> overlappingSetting = findOverlappingSetting(setting, complexMatchers);
                if (overlappingSetting != null) {
                    throw new IllegalArgumentException("complex setting key: [" + setting.getKey() + "] overlaps existing setting key: [" + overlappingSetting.getKey() + "]");
                }
                complexMatchers.putIfAbsent(setting.getKey(), setting);
            } else {
                keySettings.putIfAbsent(setting.getKey(), setting);
            }
        }
        this.complexMatchers = Collections.unmodifiableMap(complexMatchers);
        this.keySettings = Collections.unmodifiableMap(keySettings);
    }

    protected void validateSettingKey(Setting<?> setting) {
        if (isValidKey(setting.getKey()) == false && (setting.isGroupSetting() && isValidGroupKey(setting.getKey()) || isValidAffixKey(setting.getKey())) == false || setting.getKey().endsWith(".0")) {
            throw new IllegalArgumentException("illegal settings key: [" + setting.getKey() + "]");
        }
    }

    protected AbstractScopedSettings(Settings nodeSettings, Settings scopeSettings, AbstractScopedSettings other) {
        this.settings = nodeSettings;
        this.lastSettingsApplied = scopeSettings;
        this.scope = other.scope;
        complexMatchers = other.complexMatchers;
        keySettings = other.keySettings;
        settingUpgraders = Collections.unmodifiableMap(new HashMap<>(other.settingUpgraders));
        settingUpdaters.addAll(other.settingUpdaters);
    }

    public static boolean isValidKey(String key) {
        return KEY_PATTERN.matcher(key).matches();
    }

    private static boolean isValidGroupKey(String key) {
        return GROUP_KEY_PATTERN.matcher(key).matches();
    }

    static boolean isValidAffixKey(String key) {
        return AFFIX_KEY_PATTERN.matcher(key).matches();
    }

    public Setting.Property getScope() {
        return this.scope;
    }

    public synchronized Settings validateUpdate(Settings settings) {
        final Settings current = Settings.builder().put(this.settings).put(settings).build();
        final Settings previous = Settings.builder().put(this.settings).put(this.lastSettingsApplied).build();
        List<RuntimeException> exceptions = new ArrayList<>();
        for (SettingUpdater<?> settingUpdater : settingUpdaters) {
            try {
                settingUpdater.getValue(current, previous);
            } catch (RuntimeException ex) {
                exceptions.add(ex);
                logger.debug(() -> new ParameterizedMessage("failed to prepareCommit settings for [{}]", settingUpdater), ex);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
        return current;
    }

    public synchronized Settings applySettings(Settings newSettings) {
        if (lastSettingsApplied != null && newSettings.equals(lastSettingsApplied)) {
            return newSettings;
        }
        final Settings current = Settings.builder().put(this.settings).put(newSettings).build();
        final Settings previous = Settings.builder().put(this.settings).put(this.lastSettingsApplied).build();
        try {
            List<Runnable> applyRunnables = new ArrayList<>();
            for (SettingUpdater<?> settingUpdater : settingUpdaters) {
                try {
                    applyRunnables.add(settingUpdater.updater(current, previous));
                } catch (Exception ex) {
                    logger.warn(() -> new ParameterizedMessage("failed to prepareCommit settings for [{}]", settingUpdater), ex);
                    throw ex;
                }
            }
            for (Runnable settingUpdater : applyRunnables) {
                settingUpdater.run();
            }
        } catch (Exception ex) {
            logger.warn("failed to apply settings", ex);
            throw ex;
        } finally {
        }
        return lastSettingsApplied = newSettings;
    }

    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer, Consumer<T> validator) {
        if (setting != get(setting.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + setting.getKey() + "]");
        }
        addSettingsUpdater(setting.newUpdater(consumer, logger, validator));
    }

    public synchronized void addSettingsUpdateConsumer(Consumer<Settings> consumer, List<? extends Setting<?>> settings) {
        addSettingsUpdater(Setting.groupedSettingsUpdater(consumer, settings));
    }

    public synchronized <T> void addAffixUpdateConsumer(Setting.AffixSetting<T> setting, BiConsumer<String, T> consumer, BiConsumer<String, T> validator) {
        ensureSettingIsRegistered(setting);
        addSettingsUpdater(setting.newAffixUpdater(consumer, logger, validator));
    }

    public synchronized <A, B> void addAffixUpdateConsumer(Setting.AffixSetting<A> settingA, Setting.AffixSetting<B> settingB, BiConsumer<String, Tuple<A, B>> consumer, BiConsumer<String, Tuple<A, B>> validator) {
        ensureSettingIsRegistered(settingA);
        ensureSettingIsRegistered(settingB);
        SettingUpdater<Map<SettingUpdater<A>, A>> affixUpdaterA = settingA.newAffixUpdater((a, b) -> {
        }, logger, (a, b) -> {
        });
        SettingUpdater<Map<SettingUpdater<B>, B>> affixUpdaterB = settingB.newAffixUpdater((a, b) -> {
        }, logger, (a, b) -> {
        });
        addSettingsUpdater(new SettingUpdater<Map<String, Tuple<A, B>>>() {

            @Override
            public boolean hasChanged(Settings current, Settings previous) {
                return affixUpdaterA.hasChanged(current, previous) || affixUpdaterB.hasChanged(current, previous);
            }

            @Override
            public Map<String, Tuple<A, B>> getValue(Settings current, Settings previous) {
                Map<String, Tuple<A, B>> map = new HashMap<>();
                BiConsumer<String, A> aConsumer = (key, value) -> {
                    assert map.containsKey(key) == false : "duplicate key: " + key;
                    map.put(key, new Tuple<>(value, settingB.getConcreteSettingForNamespace(key).get(current)));
                };
                BiConsumer<String, B> bConsumer = (key, value) -> {
                    Tuple<A, B> abTuple = map.get(key);
                    if (abTuple != null) {
                        map.put(key, new Tuple<>(abTuple.v1(), value));
                    } else {
                        assert settingA.getConcreteSettingForNamespace(key).get(current).equals(settingA.getConcreteSettingForNamespace(key).get(previous)) : "expected: " + settingA.getConcreteSettingForNamespace(key).get(current) + " but was " + settingA.getConcreteSettingForNamespace(key).get(previous);
                        map.put(key, new Tuple<>(settingA.getConcreteSettingForNamespace(key).get(current), value));
                    }
                };
                SettingUpdater<Map<SettingUpdater<A>, A>> affixUpdaterA = settingA.newAffixUpdater(aConsumer, logger, (a, b) -> {
                });
                SettingUpdater<Map<SettingUpdater<B>, B>> affixUpdaterB = settingB.newAffixUpdater(bConsumer, logger, (a, b) -> {
                });
                affixUpdaterA.apply(current, previous);
                affixUpdaterB.apply(current, previous);
                for (Map.Entry<String, Tuple<A, B>> entry : map.entrySet()) {
                    validator.accept(entry.getKey(), entry.getValue());
                }
                return Collections.unmodifiableMap(map);
            }

            @Override
            public void apply(Map<String, Tuple<A, B>> values, Settings current, Settings previous) {
                for (Map.Entry<String, Tuple<A, B>> entry : values.entrySet()) {
                    consumer.accept(entry.getKey(), entry.getValue());
                }
            }
        });
    }

    private void ensureSettingIsRegistered(Setting.AffixSetting<?> setting) {
        final Setting<?> registeredSetting = this.complexMatchers.get(setting.getKey());
        if (setting != registeredSetting) {
            throw new IllegalArgumentException("Setting is not registered for key [" + setting.getKey() + "]");
        }
    }

    public synchronized <T> void addAffixMapUpdateConsumer(Setting.AffixSetting<T> setting, Consumer<Map<String, T>> consumer, BiConsumer<String, T> validator) {
        final Setting<?> registeredSetting = this.complexMatchers.get(setting.getKey());
        if (setting != registeredSetting) {
            throw new IllegalArgumentException("Setting is not registered for key [" + setting.getKey() + "]");
        }
        addSettingsUpdater(setting.newAffixMapUpdater(consumer, logger, validator));
    }

    synchronized void addSettingsUpdater(SettingUpdater<?> updater) {
        this.settingUpdaters.add(updater);
    }

    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b, BiConsumer<A, B> consumer) {
        addSettingsUpdateConsumer(a, b, consumer, (i, j) -> {
        });
    }

    public synchronized <A, B> void addSettingsUpdateConsumer(Setting<A> a, Setting<B> b, BiConsumer<A, B> consumer, BiConsumer<A, B> validator) {
        if (a != get(a.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + a.getKey() + "]");
        }
        if (b != get(b.getKey())) {
            throw new IllegalArgumentException("Setting is not registered for key [" + b.getKey() + "]");
        }
        addSettingsUpdater(Setting.compoundUpdater(consumer, validator, a, b, logger));
    }

    public synchronized <T> void addSettingsUpdateConsumer(Setting<T> setting, Consumer<T> consumer) {
        addSettingsUpdateConsumer(setting, consumer, (s) -> {
        });
    }

    public final void validate(final Settings settings, final boolean validateDependencies) {
        validate(settings, validateDependencies, false, false);
    }

    public final void validate(final Settings settings, final boolean validateDependencies, final boolean validateInternalOrPrivateIndex) {
        validate(settings, validateDependencies, false, false, validateInternalOrPrivateIndex);
    }

    public final void validate(final Settings settings, final boolean validateDependencies, final boolean ignorePrivateSettings, final boolean ignoreArchivedSettings) {
        validate(settings, validateDependencies, ignorePrivateSettings, ignoreArchivedSettings, false);
    }

    public final void validate(final Settings settings, final boolean validateDependencies, final boolean ignorePrivateSettings, final boolean ignoreArchivedSettings, final boolean validateInternalOrPrivateIndex) {
        final List<RuntimeException> exceptions = new ArrayList<>();
        for (final String key : settings.keySet()) {
            final Setting<?> setting = getRaw(key);
            if (((isPrivateSetting(key) || (setting != null && setting.isPrivateIndex())) && ignorePrivateSettings)) {
                continue;
            }
            if (key.startsWith(ARCHIVED_SETTINGS_PREFIX) && ignoreArchivedSettings) {
                continue;
            }
            try {
                validate(key, settings, validateDependencies, validateInternalOrPrivateIndex);
            } catch (final RuntimeException ex) {
                exceptions.add(ex);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    void validate(final String key, final Settings settings, final boolean validateDependencies) {
        validate(key, settings, validateDependencies, false);
    }

    void validate(final String key, final Settings settings, final boolean validateDependencies, final boolean validateInternalOrPrivateIndex) {
        Setting setting = getRaw(key);
        if (setting == null) {
            LevenshteinDistance ld = new LevenshteinDistance();
            List<Tuple<Float, String>> scoredKeys = new ArrayList<>();
            for (String k : this.keySettings.keySet()) {
                float distance = ld.getDistance(key, k);
                if (distance > 0.7f) {
                    scoredKeys.add(new Tuple<>(distance, k));
                }
            }
            CollectionUtil.timSort(scoredKeys, (a, b) -> b.v1().compareTo(a.v1()));
            String msgPrefix = "unknown setting";
            SecureSettings secureSettings = settings.getSecureSettings();
            if (secureSettings != null && settings.getSecureSettings().getSettingNames().contains(key)) {
                msgPrefix = "unknown secure setting";
            }
            String msg = msgPrefix + " [" + key + "]";
            List<String> keys = scoredKeys.stream().map((a) -> a.v2()).collect(Collectors.toList());
            if (keys.isEmpty() == false) {
                msg += " did you mean " + (keys.size() == 1 ? "[" + keys.get(0) + "]" : "any of " + keys.toString()) + "?";
            } else {
                msg += " please check that any required plugins are installed, or check the breaking changes documentation for removed " + "settings";
            }
            throw new IllegalArgumentException(msg);
        } else {
            Set<Setting<?>> settingsDependencies = setting.getSettingsDependencies(key);
            if (setting.hasComplexMatcher()) {
                setting = setting.getConcreteSetting(key);
            }
            if (validateDependencies && settingsDependencies.isEmpty() == false) {
                for (final Setting<?> settingDependency : settingsDependencies) {
                    if (settingDependency.existsOrFallbackExists(settings) == false) {
                        final String message = String.format(Locale.ROOT, "missing required setting [%s] for setting [%s]", settingDependency.getKey(), setting.getKey());
                        throw new IllegalArgumentException(message);
                    }
                }
            }
            if (validateInternalOrPrivateIndex) {
                if (setting.isInternalIndex()) {
                    throw new IllegalArgumentException("can not update internal setting [" + setting.getKey() + "]; this setting is managed via a dedicated API");
                } else if (setting.isPrivateIndex()) {
                    throw new IllegalArgumentException("can not update private setting [" + setting.getKey() + "]; this setting is managed by Elasticsearch");
                }
            }
        }
        setting.get(settings);
    }

    public interface SettingUpdater<T> {

        boolean hasChanged(Settings current, Settings previous);

        T getValue(Settings current, Settings previous);

        void apply(T value, Settings current, Settings previous);

        default boolean apply(Settings current, Settings previous) {
            if (hasChanged(current, previous)) {
                T value = getValue(current, previous);
                apply(value, current, previous);
                return true;
            }
            return false;
        }

        default Runnable updater(Settings current, Settings previous) {
            if (hasChanged(current, previous)) {
                T value = getValue(current, previous);
                return () -> {
                    apply(value, current, previous);
                };
            }
            return () -> {
            };
        }
    }

    public final Setting<?> get(String key) {
        Setting<?> raw = getRaw(key);
        if (raw == null) {
            return null;
        }
        if (raw.hasComplexMatcher()) {
            return raw.getConcreteSetting(key);
        } else {
            return raw;
        }
    }

    private Setting<?> getRaw(String key) {
        Setting<?> setting = keySettings.get(key);
        if (setting != null) {
            return setting;
        }
        for (Map.Entry<String, Setting<?>> entry : complexMatchers.entrySet()) {
            if (entry.getValue().match(key)) {
                assert assertMatcher(key, 1);
                assert entry.getValue().hasComplexMatcher();
                return entry.getValue();
            }
        }
        return null;
    }

    private boolean assertMatcher(String key, int numComplexMatchers) {
        List<Setting<?>> list = new ArrayList<>();
        for (Map.Entry<String, Setting<?>> entry : complexMatchers.entrySet()) {
            if (entry.getValue().match(key)) {
                list.add(entry.getValue().getConcreteSetting(key));
            }
        }
        assert list.size() == numComplexMatchers : "Expected " + numComplexMatchers + " complex matchers to match key [" + key + "] but got: " + list.toString();
        return true;
    }

    public boolean isDynamicSetting(String key) {
        final Setting<?> setting = get(key);
        return setting != null && setting.isDynamic();
    }

    public boolean isFinalSetting(String key) {
        final Setting<?> setting = get(key);
        return setting != null && setting.isFinal();
    }

    public Settings diff(Settings source, Settings defaultSettings) {
        Settings.Builder builder = Settings.builder();
        for (Setting<?> setting : keySettings.values()) {
            setting.diff(builder, source, defaultSettings);
        }
        for (Setting<?> setting : complexMatchers.values()) {
            setting.diff(builder, source, defaultSettings);
        }
        return builder.build();
    }

    public <T> T get(Setting<T> setting) {
        if (setting.getProperties().contains(scope) == false) {
            throw new IllegalArgumentException("settings scope doesn't match the setting scope [" + this.scope + "] not in [" + setting.getProperties() + "]");
        }
        if (get(setting.getKey()) == null) {
            throw new IllegalArgumentException("setting " + setting.getKey() + " has not been registered");
        }
        return setting.get(this.lastSettingsApplied, settings);
    }

    public boolean updateDynamicSettings(Settings toApply, Settings.Builder target, Settings.Builder updates, String type) {
        return updateSettings(toApply, target, updates, type, true);
    }

    public boolean updateSettings(Settings toApply, Settings.Builder target, Settings.Builder updates, String type) {
        return updateSettings(toApply, target, updates, type, false);
    }

    private boolean isValidDelete(String key, boolean onlyDynamic) {
        return isFinalSetting(key) == false && (onlyDynamic && isDynamicSetting(key) || get(key) == null && key.startsWith(ARCHIVED_SETTINGS_PREFIX) || (onlyDynamic == false && get(key) != null));
    }

    private boolean updateSettings(Settings toApply, Settings.Builder target, Settings.Builder updates, String type, boolean onlyDynamic) {
        boolean changed = false;
        final Set<String> toRemove = new HashSet<>();
        Settings.Builder settingsBuilder = Settings.builder();
        final Predicate<String> canUpdate = (key) -> (isFinalSetting(key) == false && ((onlyDynamic == false && get(key) != null) || isDynamicSetting(key)));
        for (String key : toApply.keySet()) {
            boolean isDelete = toApply.hasValue(key) == false;
            if (isDelete && (isValidDelete(key, onlyDynamic) || key.endsWith("*"))) {
                toRemove.add(key);
            } else if (get(key) == null) {
                throw new IllegalArgumentException(type + " setting [" + key + "], not recognized");
            } else if (isDelete == false && canUpdate.test(key)) {
                validate(key, toApply, false);
                settingsBuilder.copy(key, toApply);
                updates.copy(key, toApply);
                changed = true;
            } else {
                if (isFinalSetting(key)) {
                    throw new IllegalArgumentException("final " + type + " setting [" + key + "], not updateable");
                } else {
                    throw new IllegalArgumentException(type + " setting [" + key + "], not dynamically updateable");
                }
            }
        }
        changed |= applyDeletes(toRemove, target, k -> isValidDelete(k, onlyDynamic));
        target.put(settingsBuilder.build());
        return changed;
    }

    private static boolean applyDeletes(Set<String> deletes, Settings.Builder builder, Predicate<String> canRemove) {
        boolean changed = false;
        for (String entry : deletes) {
            Set<String> keysToRemove = new HashSet<>();
            Set<String> keySet = builder.keys();
            for (String key : keySet) {
                if (Regex.simpleMatch(entry, key) && canRemove.test(key)) {
                    keysToRemove.add(key);
                }
            }
            for (String key : keysToRemove) {
                builder.remove(key);
                changed = true;
            }
        }
        return changed;
    }

    private static Setting<?> findOverlappingSetting(Setting<?> newSetting, Map<String, Setting<?>> complexMatchers) {
        assert newSetting.hasComplexMatcher();
        if (complexMatchers.containsKey(newSetting.getKey())) {
            return null;
        }
        for (Setting<?> existingSetting : complexMatchers.values()) {
            if (newSetting.match(existingSetting.getKey()) || existingSetting.match(newSetting.getKey())) {
                return existingSetting;
            }
        }
        return null;
    }

    public Settings upgradeSettings(final Settings settings) {
        final Settings.Builder builder = Settings.builder();
        boolean changed = false;
        for (final String key : settings.keySet()) {
            final Setting<?> setting = getRaw(key);
            final SettingUpgrader<?> upgrader = settingUpgraders.get(setting);
            if (upgrader == null) {
                builder.copy(key, settings);
            } else {
                changed = true;
                if (setting.getConcreteSetting(key).isListSetting()) {
                    final List<String> value = settings.getAsList(key);
                    final String upgradedKey = upgrader.getKey(key);
                    final List<String> upgradedValue = upgrader.getListValue(value);
                    builder.putList(upgradedKey, upgradedValue);
                } else {
                    final String value = settings.get(key);
                    final String upgradedKey = upgrader.getKey(key);
                    final String upgradedValue = upgrader.getValue(value);
                    builder.put(upgradedKey, upgradedValue);
                }
            }
        }
        return changed ? builder.build() : settings;
    }

    public Settings archiveUnknownOrInvalidSettings(final Settings settings, final Consumer<Map.Entry<String, String>> unknownConsumer, final BiConsumer<Map.Entry<String, String>, IllegalArgumentException> invalidConsumer) {
        Settings.Builder builder = Settings.builder();
        boolean changed = false;
        for (String key : settings.keySet()) {
            try {
                Setting<?> setting = get(key);
                if (setting != null) {
                    setting.get(settings);
                    builder.copy(key, settings);
                } else {
                    if (key.startsWith(ARCHIVED_SETTINGS_PREFIX) || isPrivateSetting(key)) {
                        builder.copy(key, settings);
                    } else {
                        changed = true;
                        unknownConsumer.accept(new Entry(key, settings));
                        builder.copy(ARCHIVED_SETTINGS_PREFIX + key, key, settings);
                    }
                }
            } catch (IllegalArgumentException ex) {
                changed = true;
                invalidConsumer.accept(new Entry(key, settings), ex);
                builder.copy(ARCHIVED_SETTINGS_PREFIX + key, key, settings);
            }
        }
        if (changed) {
            return builder.build();
        } else {
            return settings;
        }
    }

    private static final class Entry implements Map.Entry<String, String> {

        private final String key;

        private final Settings settings;

        private Entry(String key, Settings settings) {
            this.key = key;
            this.settings = settings;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return settings.get(key);
        }

        @Override
        public String setValue(String value) {
            throw new UnsupportedOperationException();
        }
    }

    public boolean isPrivateSetting(String key) {
        return false;
    }
}