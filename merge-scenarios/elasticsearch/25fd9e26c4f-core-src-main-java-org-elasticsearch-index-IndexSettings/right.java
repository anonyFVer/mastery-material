package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.Node;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public final class IndexSettings {

    public static final Setting<String> DEFAULT_FIELD_SETTING = new Setting<>("index.query.default_field", AllFieldMapper.NAME, Function.identity(), Property.IndexScope);

    public static final Setting<Boolean> QUERY_STRING_LENIENT_SETTING = Setting.boolSetting("index.query_string.lenient", false, Property.IndexScope);

    public static final Setting<Boolean> QUERY_STRING_ANALYZE_WILDCARD = Setting.boolSetting("indices.query.query_string.analyze_wildcard", false, Property.NodeScope);

    public static final Setting<Boolean> QUERY_STRING_ALLOW_LEADING_WILDCARD = Setting.boolSetting("indices.query.query_string.allowLeadingWildcard", true, Property.NodeScope);

    public static final Setting<Boolean> ALLOW_UNMAPPED = Setting.boolSetting("index.query.parse.allow_unmapped_fields", true, Property.IndexScope);

    public static final Setting<TimeValue> INDEX_TRANSLOG_SYNC_INTERVAL_SETTING = Setting.timeSetting("index.translog.sync_interval", TimeValue.timeValueSeconds(5), TimeValue.timeValueMillis(100), Property.IndexScope);

    public static final Setting<Translog.Durability> INDEX_TRANSLOG_DURABILITY_SETTING = new Setting<>("index.translog.durability", Translog.Durability.REQUEST.name(), (value) -> Translog.Durability.valueOf(value.toUpperCase(Locale.ROOT)), Property.Dynamic, Property.IndexScope);

    public static final Setting<Boolean> INDEX_WARMER_ENABLED_SETTING = Setting.boolSetting("index.warmer.enabled", true, Property.Dynamic, Property.IndexScope);

    public static final Setting<Boolean> INDEX_TTL_DISABLE_PURGE_SETTING = Setting.boolSetting("index.ttl.disable_purge", false, Property.Dynamic, Property.IndexScope);

    public static final Setting<String> INDEX_CHECK_ON_STARTUP = new Setting<>("index.shard.check_on_startup", "false", (s) -> {
        switch(s) {
            case "false":
            case "true":
            case "fix":
            case "checksum":
                return s;
            default:
                throw new IllegalArgumentException("unknown value for [index.shard.check_on_startup] must be one of [true, false, fix, checksum] but was: " + s);
        }
    }, Property.IndexScope);

    public static final Setting<Integer> MAX_RESULT_WINDOW_SETTING = Setting.intSetting("index.max_result_window", 10000, 1, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_RESCORE_WINDOW_SETTING = Setting.intSetting("index.max_rescore_window", MAX_RESULT_WINDOW_SETTING, 1, Property.Dynamic, Property.IndexScope);

    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);

    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING = Setting.timeSetting("index.refresh_interval", DEFAULT_REFRESH_INTERVAL, new TimeValue(-1, TimeUnit.MILLISECONDS), Property.Dynamic, Property.IndexScope);

    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting("index.translog.flush_threshold_size", new ByteSizeValue(512, ByteSizeUnit.MB), Property.Dynamic, Property.IndexScope);

    public static final TimeValue DEFAULT_GC_DELETES = TimeValue.timeValueSeconds(60);

    public static final Setting<TimeValue> INDEX_GC_DELETES_SETTING = Setting.timeSetting("index.gc_deletes", DEFAULT_GC_DELETES, new TimeValue(-1, TimeUnit.MILLISECONDS), Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_REFRESH_LISTENERS_PER_SHARD = Setting.intSetting("index.max_refresh_listeners", 1000, 0, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_SLICES_PER_SCROLL = Setting.intSetting("index.max_slices_per_scroll", 1024, 1, Property.Dynamic, Property.IndexScope);

    private final Index index;

    private final Version version;

    private final Logger logger;

    private final String nodeName;

    private final Settings nodeSettings;

    private final int numberOfShards;

    private final boolean isShadowReplicaIndex;

    private final ParseFieldMatcher parseFieldMatcher;

    private volatile Settings settings;

    private volatile IndexMetaData indexMetaData;

    private final String defaultField;

    private final boolean queryStringLenient;

    private final boolean queryStringAnalyzeWildcard;

    private final boolean queryStringAllowLeadingWildcard;

    private final boolean defaultAllowUnmappedFields;

    private final Predicate<String> indexNameMatcher;

    private volatile Translog.Durability durability;

    private final TimeValue syncInterval;

    private volatile TimeValue refreshInterval;

    private volatile ByteSizeValue flushThresholdSize;

    private final MergeSchedulerConfig mergeSchedulerConfig;

    private final MergePolicyConfig mergePolicyConfig;

    private final IndexScopedSettings scopedSettings;

    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();

    private volatile boolean warmerEnabled;

    private volatile int maxResultWindow;

    private volatile int maxRescoreWindow;

    private volatile boolean TTLPurgeDisabled;

    private volatile int maxRefreshListeners;

    private volatile int maxSlicesPerScroll;

    public String getDefaultField() {
        return defaultField;
    }

    public boolean isQueryStringLenient() {
        return queryStringLenient;
    }

    public boolean isQueryStringAnalyzeWildcard() {
        return queryStringAnalyzeWildcard;
    }

    public boolean isQueryStringAllowLeadingWildcard() {
        return queryStringAllowLeadingWildcard;
    }

    public boolean isDefaultAllowUnmappedFields() {
        return defaultAllowUnmappedFields;
    }

    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings) {
        this(indexMetaData, nodeSettings, (index) -> Regex.simpleMatch(index, indexMetaData.getIndex().getName()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings, final Predicate<String> indexNameMatcher, IndexScopedSettings indexScopedSettings) {
        scopedSettings = indexScopedSettings.copy(nodeSettings, indexMetaData);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetaData.getSettings()).build();
        this.index = indexMetaData.getIndex();
        version = Version.indexCreated(settings);
        logger = Loggers.getLogger(getClass(), settings, index);
        nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.indexMetaData = indexMetaData;
        numberOfShards = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);
        isShadowReplicaIndex = IndexMetaData.isIndexUsingShadowReplicas(settings);
        this.defaultField = DEFAULT_FIELD_SETTING.get(settings);
        this.queryStringLenient = QUERY_STRING_LENIENT_SETTING.get(settings);
        this.queryStringAnalyzeWildcard = QUERY_STRING_ANALYZE_WILDCARD.get(nodeSettings);
        this.queryStringAllowLeadingWildcard = QUERY_STRING_ALLOW_LEADING_WILDCARD.get(nodeSettings);
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.defaultAllowUnmappedFields = scopedSettings.get(ALLOW_UNMAPPED);
        this.indexNameMatcher = indexNameMatcher;
        this.durability = scopedSettings.get(INDEX_TRANSLOG_DURABILITY_SETTING);
        syncInterval = INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.get(settings);
        refreshInterval = scopedSettings.get(INDEX_REFRESH_INTERVAL_SETTING);
        flushThresholdSize = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        mergeSchedulerConfig = new MergeSchedulerConfig(this);
        gcDeletesInMillis = scopedSettings.get(INDEX_GC_DELETES_SETTING).getMillis();
        warmerEnabled = scopedSettings.get(INDEX_WARMER_ENABLED_SETTING);
        maxResultWindow = scopedSettings.get(MAX_RESULT_WINDOW_SETTING);
        maxRescoreWindow = scopedSettings.get(MAX_RESCORE_WINDOW_SETTING);
        TTLPurgeDisabled = scopedSettings.get(INDEX_TTL_DISABLE_PURGE_SETTING);
        maxRefreshListeners = scopedSettings.get(MAX_REFRESH_LISTENERS_PER_SHARD);
        maxSlicesPerScroll = scopedSettings.get(MAX_SLICES_PER_SCROLL);
        this.mergePolicyConfig = new MergePolicyConfig(logger, this);
        assert indexNameMatcher.test(indexMetaData.getIndex().getName());
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING, mergePolicyConfig::setNoCFSRatio);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING, mergePolicyConfig::setExpungeDeletesAllowed);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING, mergePolicyConfig::setFloorSegmentSetting);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING, mergePolicyConfig::setMaxMergesAtOnce);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING, mergePolicyConfig::setMaxMergesAtOnceExplicit);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING, mergePolicyConfig::setMaxMergedSegment);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING, mergePolicyConfig::setSegmentsPerTier);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING, mergePolicyConfig::setReclaimDeletesWeight);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING, MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING, mergeSchedulerConfig::setMaxThreadAndMergeCount);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.AUTO_THROTTLE_SETTING, mergeSchedulerConfig::setAutoThrottle);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_DURABILITY_SETTING, this::setTranslogDurability);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TTL_DISABLE_PURGE_SETTING, this::setTTLPurgeDisabled);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESULT_WINDOW_SETTING, this::setMaxResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESCORE_WINDOW_SETTING, this::setMaxRescoreWindow);
        scopedSettings.addSettingsUpdateConsumer(INDEX_WARMER_ENABLED_SETTING, this::setEnableWarmer);
        scopedSettings.addSettingsUpdateConsumer(INDEX_GC_DELETES_SETTING, this::setGCDeletes);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_REFRESH_LISTENERS_PER_SHARD, this::setMaxRefreshListeners);
        scopedSettings.addSettingsUpdateConsumer(MAX_SLICES_PER_SCROLL, this::setMaxSlicesPerScroll);
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushThresholdSize = byteSizeValue;
    }

    private void setGCDeletes(TimeValue timeValue) {
        this.gcDeletesInMillis = timeValue.getMillis();
    }

    private void setRefreshInterval(TimeValue timeValue) {
        this.refreshInterval = timeValue;
    }

    public Settings getSettings() {
        return settings;
    }

    public Index getIndex() {
        return index;
    }

    public String getUUID() {
        return getIndex().getUUID();
    }

    public boolean hasCustomDataPath() {
        return customDataPath() != null;
    }

    public String customDataPath() {
        return settings.get(IndexMetaData.SETTING_DATA_PATH);
    }

    public boolean isOnSharedFilesystem() {
        return IndexMetaData.isOnSharedFilesystem(getSettings());
    }

    public boolean isIndexUsingShadowReplicas() {
        return IndexMetaData.isOnSharedFilesystem(getSettings());
    }

    public Version getIndexVersionCreated() {
        return version;
    }

    public String getNodeName() {
        return nodeName;
    }

    public IndexMetaData getIndexMetaData() {
        return indexMetaData;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, null);
    }

    public boolean isShadowReplicaIndex() {
        return isShadowReplicaIndex;
    }

    public Settings getNodeSettings() {
        return nodeSettings;
    }

    public ParseFieldMatcher getParseFieldMatcher() {
        return parseFieldMatcher;
    }

    public boolean matchesIndexName(String expression) {
        return indexNameMatcher.test(expression);
    }

    public synchronized boolean updateIndexMetaData(IndexMetaData indexMetaData) {
        final Settings newSettings = indexMetaData.getSettings();
        if (version.equals(Version.indexCreated(newSettings)) == false) {
            throw new IllegalArgumentException("version mismatch on settings update expected: " + version + " but was: " + Version.indexCreated(newSettings));
        }
        final String newUUID = newSettings.get(IndexMetaData.SETTING_INDEX_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        if (newUUID.equals(getUUID()) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + getUUID() + " but was: " + newUUID);
        }
        this.indexMetaData = indexMetaData;
        final Settings existingSettings = this.settings;
        if (existingSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE).getAsMap().equals(newSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE).getAsMap())) {
            return false;
        }
        scopedSettings.applySettings(newSettings);
        this.settings = Settings.builder().put(nodeSettings).put(newSettings).build();
        return true;
    }

    public Translog.Durability getTranslogDurability() {
        return durability;
    }

    private void setTranslogDurability(Translog.Durability durability) {
        this.durability = durability;
    }

    public boolean isWarmerEnabled() {
        return warmerEnabled;
    }

    private void setEnableWarmer(boolean enableWarmer) {
        this.warmerEnabled = enableWarmer;
    }

    public TimeValue getTranslogSyncInterval() {
        return syncInterval;
    }

    public TimeValue getRefreshInterval() {
        return refreshInterval;
    }

    public ByteSizeValue getFlushThresholdSize() {
        return flushThresholdSize;
    }

    public MergeSchedulerConfig getMergeSchedulerConfig() {
        return mergeSchedulerConfig;
    }

    public int getMaxResultWindow() {
        return this.maxResultWindow;
    }

    private void setMaxResultWindow(int maxResultWindow) {
        this.maxResultWindow = maxResultWindow;
    }

    public int getMaxRescoreWindow() {
        return maxRescoreWindow;
    }

    private void setMaxRescoreWindow(int maxRescoreWindow) {
        this.maxRescoreWindow = maxRescoreWindow;
    }

    public long getGcDeletesInMillis() {
        return gcDeletesInMillis;
    }

    public MergePolicy getMergePolicy() {
        return mergePolicyConfig.getMergePolicy();
    }

    public boolean isTTLPurgeDisabled() {
        return TTLPurgeDisabled;
    }

    private void setTTLPurgeDisabled(boolean ttlPurgeDisabled) {
        this.TTLPurgeDisabled = ttlPurgeDisabled;
    }

    public <T> T getValue(Setting<T> setting) {
        return scopedSettings.get(setting);
    }

    public int getMaxRefreshListeners() {
        return maxRefreshListeners;
    }

    private void setMaxRefreshListeners(int maxRefreshListeners) {
        this.maxRefreshListeners = maxRefreshListeners;
    }

    public int getMaxSlicesPerScroll() {
        return maxSlicesPerScroll;
    }

    private void setMaxSlicesPerScroll(int value) {
        this.maxSlicesPerScroll = value;
    }

    public IndexScopedSettings getScopedSettings() {
        return scopedSettings;
    }
}