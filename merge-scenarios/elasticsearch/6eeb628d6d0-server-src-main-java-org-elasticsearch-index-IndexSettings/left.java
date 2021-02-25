package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.Node;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public final class IndexSettings {

    public static final Setting<List<String>> DEFAULT_FIELD_SETTING = Setting.listSetting("index.query.default_field", Collections.singletonList("*"), Function.identity(), Property.IndexScope, Property.Dynamic);

    public static final Setting<Boolean> QUERY_STRING_LENIENT_SETTING = Setting.boolSetting("index.query_string.lenient", false, Property.IndexScope);

    public static final Setting<Boolean> QUERY_STRING_ANALYZE_WILDCARD = Setting.boolSetting("indices.query.query_string.analyze_wildcard", false, Property.NodeScope);

    public static final Setting<Boolean> QUERY_STRING_ALLOW_LEADING_WILDCARD = Setting.boolSetting("indices.query.query_string.allowLeadingWildcard", true, Property.NodeScope);

    public static final Setting<Boolean> ALLOW_UNMAPPED = Setting.boolSetting("index.query.parse.allow_unmapped_fields", true, Property.IndexScope);

    public static final Setting<TimeValue> INDEX_TRANSLOG_SYNC_INTERVAL_SETTING = Setting.timeSetting("index.translog.sync_interval", TimeValue.timeValueSeconds(5), TimeValue.timeValueMillis(100), Property.IndexScope);

    public static final Setting<TimeValue> INDEX_SEARCH_IDLE_AFTER = Setting.timeSetting("index.search.idle.after", TimeValue.timeValueSeconds(30), TimeValue.timeValueMinutes(0), Property.IndexScope, Property.Dynamic);

    public static final Setting<Translog.Durability> INDEX_TRANSLOG_DURABILITY_SETTING = new Setting<>("index.translog.durability", Translog.Durability.REQUEST.name(), (value) -> Translog.Durability.valueOf(value.toUpperCase(Locale.ROOT)), Property.Dynamic, Property.IndexScope);

    public static final Setting<Boolean> INDEX_WARMER_ENABLED_SETTING = Setting.boolSetting("index.warmer.enabled", true, Property.Dynamic, Property.IndexScope);

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

    public static final Setting<Integer> MAX_INNER_RESULT_WINDOW_SETTING = Setting.intSetting("index.max_inner_result_window", 100, 1, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_SCRIPT_FIELDS_SETTING = Setting.intSetting("index.max_script_fields", 32, 0, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_TOKEN_COUNT_SETTING = Setting.intSetting("index.analyze.max_token_count", 10000, 1, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_ANALYZED_OFFSET_SETTING = Setting.intSetting("index.highlight.max_analyzed_offset", 1000000, 1, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_TERMS_COUNT_SETTING = Setting.intSetting("index.max_terms_count", 65536, 1, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_NGRAM_DIFF_SETTING = Setting.intSetting("index.max_ngram_diff", 1, 0, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_SHINGLE_DIFF_SETTING = Setting.intSetting("index.max_shingle_diff", 3, 0, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_DOCVALUE_FIELDS_SEARCH_SETTING = Setting.intSetting("index.max_docvalue_fields_search", 100, 0, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_RESCORE_WINDOW_SETTING = Setting.intSetting("index.max_rescore_window", MAX_RESULT_WINDOW_SETTING, 1, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_ADJACENCY_MATRIX_FILTERS_SETTING = Setting.intSetting("index.max_adjacency_matrix_filters", 100, 2, Property.Dynamic, Property.IndexScope);

    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);

    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING = Setting.timeSetting("index.refresh_interval", DEFAULT_REFRESH_INTERVAL, new TimeValue(-1, TimeUnit.MILLISECONDS), Property.Dynamic, Property.IndexScope);

    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting("index.translog.flush_threshold_size", new ByteSizeValue(512, ByteSizeUnit.MB), new ByteSizeValue(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1, ByteSizeUnit.BYTES), new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES), Property.Dynamic, Property.IndexScope);

    public static final Setting<TimeValue> INDEX_TRANSLOG_RETENTION_AGE_SETTING = Setting.timeSetting("index.translog.retention.age", TimeValue.timeValueHours(12), TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);

    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_RETENTION_SIZE_SETTING = Setting.byteSizeSetting("index.translog.retention.size", new ByteSizeValue(512, ByteSizeUnit.MB), Property.Dynamic, Property.IndexScope);

    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting("index.translog.generation_threshold_size", new ByteSizeValue(64, ByteSizeUnit.MB), new ByteSizeValue(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1, ByteSizeUnit.BYTES), new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES), Property.Dynamic, Property.IndexScope);

    public static final TimeValue DEFAULT_GC_DELETES = TimeValue.timeValueSeconds(60);

    public static final Setting<TimeValue> INDEX_GC_DELETES_SETTING = Setting.timeSetting("index.gc_deletes", DEFAULT_GC_DELETES, new TimeValue(-1, TimeUnit.MILLISECONDS), Property.Dynamic, Property.IndexScope);

    public static final Setting<Boolean> INDEX_SOFT_DELETES_SETTING = Setting.boolSetting("index.soft_deletes.enabled", true, Property.IndexScope);

    public static final Setting<Long> INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING = Setting.longSetting("index.soft_deletes.retention.operations", 0, 0, Property.IndexScope, Property.Dynamic);

    public static final Setting<Integer> MAX_REFRESH_LISTENERS_PER_SHARD = Setting.intSetting("index.max_refresh_listeners", 1000, 0, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_SLICES_PER_SCROLL = Setting.intSetting("index.max_slices_per_scroll", 1024, 1, Property.Dynamic, Property.IndexScope);

    public static final Setting<Integer> MAX_REGEX_LENGTH_SETTING = Setting.intSetting("index.max_regex_length", 1000, 1, Property.Dynamic, Property.IndexScope);

    private final Index index;

    private final Version version;

    private final Logger logger;

    private final String nodeName;

    private final Settings nodeSettings;

    private final int numberOfShards;

    private volatile Settings settings;

    private volatile IndexMetaData indexMetaData;

    private volatile List<String> defaultFields;

    private final boolean queryStringLenient;

    private final boolean queryStringAnalyzeWildcard;

    private final boolean queryStringAllowLeadingWildcard;

    private final boolean defaultAllowUnmappedFields;

    private volatile Translog.Durability durability;

    private final TimeValue syncInterval;

    private volatile TimeValue refreshInterval;

    private volatile ByteSizeValue flushThresholdSize;

    private volatile TimeValue translogRetentionAge;

    private volatile ByteSizeValue translogRetentionSize;

    private volatile ByteSizeValue generationThresholdSize;

    private final MergeSchedulerConfig mergeSchedulerConfig;

    private final MergePolicyConfig mergePolicyConfig;

    private final IndexSortConfig indexSortConfig;

    private final IndexScopedSettings scopedSettings;

    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();

    private final boolean softDeleteEnabled;

    private volatile long softDeleteRetentionOperations;

    private volatile boolean warmerEnabled;

    private volatile int maxResultWindow;

    private volatile int maxInnerResultWindow;

    private volatile int maxAdjacencyMatrixFilters;

    private volatile int maxRescoreWindow;

    private volatile int maxDocvalueFields;

    private volatile int maxScriptFields;

    private volatile int maxTokenCount;

    private volatile int maxNgramDiff;

    private volatile int maxShingleDiff;

    private volatile TimeValue searchIdleAfter;

    private volatile int maxAnalyzedOffset;

    private volatile int maxTermsCount;

    private volatile int maxRefreshListeners;

    private volatile int maxSlicesPerScroll;

    private volatile int maxRegexLength;

    public List<String> getDefaultFields() {
        return defaultFields;
    }

    private void setDefaultFields(List<String> defaultFields) {
        this.defaultFields = defaultFields;
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
        this(indexMetaData, nodeSettings, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings, IndexScopedSettings indexScopedSettings) {
        scopedSettings = indexScopedSettings.copy(nodeSettings, indexMetaData);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetaData.getSettings()).build();
        this.index = indexMetaData.getIndex();
        version = Version.indexCreated(settings);
        logger = Loggers.getLogger(getClass(), settings, index);
        nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.indexMetaData = indexMetaData;
        numberOfShards = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);
        this.queryStringLenient = QUERY_STRING_LENIENT_SETTING.get(settings);
        this.queryStringAnalyzeWildcard = QUERY_STRING_ANALYZE_WILDCARD.get(nodeSettings);
        this.queryStringAllowLeadingWildcard = QUERY_STRING_ALLOW_LEADING_WILDCARD.get(nodeSettings);
        this.defaultAllowUnmappedFields = scopedSettings.get(ALLOW_UNMAPPED);
        this.durability = scopedSettings.get(INDEX_TRANSLOG_DURABILITY_SETTING);
        defaultFields = scopedSettings.get(DEFAULT_FIELD_SETTING);
        syncInterval = INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.get(settings);
        refreshInterval = scopedSettings.get(INDEX_REFRESH_INTERVAL_SETTING);
        flushThresholdSize = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        translogRetentionAge = scopedSettings.get(INDEX_TRANSLOG_RETENTION_AGE_SETTING);
        translogRetentionSize = scopedSettings.get(INDEX_TRANSLOG_RETENTION_SIZE_SETTING);
        generationThresholdSize = scopedSettings.get(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING);
        mergeSchedulerConfig = new MergeSchedulerConfig(this);
        gcDeletesInMillis = scopedSettings.get(INDEX_GC_DELETES_SETTING).getMillis();
        softDeleteEnabled = version.onOrAfter(Version.V_6_5_0) && scopedSettings.get(INDEX_SOFT_DELETES_SETTING);
        softDeleteRetentionOperations = scopedSettings.get(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING);
        warmerEnabled = scopedSettings.get(INDEX_WARMER_ENABLED_SETTING);
        maxResultWindow = scopedSettings.get(MAX_RESULT_WINDOW_SETTING);
        maxInnerResultWindow = scopedSettings.get(MAX_INNER_RESULT_WINDOW_SETTING);
        maxAdjacencyMatrixFilters = scopedSettings.get(MAX_ADJACENCY_MATRIX_FILTERS_SETTING);
        maxRescoreWindow = scopedSettings.get(MAX_RESCORE_WINDOW_SETTING);
        maxDocvalueFields = scopedSettings.get(MAX_DOCVALUE_FIELDS_SEARCH_SETTING);
        maxScriptFields = scopedSettings.get(MAX_SCRIPT_FIELDS_SETTING);
        maxTokenCount = scopedSettings.get(MAX_TOKEN_COUNT_SETTING);
        maxNgramDiff = scopedSettings.get(MAX_NGRAM_DIFF_SETTING);
        maxShingleDiff = scopedSettings.get(MAX_SHINGLE_DIFF_SETTING);
        maxRefreshListeners = scopedSettings.get(MAX_REFRESH_LISTENERS_PER_SHARD);
        maxSlicesPerScroll = scopedSettings.get(MAX_SLICES_PER_SCROLL);
        maxAnalyzedOffset = scopedSettings.get(MAX_ANALYZED_OFFSET_SETTING);
        maxTermsCount = scopedSettings.get(MAX_TERMS_COUNT_SETTING);
        maxRegexLength = scopedSettings.get(MAX_REGEX_LENGTH_SETTING);
        this.mergePolicyConfig = new MergePolicyConfig(logger, this);
        this.indexSortConfig = new IndexSortConfig(this);
        searchIdleAfter = scopedSettings.get(INDEX_SEARCH_IDLE_AFTER);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING, mergePolicyConfig::setNoCFSRatio);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING, mergePolicyConfig::setExpungeDeletesAllowed);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING, mergePolicyConfig::setFloorSegmentSetting);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING, mergePolicyConfig::setMaxMergesAtOnce);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING, mergePolicyConfig::setMaxMergesAtOnceExplicit);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING, mergePolicyConfig::setMaxMergedSegment);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING, mergePolicyConfig::setSegmentsPerTier);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING, MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING, mergeSchedulerConfig::setMaxThreadAndMergeCount);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.AUTO_THROTTLE_SETTING, mergeSchedulerConfig::setAutoThrottle);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_DURABILITY_SETTING, this::setTranslogDurability);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESULT_WINDOW_SETTING, this::setMaxResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_INNER_RESULT_WINDOW_SETTING, this::setMaxInnerResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_ADJACENCY_MATRIX_FILTERS_SETTING, this::setMaxAdjacencyMatrixFilters);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESCORE_WINDOW_SETTING, this::setMaxRescoreWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_DOCVALUE_FIELDS_SEARCH_SETTING, this::setMaxDocvalueFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_SCRIPT_FIELDS_SETTING, this::setMaxScriptFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_TOKEN_COUNT_SETTING, this::setMaxTokenCount);
        scopedSettings.addSettingsUpdateConsumer(MAX_NGRAM_DIFF_SETTING, this::setMaxNgramDiff);
        scopedSettings.addSettingsUpdateConsumer(MAX_SHINGLE_DIFF_SETTING, this::setMaxShingleDiff);
        scopedSettings.addSettingsUpdateConsumer(INDEX_WARMER_ENABLED_SETTING, this::setEnableWarmer);
        scopedSettings.addSettingsUpdateConsumer(INDEX_GC_DELETES_SETTING, this::setGCDeletes);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING, this::setGenerationThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_AGE_SETTING, this::setTranslogRetentionAge);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_SIZE_SETTING, this::setTranslogRetentionSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_REFRESH_LISTENERS_PER_SHARD, this::setMaxRefreshListeners);
        scopedSettings.addSettingsUpdateConsumer(MAX_ANALYZED_OFFSET_SETTING, this::setHighlightMaxAnalyzedOffset);
        scopedSettings.addSettingsUpdateConsumer(MAX_TERMS_COUNT_SETTING, this::setMaxTermsCount);
        scopedSettings.addSettingsUpdateConsumer(MAX_SLICES_PER_SCROLL, this::setMaxSlicesPerScroll);
        scopedSettings.addSettingsUpdateConsumer(DEFAULT_FIELD_SETTING, this::setDefaultFields);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SEARCH_IDLE_AFTER, this::setSearchIdleAfter);
        scopedSettings.addSettingsUpdateConsumer(MAX_REGEX_LENGTH_SETTING, this::setMaxRegexLength);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING, this::setSoftDeleteRetentionOperations);
    }

    private void setSearchIdleAfter(TimeValue searchIdleAfter) {
        this.searchIdleAfter = searchIdleAfter;
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushThresholdSize = byteSizeValue;
    }

    private void setTranslogRetentionSize(ByteSizeValue byteSizeValue) {
        this.translogRetentionSize = byteSizeValue;
    }

    private void setTranslogRetentionAge(TimeValue age) {
        this.translogRetentionAge = age;
    }

    private void setGenerationThresholdSize(final ByteSizeValue generationThresholdSize) {
        this.generationThresholdSize = generationThresholdSize;
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

    public Settings getNodeSettings() {
        return nodeSettings;
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
        if (existingSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE).equals(newSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE))) {
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

    public ByteSizeValue getTranslogRetentionSize() {
        return translogRetentionSize;
    }

    public TimeValue getTranslogRetentionAge() {
        return translogRetentionAge;
    }

    public ByteSizeValue getGenerationThresholdSize() {
        return generationThresholdSize;
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

    public int getMaxInnerResultWindow() {
        return maxInnerResultWindow;
    }

    private void setMaxInnerResultWindow(int maxInnerResultWindow) {
        this.maxInnerResultWindow = maxInnerResultWindow;
    }

    public int getMaxAdjacencyMatrixFilters() {
        return this.maxAdjacencyMatrixFilters;
    }

    private void setMaxAdjacencyMatrixFilters(int maxAdjacencyFilters) {
        this.maxAdjacencyMatrixFilters = maxAdjacencyFilters;
    }

    public int getMaxRescoreWindow() {
        return maxRescoreWindow;
    }

    private void setMaxRescoreWindow(int maxRescoreWindow) {
        this.maxRescoreWindow = maxRescoreWindow;
    }

    public int getMaxDocvalueFields() {
        return this.maxDocvalueFields;
    }

    private void setMaxDocvalueFields(int maxDocvalueFields) {
        this.maxDocvalueFields = maxDocvalueFields;
    }

    public int getMaxTokenCount() {
        return maxTokenCount;
    }

    private void setMaxTokenCount(int maxTokenCount) {
        this.maxTokenCount = maxTokenCount;
    }

    public int getMaxNgramDiff() {
        return this.maxNgramDiff;
    }

    private void setMaxNgramDiff(int maxNgramDiff) {
        this.maxNgramDiff = maxNgramDiff;
    }

    public int getMaxShingleDiff() {
        return this.maxShingleDiff;
    }

    private void setMaxShingleDiff(int maxShingleDiff) {
        this.maxShingleDiff = maxShingleDiff;
    }

    public int getHighlightMaxAnalyzedOffset() {
        return this.maxAnalyzedOffset;
    }

    private void setHighlightMaxAnalyzedOffset(int maxAnalyzedOffset) {
        this.maxAnalyzedOffset = maxAnalyzedOffset;
    }

    public int getMaxTermsCount() {
        return this.maxTermsCount;
    }

    private void setMaxTermsCount(int maxTermsCount) {
        this.maxTermsCount = maxTermsCount;
    }

    public int getMaxScriptFields() {
        return this.maxScriptFields;
    }

    private void setMaxScriptFields(int maxScriptFields) {
        this.maxScriptFields = maxScriptFields;
    }

    public long getGcDeletesInMillis() {
        return gcDeletesInMillis;
    }

    public MergePolicy getMergePolicy() {
        return mergePolicyConfig.getMergePolicy();
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

    public int getMaxRegexLength() {
        return maxRegexLength;
    }

    private void setMaxRegexLength(int maxRegexLength) {
        this.maxRegexLength = maxRegexLength;
    }

    public IndexSortConfig getIndexSortConfig() {
        return indexSortConfig;
    }

    public IndexScopedSettings getScopedSettings() {
        return scopedSettings;
    }

    public boolean isExplicitRefresh() {
        return INDEX_REFRESH_INTERVAL_SETTING.exists(settings);
    }

    public TimeValue getSearchIdleAfter() {
        return searchIdleAfter;
    }

    public boolean isSoftDeleteEnabled() {
        return softDeleteEnabled;
    }

    private void setSoftDeleteRetentionOperations(long ops) {
        this.softDeleteRetentionOperations = ops;
    }

    public long getSoftDeleteRetentionOperations() {
        return this.softDeleteRetentionOperations;
    }
}