package org.elasticsearch.xpack.core;

public final class XPackField {

    public static final String SECURITY = "security";

    public static final String MONITORING = "monitoring";

    public static final String WATCHER = "watcher";

    public static final String GRAPH = "graph";

    public static final String MACHINE_LEARNING = "ml";

    public static final String LOGSTASH = "logstash";

    public static final String DEPRECATION = "deprecation";

    public static final String UPGRADE = "upgrade";

    public static final String SETTINGS_NAME = "xpack";

    public static final String SQL = "sql";

    public static final String ROLLUP = "rollup";

    private XPackField() {
    }

    public static String featureSettingPrefix(String featureName) {
        return XPackField.SETTINGS_NAME + "." + featureName;
    }
}