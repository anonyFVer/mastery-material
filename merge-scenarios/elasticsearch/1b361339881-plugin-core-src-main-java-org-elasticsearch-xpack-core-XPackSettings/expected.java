package org.elasticsearch.xpack.core;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.elasticsearch.xpack.core.security.SecurityField.USER_SETTING;

public class XPackSettings {

    public static final Setting<Boolean> SECURITY_ENABLED = Setting.boolSetting("xpack.security.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> MONITORING_ENABLED = Setting.boolSetting("xpack.monitoring.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> WATCHER_ENABLED = Setting.boolSetting("xpack.watcher.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> GRAPH_ENABLED = Setting.boolSetting("xpack.graph.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> MACHINE_LEARNING_ENABLED = Setting.boolSetting("xpack.ml.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> AUDIT_ENABLED = Setting.boolSetting("xpack.security.audit.enabled", false, Setting.Property.NodeScope);

    public static final Setting<Boolean> DLS_FLS_ENABLED = Setting.boolSetting("xpack.security.dls_fls.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> LOGSTASH_ENABLED = Setting.boolSetting("xpack.logstash.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> TRANSPORT_SSL_ENABLED = Setting.boolSetting("xpack.security.transport.ssl.enabled", false, Property.NodeScope);

    public static final Setting<Boolean> HTTP_SSL_ENABLED = Setting.boolSetting("xpack.security.http.ssl.enabled", false, Setting.Property.NodeScope);

    public static final Setting<Boolean> RESERVED_REALM_ENABLED_SETTING = Setting.boolSetting("xpack.security.authc.reserved_realm.enabled", true, Setting.Property.NodeScope);

    public static final Setting<Boolean> TOKEN_SERVICE_ENABLED_SETTING = Setting.boolSetting("xpack.security.authc.token.enabled", (s) -> {
        if (NetworkModule.HTTP_ENABLED.get(s)) {
            return XPackSettings.HTTP_SSL_ENABLED.getRaw(s);
        } else {
            return Boolean.TRUE.toString();
        }
    }, Setting.Property.NodeScope);

    public static final Setting<Boolean> SQL_ENABLED = Setting.boolSetting("xpack.sql.enabled", true, Setting.Property.NodeScope);

    public static final List<String> DEFAULT_CIPHERS;

    static {
        List<String> ciphers = Arrays.asList("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA");
        try {
            final boolean use256Bit = Cipher.getMaxAllowedKeyLength("AES") > 128;
            if (use256Bit) {
                List<String> strongerCiphers = new ArrayList<>(ciphers.size() * 2);
                strongerCiphers.addAll(Arrays.asList("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA256", "TLS_RSA_WITH_AES_256_CBC_SHA"));
                strongerCiphers.addAll(ciphers);
                ciphers = strongerCiphers;
            }
        } catch (NoSuchAlgorithmException e) {
        }
        DEFAULT_CIPHERS = ciphers;
    }

    public static final List<String> DEFAULT_SUPPORTED_PROTOCOLS = Arrays.asList("TLSv1.2", "TLSv1.1", "TLSv1");

    public static final SSLClientAuth CLIENT_AUTH_DEFAULT = SSLClientAuth.REQUIRED;

    public static final SSLClientAuth HTTP_CLIENT_AUTH_DEFAULT = SSLClientAuth.NONE;

    public static final VerificationMode VERIFICATION_MODE_DEFAULT = VerificationMode.FULL;

    public static final String GLOBAL_SSL_PREFIX = "xpack.ssl.";

    private static final SSLConfigurationSettings GLOBAL_SSL = SSLConfigurationSettings.withPrefix(GLOBAL_SSL_PREFIX);

    public static final String HTTP_SSL_PREFIX = SecurityField.setting("http.ssl.");

    private static final SSLConfigurationSettings HTTP_SSL = SSLConfigurationSettings.withPrefix(HTTP_SSL_PREFIX);

    public static final String TRANSPORT_SSL_PREFIX = SecurityField.setting("transport.ssl.");

    private static final SSLConfigurationSettings TRANSPORT_SSL = SSLConfigurationSettings.withPrefix(TRANSPORT_SSL_PREFIX);

    public static List<Setting<?>> getAllSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.addAll(GLOBAL_SSL.getAllSettings());
        settings.addAll(HTTP_SSL.getAllSettings());
        settings.addAll(TRANSPORT_SSL.getAllSettings());
        settings.add(SECURITY_ENABLED);
        settings.add(MONITORING_ENABLED);
        settings.add(GRAPH_ENABLED);
        settings.add(MACHINE_LEARNING_ENABLED);
        settings.add(AUDIT_ENABLED);
        settings.add(WATCHER_ENABLED);
        settings.add(DLS_FLS_ENABLED);
        settings.add(LOGSTASH_ENABLED);
        settings.add(TRANSPORT_SSL_ENABLED);
        settings.add(HTTP_SSL_ENABLED);
        settings.add(RESERVED_REALM_ENABLED_SETTING);
        settings.add(TOKEN_SERVICE_ENABLED_SETTING);
        settings.add(SQL_ENABLED);
        settings.add(USER_SETTING);
        return Collections.unmodifiableList(settings);
    }
}