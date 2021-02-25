package org.apache.dubbo.common;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassHelper;
import org.apache.dubbo.common.utils.StringUtils;
import java.net.URL;
import java.security.CodeSource;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class Version {

    private static final Logger logger = LoggerFactory.getLogger(Version.class);

    public static final String DEFAULT_DUBBO_PROTOCOL_VERSION = "2.0.2";

    private static final String VERSION = getVersion(Version.class, "");

    private static final int LOWEST_VERSION_FOR_RESPONSE_ATTACHMENT = 2000200;

    private static final Map<String, Integer> VERSION2INT = new HashMap<String, Integer>();

    static {
        Version.checkDuplicate(Version.class);
    }

    private Version() {
    }

    public static String getProtocolVersion() {
        return DEFAULT_DUBBO_PROTOCOL_VERSION;
    }

    public static String getVersion() {
        return VERSION;
    }

    public static boolean isRelease270OrHigher(String version) {
        if (StringUtils.isEmpty(version)) {
            return false;
        }
        if (getIntVersion(version) >= 2070000) {
            return true;
        }
        return false;
    }

    public static boolean isRelease263OrHigher(String version) {
        return isSupportResponseAttachment(version);
    }

    public static boolean isSupportResponseAttachment(String version) {
        if (version == null || version.length() == 0) {
            return false;
        }
        int iVersion = getIntVersion(version);
        if (iVersion >= 2001000 && iVersion <= 2060200) {
            return false;
        }
        return iVersion >= LOWEST_VERSION_FOR_RESPONSE_ATTACHMENT;
    }

    public static int getIntVersion(String version) {
        Integer v = VERSION2INT.get(version);
        if (v == null) {
            v = parseInt(version);
            if (version.split("\\.").length == 3) {
                v = v * 100;
            }
            VERSION2INT.put(version, v);
        }
        return v;
    }

    private static int parseInt(String version) {
        int v = 0;
        String[] vArr = version.split("\\.");
        int len = vArr.length;
        for (int i = 0; i < len; i++) {
            v += Integer.parseInt(getDigital(vArr[i])) * Math.pow(10, (len - i - 1) * 2);
        }
        return v;
    }

    private static String getDigital(String v) {
        int index = 0;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (Character.isDigit(c)) {
                if (i == v.length() - 1) {
                    index = i + 1;
                } else {
                    index = i;
                }
            } else {
                index = i;
                break;
            }
        }
        return v.substring(0, index);
    }

    private static boolean hasResource(String path) {
        try {
            return Version.class.getClassLoader().getResource(path) != null;
        } catch (Throwable t) {
            return false;
        }
    }

    public static String getVersion(Class<?> cls, String defaultVersion) {
        try {
            String version = cls.getPackage().getImplementationVersion();
            if (version == null || version.length() == 0) {
                version = cls.getPackage().getSpecificationVersion();
            }
            if (version == null || version.length() == 0) {
                CodeSource codeSource = cls.getProtectionDomain().getCodeSource();
                if (codeSource == null) {
                    logger.info("No codeSource for class " + cls.getName() + " when getVersion, use default version " + defaultVersion);
                } else {
                    String file = codeSource.getLocation().getFile();
                    if (file != null && file.length() > 0 && file.endsWith(".jar")) {
                        file = file.substring(0, file.length() - 4);
                        int i = file.lastIndexOf('/');
                        if (i >= 0) {
                            file = file.substring(i + 1);
                        }
                        i = file.indexOf("-");
                        if (i >= 0) {
                            file = file.substring(i + 1);
                        }
                        while (file.length() > 0 && !Character.isDigit(file.charAt(0))) {
                            i = file.indexOf("-");
                            if (i >= 0) {
                                file = file.substring(i + 1);
                            } else {
                                break;
                            }
                        }
                        version = file;
                    }
                }
            }
            return version == null || version.length() == 0 ? defaultVersion : version;
        } catch (Throwable e) {
            logger.error("return default version, ignore exception " + e.getMessage(), e);
            return defaultVersion;
        }
    }

    public static void checkDuplicate(Class<?> cls, boolean failOnError) {
        checkDuplicate(cls.getName().replace('.', '/') + ".class", failOnError);
    }

    public static void checkDuplicate(Class<?> cls) {
        checkDuplicate(cls, false);
    }

    public static void checkDuplicate(String path, boolean failOnError) {
        try {
            Enumeration<URL> urls = ClassHelper.getCallerClassLoader(Version.class).getResources(path);
            Set<String> files = new HashSet<String>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String file = url.getFile();
                    if (file != null && file.length() > 0) {
                        files.add(file);
                    }
                }
            }
            if (files.size() > 1) {
                String error = "Duplicate class " + path + " in " + files.size() + " jar " + files;
                if (failOnError) {
                    throw new IllegalStateException(error);
                } else {
                    logger.error(error);
                }
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }
}