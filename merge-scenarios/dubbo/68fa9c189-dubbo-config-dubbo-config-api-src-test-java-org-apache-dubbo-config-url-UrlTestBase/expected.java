package org.apache.dubbo.config.url;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.api.DemoService;
import org.apache.dubbo.config.provider.impl.DemoServiceImpl;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("unused")
public class UrlTestBase {

    protected static final int KEY = 0;

    protected static final int URL_KEY = 1;

    protected static final int TESTVALUE1 = 4;

    private static final Logger log = LoggerFactory.getLogger(UrlTestBase.class);

    private static final int TYPE = 2;

    private static final int DEFAULT = 3;

    private static final int TESTVALUE2 = 5;

    private static final int TESTVALUE3 = 6;

    private static final int TESTVALUE4 = 7;

    private static final int TESTVALUE5 = 8;

    private static final int TESTVALUE6 = 9;

    private static final int TESTVALUE7 = 10;

    protected ApplicationConfig application = new ApplicationConfig();

    protected RegistryConfig regConfForProvider;

    protected RegistryConfig regConfForService;

    protected ProviderConfig provConf;

    protected ProtocolConfig protoConfForProvider;

    protected ProtocolConfig protoConfForService;

    protected MethodConfig methodConfForService;

    protected ServiceConfig<DemoService> servConf;

    protected Object[][] servConfTable = { { "proxy", "proxy", "string", "javassist", "jdk", "javassist", "", "", "", "" }, { "actives", "actives", "int", 0, 90, "", "", "", "", "" }, { "executes", "executes", "int", 0, 90, "", "", "", "", "" }, { "deprecated", "deprecated", "boolean", false, true, "", "", "", "", "" }, { "dynamic", "dynamic", "boolean", true, false, "", "", "", "", "" }, { "accesslog", "accesslog", "string", "", "haominTest", "", "", "", "", "" }, { "document", "document", "string", "", "http://b2b-doc.alibaba-inc.com/display/RC/dubbo_devguide.htm?testquery=你好你好", "", "", "", "", "" }, { "weight", "weight", "int", 0, 90, "", "", "", "", "" } };

    protected Object[][] regConfForServiceTable = { { "dynamic", "dynamic", "boolean", true, false, "", "", "", "", "" } };

    protected Object[][] provConfTable = { { "cluster", "default.cluster", "string", "string", "failover", "failfast", "failsafe", "", "", "" }, { "async", "default.async", "boolean", false, true, "", "", "", "", "" }, { "loadbalance", "default.loadbalance", "string", "random", "leastactive", "", "", "", "", "" }, { "connections", "default.connections", "int", 0, 60, "", "", "", "", "" }, { "retries", "default.retries", "int", 2, 60, "", "", "", "", "" }, { "timeout", "default.timeout", "int", 5000, 60, "", "", "", "", "" } };

    protected Object[][] methodConfForServiceTable = { { "actives", "sayName.actives", "int", 0, 90, "", "", "", "", "" }, { "executes", "sayName.executes", "int", 0, 90, "", "", "", "", "" }, { "deprecated", "sayName.deprecated", "boolean", false, true, "", "", "", "", "" }, { "async", "sayName.async", "boolean", false, true, "", "", "", "", "" }, { "timeout", "sayName.timeout", "int", 0, 90, "", "", "", "", "" } };

    protected DemoService demoService = new DemoServiceImpl();

    private Object[][] appConfForProviderTable = { { "", "", "", "", "", "", "", "", "", "" } };

    private Object[][] appConfForServiceTable = { { "", "", "", "", "", "", "", "", "", "" } };

    private Object[][] regConfForProviderTable = { { "", "", "", "", "", "", "", "", "", "" } };

    private Object[][] protoConfForProviderTable = { { "", "", "", "", "", "", "", "", "", "" } };

    private Object[][] protoConfForServiceTable = { { "", "", "", "", "", "", "", "", "", "" } };

    protected String genParamString(Object urlKey, Object value) {
        return (String) urlKey + "=" + value.toString();
    }

    protected <T> void fillConfigs(T conf, Object[][] table, int column) {
        for (Object[] row : table) {
            fillConfig(conf, row, column);
        }
    }

    protected <T> void fillConfig(T conf, Object[] row, int column) {
        RpcConfigGetSetProxy proxy = new RpcConfigGetSetProxy(conf);
        proxy.setValue((String) row[KEY], row[column]);
    }

    @SuppressWarnings("deprecation")
    protected void initServConf() {
        regConfForProvider = new RegistryConfig();
        regConfForService = new RegistryConfig();
        provConf = new ProviderConfig();
        protoConfForProvider = new ProtocolConfig("mockprotocol");
        protoConfForService = new ProtocolConfig("mockprotocol");
        methodConfForService = new MethodConfig();
        servConf = new ServiceConfig<DemoService>();
        servConf.setApplication(application);
        provConf.setRegistry(regConfForProvider);
        servConf.setRegistry(regConfForService);
        provConf.setProtocols(Arrays.asList(new ProtocolConfig[] { protoConfForProvider }));
        servConf.setProtocols(Arrays.asList(new ProtocolConfig[] { protoConfForService }));
        servConf.setMethods(Arrays.asList(new MethodConfig[] { methodConfForService }));
        servConf.setProvider(provConf);
        servConf.setRef(demoService);
        servConf.setInterfaceClass(DemoService.class);
        methodConfForService.setName("sayName");
        regConfForService.setAddress("127.0.0.1:9090");
        regConfForService.setProtocol("mockregistry");
        application.setName("ConfigTests");
    }

    protected String getProviderParamString() {
        return servConf.getExportedUrls().get(0).toString();
    }

    protected void assertUrlStringWithLocalTable(String paramStringFromDb, Object[][] dataTable, String configName, int column) {
        final String FAILLOG_HEADER = "The following config items are not found in URLONE: ";
        log.warn("Verifying service url for " + configName + "... ");
        log.warn("Consumer url string: " + paramStringFromDb);
        String failLog = FAILLOG_HEADER;
        for (Object[] row : dataTable) {
            String targetString = genParamString(row[URL_KEY], row[column]);
            log.warn("Checking " + (String) row[KEY] + "for" + targetString);
            if (paramStringFromDb.contains(targetString)) {
                log.warn((String) row[KEY] + " --> " + targetString + " OK!");
            } else {
                failLog += targetString + ", ";
            }
        }
        if (!failLog.equals(FAILLOG_HEADER)) {
            fail(failLog);
        }
    }
}