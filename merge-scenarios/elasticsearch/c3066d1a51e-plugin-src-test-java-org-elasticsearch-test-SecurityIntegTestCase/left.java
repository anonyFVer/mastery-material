package org.elasticsearch.test;

import org.elasticsearch.AbstractOldXPackIndicesBackwardsCompatibilityTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.securityIndexMappingAndTemplateSufficientToRead;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.securityIndexMappingAndTemplateUpToDate;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

public abstract class SecurityIntegTestCase extends ESIntegTestCase {

    private static SecuritySettingsSource SECURITY_DEFAULT_SETTINGS;

    protected static SecureString BOOTSTRAP_PASSWORD = null;

    private static CustomSecuritySettingsSource customSecuritySettingsSource = null;

    @BeforeClass
    public static void generateBootstrapPassword() {
        BOOTSTRAP_PASSWORD = new SecureString("FOOBAR".toCharArray());
    }

    protected static int defaultMaxNumberOfNodes() {
        ClusterScope clusterScope = SecurityIntegTestCase.class.getAnnotation(ClusterScope.class);
        if (clusterScope == null) {
            return InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES + InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES + InternalTestCluster.DEFAULT_MAX_NUM_CLIENT_NODES;
        } else {
            int clientNodes = clusterScope.numClientNodes();
            if (clientNodes < 0) {
                clientNodes = InternalTestCluster.DEFAULT_MAX_NUM_CLIENT_NODES;
            }
            int masterNodes = 0;
            if (clusterScope.supportsDedicatedMasters()) {
                masterNodes = InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES;
            }
            int dataNodes = 0;
            if (clusterScope.numDataNodes() < 0) {
                if (clusterScope.maxNumDataNodes() < 0) {
                    dataNodes = InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES;
                } else {
                    dataNodes = clusterScope.maxNumDataNodes();
                }
            } else {
                dataNodes = clusterScope.numDataNodes();
            }
            return masterNodes + dataNodes + clientNodes;
        }
    }

    private static ClusterScope getAnnotation(Class<?> clazz) {
        if (clazz == Object.class || clazz == SecurityIntegTestCase.class) {
            return null;
        }
        ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }

    Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz);
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    @BeforeClass
    public static void initDefaultSettings() {
        if (SECURITY_DEFAULT_SETTINGS == null) {
            SECURITY_DEFAULT_SETTINGS = new SecuritySettingsSource(defaultMaxNumberOfNodes(), randomBoolean(), createTempDir(), Scope.SUITE);
        }
    }

    @AfterClass
    public static void destroyDefaultSettings() {
        SECURITY_DEFAULT_SETTINGS = null;
        customSecuritySettingsSource = null;
    }

    @Rule
    public ExternalResource externalResource = new ExternalResource() {

        @Override
        protected void before() throws Throwable {
            Scope currentClusterScope = getCurrentClusterScope();
            switch(currentClusterScope) {
                case SUITE:
                    if (customSecuritySettingsSource == null) {
                        customSecuritySettingsSource = new CustomSecuritySettingsSource(transportSSLEnabled(), createTempDir(), currentClusterScope);
                    }
                    break;
                case TEST:
                    customSecuritySettingsSource = new CustomSecuritySettingsSource(transportSSLEnabled(), createTempDir(), currentClusterScope);
                    break;
            }
        }
    };

    @Before
    public void assertXPackIsInstalled() {
        if (false == shouldAssertXPackIsInstalled()) {
            return;
        }
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            Collection<String> pluginNames = nodeInfo.getPlugins().getPluginInfos().stream().map(p -> p.getClassname()).collect(Collectors.toList());
            assertThat("plugin [" + xpackPluginClass().getName() + "] not found in [" + pluginNames + "]", pluginNames, hasItem(xpackPluginClass().getName()));
        }
    }

    protected boolean shouldAssertXPackIsInstalled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(MachineLearning.AUTODETECT_PROCESS.getKey(), false);
        Settings customSettings = customSecuritySettingsSource.nodeSettings(nodeOrdinal);
        builder.put(customSettings.getAsMap());
        Settings.Builder customBuilder = Settings.builder().put(customSettings);
        if (customBuilder.getSecureSettings() != null) {
            SecuritySettingsSource.addSecureSettings(builder, secureSettings -> secureSettings.merge((MockSecureSettings) customBuilder.getSecureSettings()));
        }
        if (builder.getSecureSettings() == null) {
            builder.setSecureSettings(new MockSecureSettings());
        }
        ((MockSecureSettings) builder.getSecureSettings()).setString("bootstrap.password", BOOTSTRAP_PASSWORD.toString());
        return builder.build();
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return customSecuritySettingsSource.nodeConfigPath(nodeOrdinal);
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder().put(super.transportClientSettings()).put(customSecuritySettingsSource.transportClientSettings()).build();
    }

    @Override
    protected boolean addMockTransportService() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return customSecuritySettingsSource.nodePlugins();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return customSecuritySettingsSource.transportClientPlugins();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder().put(Security.USER_SETTING.getKey(), SecuritySettingsSource.TEST_USER_NAME + ":" + SecuritySettingsSource.TEST_PASSWORD).build();
    }

    protected String configUsers() {
        return SECURITY_DEFAULT_SETTINGS.configUsers();
    }

    protected String configUsersRoles() {
        return SECURITY_DEFAULT_SETTINGS.configUsersRoles();
    }

    protected String configRoles() {
        return SECURITY_DEFAULT_SETTINGS.configRoles();
    }

    protected String nodeClientUsername() {
        return SECURITY_DEFAULT_SETTINGS.nodeClientUsername();
    }

    protected SecureString nodeClientPassword() {
        return SECURITY_DEFAULT_SETTINGS.nodeClientPassword();
    }

    protected String transportClientUsername() {
        return SECURITY_DEFAULT_SETTINGS.transportClientUsername();
    }

    protected SecureString transportClientPassword() {
        return SECURITY_DEFAULT_SETTINGS.transportClientPassword();
    }

    protected boolean transportSSLEnabled() {
        return randomBoolean();
    }

    protected int maxNumberOfNodes() {
        return defaultMaxNumberOfNodes();
    }

    protected Class<? extends XPackPlugin> xpackPluginClass() {
        return SECURITY_DEFAULT_SETTINGS.xpackPluginClass();
    }

    private class CustomSecuritySettingsSource extends SecuritySettingsSource {

        private CustomSecuritySettingsSource(boolean sslEnabled, Path configDir, Scope scope) {
            super(maxNumberOfNodes(), sslEnabled, configDir, scope);
        }

        @Override
        protected String configUsers() {
            return SecurityIntegTestCase.this.configUsers();
        }

        @Override
        protected String configUsersRoles() {
            return SecurityIntegTestCase.this.configUsersRoles();
        }

        @Override
        protected String configRoles() {
            return SecurityIntegTestCase.this.configRoles();
        }

        @Override
        protected String nodeClientUsername() {
            return SecurityIntegTestCase.this.nodeClientUsername();
        }

        @Override
        protected SecureString nodeClientPassword() {
            return SecurityIntegTestCase.this.nodeClientPassword();
        }

        @Override
        protected String transportClientUsername() {
            return SecurityIntegTestCase.this.transportClientUsername();
        }

        @Override
        protected SecureString transportClientPassword() {
            return SecurityIntegTestCase.this.transportClientPassword();
        }

        @Override
        protected Class<? extends XPackPlugin> xpackPluginClass() {
            return SecurityIntegTestCase.this.xpackPluginClass();
        }
    }

    protected static void assertGreenClusterState(Client client) {
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
        assertNoTimeout(clusterHealthResponse);
        assertThat(clusterHealthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
    }

    protected void createIndicesWithRandomAliases(String... indices) {
        createIndex(indices);
        if (frequently()) {
            IndicesAliasesRequestBuilder builder = client().admin().indices().prepareAliases();
            for (String index : indices) {
                if (frequently()) {
                    builder.addAlias(index, "alias-" + index);
                }
            }
            if (randomBoolean()) {
                for (String index : indices) {
                    builder.addAlias(index, "alias");
                }
            }
            assertAcked(builder);
        }
        for (String index : indices) {
            client().prepareIndex(index, "type").setSource("field", "value").get();
        }
        refresh(indices);
    }

    @Override
    protected Function<Client, Client> getClientWrapper() {
        Map<String, String> headers = Collections.singletonMap("Authorization", basicAuthHeaderValue(nodeClientUsername(), nodeClientPassword()));
        return client -> (client instanceof NodeClient) ? client.filterWithHeader(headers) : client;
    }

    protected InternalClient internalClient() {
        return internalCluster().getInstance(InternalClient.class);
    }

    protected SecurityClient securityClient() {
        return securityClient(client());
    }

    public static SecurityClient securityClient(Client client) {
        return randomBoolean() ? new XPackClient(client).security() : new SecurityClient(client);
    }

    protected String getHttpURL() {
        final NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().get();
        final List<NodeInfo> nodes = nodeInfos.getNodes();
        assertTrue("there is at least one node", nodes.size() > 0);
        NodeInfo ni = randomFrom(nodes);
        boolean useSSL = XPackSettings.HTTP_SSL_ENABLED.get(ni.getSettings());
        TransportAddress publishAddress = ni.getHttp().address().publishAddress();
        InetSocketAddress address = publishAddress.address();
        return (useSSL ? "https://" : "http://") + NetworkAddress.format(address.getAddress()) + ":" + address.getPort();
    }

    public void assertSecurityIndexActive() throws Exception {
        assertSecurityIndexActive(internalCluster());
    }

    public void assertSecurityIndexActive(InternalTestCluster internalTestCluster) throws Exception {
        for (ClusterService clusterService : internalTestCluster.getInstances(ClusterService.class)) {
            assertBusy(() -> {
                ClusterState clusterState = clusterService.state();
                assertFalse(clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
                assertTrue(securityIndexMappingAndTemplateSufficientToRead(clusterState, logger));
                Index securityIndex = resolveSecurityIndex(clusterState.metaData());
                if (securityIndex != null) {
                    IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(securityIndex);
                    if (indexRoutingTable != null) {
                        assertTrue(indexRoutingTable.allPrimaryShardsActive());
                    }
                }
            });
        }
    }

    public void assertSecurityIndexWriteable() throws Exception {
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            assertBusy(() -> {
                ClusterState clusterState = clusterService.state();
                assertFalse(clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
                assertTrue(securityIndexMappingAndTemplateUpToDate(clusterState, logger));
                Index securityIndex = resolveSecurityIndex(clusterState.metaData());
                if (securityIndex != null) {
                    IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(securityIndex);
                    if (indexRoutingTable != null) {
                        assertTrue(indexRoutingTable.allPrimaryShardsActive());
                    }
                }
            });
        }
    }

    protected void deleteSecurityIndex() {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(SECURITY_INDEX_NAME);
        getIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        GetIndexResponse getIndexResponse = internalClient().admin().indices().getIndex(getIndexRequest).actionGet();
        if (getIndexResponse.getIndices().length > 0) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(getIndexResponse.getIndices());
            internalClient().admin().indices().delete(deleteIndexRequest).actionGet();
        }
    }

    private static Index resolveSecurityIndex(MetaData metaData) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(SECURITY_INDEX_NAME);
        if (aliasOrIndex != null) {
            return aliasOrIndex.getIndices().get(0).getIndex();
        }
        return null;
    }
}