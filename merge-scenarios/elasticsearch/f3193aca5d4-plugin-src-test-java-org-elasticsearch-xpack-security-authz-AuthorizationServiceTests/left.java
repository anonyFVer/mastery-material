package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.security.action.user.AuthenticateRequestBuilder;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.action.user.UserRequest;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.ldap.LdapRealm;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest;
import org.junit.Before;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import static java.util.Collections.singleton;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionRunAs;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class AuthorizationServiceTests extends ESTestCase {

    private AuditTrailService auditTrail;

    private ClusterService clusterService;

    private AuthorizationService authorizationService;

    private ThreadContext threadContext;

    private ThreadPool threadPool;

    private Map<String, RoleDescriptor> roleMap = new HashMap<>();

    private CompositeRolesStore rolesStore;

    @Before
    public void setup() {
        rolesStore = mock(CompositeRolesStore.class);
        clusterService = mock(ClusterService.class);
        final Settings settings = Settings.builder().put("search.remote.other_cluster.seeds", "localhost:9999").build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        auditTrail = mock(AuditTrailService.class);
        threadContext = new ThreadContext(settings);
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(settings);
        doAnswer((i) -> {
            ActionListener<Role> callback = (ActionListener<Role>) i.getArguments()[2];
            Set<String> names = (Set<String>) i.getArguments()[0];
            assertNotNull(names);
            Set<RoleDescriptor> roleDescriptors = new HashSet<>();
            for (String name : names) {
                RoleDescriptor descriptor = roleMap.get(name);
                if (descriptor != null) {
                    roleDescriptors.add(descriptor);
                }
            }
            if (roleDescriptors.isEmpty()) {
                callback.onResponse(Role.EMPTY);
            } else {
                callback.onResponse(CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache));
            }
            return Void.TYPE;
        }).when(rolesStore).roles(any(Set.class), any(FieldPermissionsCache.class), any(ActionListener.class));
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail, new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(settings));
    }

    private void authorize(Authentication authentication, String action, TransportRequest request) {
        PlainActionFuture future = new PlainActionFuture();
        AuthorizationUtils.AsyncAuthorizer authorizer = new AuthorizationUtils.AsyncAuthorizer(authentication, future, (userRoles, runAsRoles) -> {
            authorizationService.authorize(authentication, action, request, userRoles, runAsRoles);
            future.onResponse(null);
        });
        authorizer.authorize(authorizationService);
        future.actionGet();
    }

    public void testActionsSystemUserIsAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        authorize(createAuthentication(SystemUser.INSTANCE), "indices:monitor/whatever", request);
        verify(auditTrail).accessGranted(SystemUser.INSTANCE, "indices:monitor/whatever", request, null);
        authorize(createAuthentication(SystemUser.INSTANCE), "internal:whatever", request);
        verify(auditTrail).accessGranted(SystemUser.INSTANCE, "internal:whatever", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testIndicesActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(SystemUser.INSTANCE), "indices:", request), "indices:", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "indices:", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/whatever", request), "cluster:admin/whatever", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/whatever", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminSnapshotStatusActionIsNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/snapshot/status", request), "cluster:admin/snapshot/status", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/snapshot/status", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNoRolesCausesDenial() {
        TransportRequest request = new SearchRequest();
        User user = new User("test user");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), "indices:a", request), "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCanPerformRemoteSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("other_cluster:index1", "*_cluster:index2", "other_cluster:other_*");
        User user = new User("test user");
        mockEmptyMetaData();
        authorize(createAuthentication(user), SearchAction.NAME, request);
        verify(auditTrail).accessGranted(user, SearchAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCannotPerformLocalSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("no_such_cluster:index");
        User user = new User("test user");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), SearchAction.NAME, request), SearchAction.NAME, "test user");
        verify(auditTrail).accessDenied(user, SearchAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCanPerformMultiClusterSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("local_index", "wildcard_*", "other_cluster:remote_index", "*:foo?");
        User user = new User("test user");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), SearchAction.NAME, request), SearchAction.NAME, "test user");
        verify(auditTrail).accessDenied(user, SearchAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCannotSql() {
        TransportRequest request = new SqlRequest();
        User user = new User("test user");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), SqlAction.NAME, request), SqlAction.NAME, "test user");
        verify(auditTrail).accessDenied(user, SqlAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRemoteIndicesOnlyWorkWithApplicableRequestTypes() {
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices("other_cluster:index1", "other_cluster:index2");
        User user = new User("test user");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), DeleteIndexAction.NAME, request), DeleteIndexAction.NAME, "test user");
        verify(auditTrail).accessDenied(user, DeleteIndexAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUnknownRoleCausesDenial() {
        @SuppressWarnings("unchecked")
        Tuple<String, TransportRequest> tuple = randomFrom(new Tuple<>(SearchAction.NAME, new SearchRequest()), new Tuple<>(IndicesExistsAction.NAME, new IndicesExistsRequest()), new Tuple<>(SqlAction.NAME, new SqlRequest()));
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        User user = new User("test user", "non-existent-role");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), action, request), action, "test user");
        verify(auditTrail).accessDenied(user, action, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatNonIndicesAndNonClusterActionIsDenied() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), "whatever", request), "whatever", "test user");
        verify(auditTrail).accessDenied(user, "whatever", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatRoleWithNoIndicesIsDenied() {
        @SuppressWarnings("unchecked")
        Tuple<String, TransportRequest> tuple = randomFrom(new Tuple<>(SearchAction.NAME, new SearchRequest()), new Tuple<>(IndicesExistsAction.NAME, new IndicesExistsRequest()), new Tuple<>(SqlAction.NAME, new SqlRequest()));
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        User user = new User("test user", "no_indices");
        roleMap.put("no_indices", new RoleDescriptor("a_role", null, null, null));
        mockEmptyMetaData();
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), action, request), action, "test user");
        verify(auditTrail).accessDenied(user, action, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testElasticUserAuthorizedForNonChangePasswordRequestsWhenNotInSetupMode() {
        final User user = new ElasticUser(true);
        Tuple<String, TransportRequest> request = randomCompositeRequest();
        authorize(createAuthentication(user), request.v1(), request.v2());
        verify(auditTrail).accessGranted(user, request.v1(), request.v2(), null);
    }

    public void testSearchAgainstEmptyCluster() {
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        {
            SearchRequest searchRequest = new SearchRequest("does_not_exist").indicesOptions(IndicesOptions.fromOptions(false, true, true, false));
            assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), SearchAction.NAME, searchRequest), SearchAction.NAME, "test user");
            verify(auditTrail).accessDenied(user, SearchAction.NAME, searchRequest, null);
            verifyNoMoreInteractions(auditTrail);
        }
        {
            SearchRequest searchRequest = new SearchRequest("does_not_exist").indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
            authorize(createAuthentication(user), SearchAction.NAME, searchRequest);
            verify(auditTrail).accessGranted(user, SearchAction.NAME, searchRequest, null);
            IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_KEY);
            IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(IndicesAndAliasesResolver.NO_INDEX_PLACEHOLDER);
            assertFalse(indexAccessControl.getFieldPermissions().hasFieldLevelSecurity());
            assertNull(indexAccessControl.getQueries());
        }
    }

    public void testScrollRelatedRequestsAllowed() {
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        authorize(createAuthentication(user), ClearScrollAction.NAME, clearScrollRequest);
        verify(auditTrail).accessGranted(user, ClearScrollAction.NAME, clearScrollRequest, null);
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        authorize(createAuthentication(user), SearchScrollAction.NAME, searchScrollRequest);
        verify(auditTrail).accessGranted(user, SearchScrollAction.NAME, searchScrollRequest, null);
        TransportRequest request = mock(TransportRequest.class);
        authorize(createAuthentication(user), SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request, null);
        authorize(createAuthentication(user), SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request, null);
        authorize(createAuthentication(user), SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request, null);
        authorize(createAuthentication(user), SearchTransportService.QUERY_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.QUERY_SCROLL_ACTION_NAME, request, null);
        authorize(createAuthentication(user), SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeIndicesFailures() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), "indices:a", request), "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request, null);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testCreateIndexWithAliasWithoutPermissions() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), CreateIndexAction.NAME, request), IndicesAliasesAction.NAME, "test user");
        verify(auditTrail).accessDenied(user, IndicesAliasesAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testCreateIndexWithAlias() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_all", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a", "a2").privileges("all").build() }, null));
        authorize(createAuthentication(user), CreateIndexAction.NAME, request);
        verify(auditTrail).accessGranted(user, CreateIndexAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testDenialForAnonymousUser() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "a_all").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail, new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        roleMap.put("a_all", new RoleDescriptor("a_all", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(anonymousUser), "indices:a", request), "indices:a", anonymousUser.principal());
        verify(auditTrail).accessDenied(anonymousUser, "indices:a", request, null);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testDenialForAnonymousUserAuthorizationExceptionDisabled() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "a_all").put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), false).build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail, new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(settings));
        roleMap.put("a_all", new RoleDescriptor("a_all", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class, () -> authorize(createAuthentication(anonymousUser), "indices:a", request));
        assertAuthenticationException(securityException, containsString("action [indices:a] requires authentication"));
        verify(auditTrail).accessDenied(anonymousUser, "indices:a", request, null);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testAuditTrailIsRecordedWhenIndexWildcardThrowsError() {
        IndicesOptions options = IndicesOptions.fromOptions(false, false, true, true);
        TransportRequest request = new GetIndexRequest().indices("not-an-index-*").indicesOptions(options);
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_all", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        final IndexNotFoundException nfe = expectThrows(IndexNotFoundException.class, () -> authorize(createAuthentication(user), GetIndexAction.NAME, request));
        assertThat(nfe.getIndex(), is(notNullValue()));
        assertThat(nfe.getIndex().getName(), is("not-an-index-*"));
        verify(auditTrail).accessDenied(user, GetIndexAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testAuditTrailIsRecordedWhenIndexWildcardThrowsErrorDuringDelayed() {
        IndicesOptions options = IndicesOptions.fromOptions(false, false, false, false);
        TransportRequest request = new SqlRequest();
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_all", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        try (StoredContext context = threadContext.stashContext()) {
            authorize(createAuthentication(user), SqlAction.NAME, request);
            verify(auditTrail).accessGranted(user, SqlAction.NAME, request, null);
            BiFunction<IndicesOptions, String[], IndicesAccessControl> resolver = threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_RESOLVER_KEY);
            final Exception e = expectThrows(ElasticsearchParseException.class, () -> resolver.apply(options, new String[] { "<{>" }));
            assertEquals("invalid dynamic name expression [{>]. date math placeholder is open ended", e.getMessage());
            verify(auditTrail).accessDenied(user, SqlAction.NAME, request, singleton("<{>"));
        }
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testRunAsRequestWithNoRolesUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("run as me", null, new User("test user", "admin"));
        assertNotEquals(user.authenticatedUser(), user);
        assertThrowsAuthorizationExceptionRunAs(() -> authorize(createAuthentication(user), "indices:a", request), "indices:a", "test user", "run as me");
        verify(auditTrail).runAsDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithoutLookedUpBy() {
        AuthenticateRequest request = new AuthenticateRequest("run as me");
        roleMap.put("can run as", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        User user = new User("run as me", Strings.EMPTY_ARRAY, new User("test user", new String[] { "can run as" }));
        Authentication authentication = new Authentication(user, new RealmRef("foo", "bar", "baz"), null);
        assertNotEquals(user.authenticatedUser(), user);
        assertThrowsAuthorizationExceptionRunAs(() -> authorize(authentication, AuthenticateAction.NAME, request), AuthenticateAction.NAME, "test user", "run as me");
        verify(auditTrail).runAsDenied(user, AuthenticateAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestRunningAsUnAllowedUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("run as me", new String[] { "doesn't exist" }, new User("test user", "can run as"));
        assertNotEquals(user.authenticatedUser(), user);
        roleMap.put("can run as", new RoleDescriptor("can run as", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, new String[] { "not the right user" }));
        assertThrowsAuthorizationExceptionRunAs(() -> authorize(createAuthentication(user), "indices:a", request), "indices:a", "test user", "run as me");
        verify(auditTrail).runAsDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithRunAsUserWithoutPermission() {
        TransportRequest request = new GetIndexRequest().indices("a");
        User user = new User("run as me", new String[] { "b" }, new User("test user", "can run as"));
        assertNotEquals(user.authenticatedUser(), user);
        roleMap.put("can run as", new RoleDescriptor("can run as", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, new String[] { "run as me" }));
        if (randomBoolean()) {
            ClusterState state = mock(ClusterState.class);
            when(clusterService.state()).thenReturn(state);
            when(state.metaData()).thenReturn(MetaData.builder().put(new IndexMetaData.Builder("a").settings(Settings.builder().put("index.version.created", Version.CURRENT).build()).numberOfShards(1).numberOfReplicas(0).build(), true).build());
            roleMap.put("b", new RoleDescriptor("b", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("all").build() }, null));
        } else {
            mockEmptyMetaData();
        }
        assertThrowsAuthorizationExceptionRunAs(() -> authorize(createAuthentication(user), "indices:a", request), "indices:a", "test user", "run as me");
        verify(auditTrail).runAsGranted(user, "indices:a", request);
        verify(auditTrail).accessDenied(user, "indices:a", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithValidPermissions() {
        TransportRequest request = new GetIndexRequest().indices("b");
        User user = new User("run as me", new String[] { "b" }, new User("test user", new String[] { "can run as" }));
        assertNotEquals(user.authenticatedUser(), user);
        roleMap.put("can run as", new RoleDescriptor("can run as", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, new String[] { "run as me" }));
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder().put(new IndexMetaData.Builder("b").settings(Settings.builder().put("index.version.created", Version.CURRENT).build()).numberOfShards(1).numberOfReplicas(0).build(), true).build());
        roleMap.put("b", new RoleDescriptor("b", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("all").build() }, null));
        authorize(createAuthentication(user), "indices:a", request);
        verify(auditTrail).runAsGranted(user, "indices:a", request);
        verify(auditTrail).accessGranted(user, "indices:a", request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNonXPackUserCannotExecuteOperationAgainstSecurityIndex() {
        User user = new User("all_access_user", "all_access");
        roleMap.put("all_access", new RoleDescriptor("all access", new String[] { "all" }, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() }, null));
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder().put(new IndexMetaData.Builder(SecurityLifecycleService.SECURITY_INDEX_NAME).settings(Settings.builder().put("index.version.created", Version.CURRENT).build()).numberOfShards(1).numberOfReplicas(0).build(), true).build());
        List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(BulkAction.NAME + "[s]", new DeleteRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(UpdateAction.NAME, new UpdateRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(BulkAction.NAME + "[s]", new IndexRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(TermVectorsAction.NAME, new TermVectorsRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(GetAction.NAME, new GetRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(TermVectorsAction.NAME, new TermVectorsRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(IndicesAliasesAction.NAME, new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("security_alias").index(SecurityLifecycleService.SECURITY_INDEX_NAME))));
        requests.add(new Tuple<>(UpdateSettingsAction.NAME, new UpdateSettingsRequest().indices(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        for (Tuple<String, TransportRequest> requestTuple : requests) {
            String action = requestTuple.v1();
            TransportRequest request = requestTuple.v2();
            assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), action, request), action, "all_access_user");
            verify(auditTrail).accessDenied(user, action, request, null);
            verifyNoMoreInteractions(auditTrail);
        }
        TransportRequest request = new ClusterHealthRequest(SecurityLifecycleService.SECURITY_INDEX_NAME);
        authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
        request = new ClusterHealthRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "foo", "bar");
        authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
        try (StoredContext context = threadContext.stashContext()) {
            request = new SqlRequest();
            authorize(createAuthentication(user), SqlAction.NAME, request);
            verify(auditTrail).accessGranted(user, SqlAction.NAME, request, null);
            BiFunction<IndicesOptions, String[], IndicesAccessControl> resolver = threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_RESOLVER_KEY);
            assertThrowsAuthorizationException(() -> resolver.apply(IndicesOptions.strictSingleIndexNoExpandForbidClosed(), new String[] { SecurityLifecycleService.SECURITY_INDEX_NAME }), SqlAction.NAME, "all_access_user");
            verify(auditTrail).accessDenied(user, SqlAction.NAME, request, singleton(SecurityLifecycleService.SECURITY_INDEX_NAME));
            verifyNoMoreInteractions(auditTrail);
        }
        SearchRequest searchRequest = new SearchRequest("_all");
        authorize(createAuthentication(user), SearchAction.NAME, searchRequest);
        assertEquals(2, searchRequest.indices().length);
        assertEquals(IndicesAndAliasesResolver.NO_INDICES_LIST, Arrays.asList(searchRequest.indices()));
    }

    public void testGrantedNonXPackUserCanExecuteMonitoringOperationsAgainstSecurityIndex() {
        User user = new User("all_access_user", "all_access");
        roleMap.put("all_access", new RoleDescriptor("all access", new String[] { "all" }, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() }, null));
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder().put(new IndexMetaData.Builder(SecurityLifecycleService.SECURITY_INDEX_NAME).settings(Settings.builder().put("index.version.created", Version.CURRENT).build()).numberOfShards(1).numberOfReplicas(0).build(), true).build());
        List<Tuple<String, ? extends TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(IndicesStatsAction.NAME, new IndicesStatsRequest().indices(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(RecoveryAction.NAME, new RecoveryRequest().indices(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(IndicesSegmentsAction.NAME, new IndicesSegmentsRequest().indices(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(GetSettingsAction.NAME, new GetSettingsRequest().indices(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(IndicesShardStoresAction.NAME, new IndicesShardStoresRequest().indices(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(UpgradeStatusAction.NAME, new UpgradeStatusRequest().indices(SecurityLifecycleService.SECURITY_INDEX_NAME)));
        for (Tuple<String, ? extends TransportRequest> requestTuple : requests) {
            String action = requestTuple.v1();
            TransportRequest request = requestTuple.v2();
            authorize(createAuthentication(user), action, request);
            verify(auditTrail).accessGranted(user, action, request, null);
        }
    }

    public void testXPackUserAndSuperusersCanExecuteOperationAgainstSecurityIndex() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder().put(new IndexMetaData.Builder(SecurityLifecycleService.SECURITY_INDEX_NAME).settings(Settings.builder().put("index.version.created", Version.CURRENT).build()).numberOfShards(1).numberOfReplicas(0).build(), true).build());
        for (User user : Arrays.asList(XPackUser.INSTANCE, superuser)) {
            List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
            requests.add(new Tuple<>(DeleteAction.NAME, new DeleteRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(BulkAction.NAME + "[s]", new DeleteRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(UpdateAction.NAME, new UpdateRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(IndexAction.NAME, new IndexRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(BulkAction.NAME + "[s]", new IndexRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(SecurityLifecycleService.SECURITY_INDEX_NAME)));
            requests.add(new Tuple<>(TermVectorsAction.NAME, new TermVectorsRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(GetAction.NAME, new GetRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(TermVectorsAction.NAME, new TermVectorsRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(IndicesAliasesAction.NAME, new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("security_alias").index(SecurityLifecycleService.SECURITY_INDEX_NAME))));
            requests.add(new Tuple<>(ClusterHealthAction.NAME, new ClusterHealthRequest(SecurityLifecycleService.SECURITY_INDEX_NAME)));
            requests.add(new Tuple<>(ClusterHealthAction.NAME, new ClusterHealthRequest(SecurityLifecycleService.SECURITY_INDEX_NAME, "foo", "bar")));
            for (Tuple<String, TransportRequest> requestTuple : requests) {
                String action = requestTuple.v1();
                TransportRequest request = requestTuple.v2();
                authorize(createAuthentication(user), action, request);
                verify(auditTrail).accessGranted(user, action, request, null);
            }
        }
    }

    public void testXPackUserAndSuperusersCanExecuteOperationAgainstSecurityIndexWithWildcard() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder().put(new IndexMetaData.Builder(SecurityLifecycleService.SECURITY_INDEX_NAME).settings(Settings.builder().put("index.version.created", Version.CURRENT).build()).numberOfShards(1).numberOfReplicas(0).build(), true).build());
        String action = SearchAction.NAME;
        SearchRequest request = new SearchRequest("_all");
        authorize(createAuthentication(XPackUser.INSTANCE), action, request);
        verify(auditTrail).accessGranted(XPackUser.INSTANCE, action, request, null);
        assertThat(request.indices(), arrayContaining(".security"));
        request = new SearchRequest("_all");
        authorize(createAuthentication(superuser), action, request);
        verify(auditTrail).accessGranted(superuser, action, request, null);
        assertThat(request.indices(), arrayContaining(".security"));
    }

    public void testAnonymousRolesAreAppliedToOtherUsers() {
        TransportRequest request = new ClusterHealthRequest();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail, new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        roleMap.put("anonymous_user_role", new RoleDescriptor("anonymous_user_role", new String[] { "all" }, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        authorize(createAuthentication(anonymousUser), ClusterHealthAction.NAME, request);
        authorize(createAuthentication(anonymousUser), IndicesExistsAction.NAME, new IndicesExistsRequest("a"));
        final User userWithNoRoles = new User("no role user");
        authorize(createAuthentication(userWithNoRoles), ClusterHealthAction.NAME, request);
        authorize(createAuthentication(userWithNoRoles), IndicesExistsAction.NAME, new IndicesExistsRequest("a"));
    }

    public void testDefaultRoleUserWithoutRoles() {
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        authorizationService.roles(new User("no role user"), rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertEquals(Role.EMPTY, roles);
    }

    public void testAnonymousUserEnabledRoleAdded() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail, new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        roleMap.put("anonymous_user_role", new RoleDescriptor("anonymous_user_role", new String[] { "all" }, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        authorizationService.roles(new User("no role user"), rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertThat(roles.name(), containsString("anonymous_user_role"));
    }

    public void testCompositeActionsAreImmediatelyRejected() {
        Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        String action = compositeRequest.v1();
        TransportRequest request = compositeRequest.v2();
        User user = new User("test user", "no_indices");
        roleMap.put("no_indices", new RoleDescriptor("no_indices", null, null, null));
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), action, request), action, "test user");
        verify(auditTrail).accessDenied(user, action, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsIndicesAreNotChecked() {
        Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        String action = compositeRequest.v1();
        TransportRequest request = compositeRequest.v2();
        User user = new User("test user", "role");
        roleMap.put("role", new RoleDescriptor("role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build() }, null));
        authorize(createAuthentication(user), action, request);
        verify(auditTrail).accessGranted(user, action, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsMustImplementCompositeIndicesRequest() {
        String action = randomCompositeRequest().v1();
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "role");
        roleMap.put("role", new RoleDescriptor("role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build() }, null));
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, () -> authorize(createAuthentication(user), action, request));
        assertThat(illegalStateException.getMessage(), containsString("Composite actions must implement CompositeIndicesRequest"));
    }

    public void testCompositeActionsIndicesAreCheckedAtTheShardLevel() {
        String action;
        switch(randomIntBetween(0, 4)) {
            case 0:
                action = MultiGetAction.NAME + "[shard]";
                break;
            case 1:
                action = SearchAction.NAME;
                break;
            case 2:
                action = MultiTermVectorsAction.NAME + "[shard]";
                break;
            case 3:
                action = BulkAction.NAME + "[s]";
                break;
            case 4:
                action = "indices:data/read/mpercolate[s]";
                break;
            default:
                throw new UnsupportedOperationException();
        }
        logger.info("--> action: {}", action);
        TransportRequest request = new MockIndicesRequest(IndicesOptions.strictExpandOpen(), "index");
        User userAllowed = new User("userAllowed", "roleAllowed");
        roleMap.put("roleAllowed", new RoleDescriptor("roleAllowed", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index").privileges("all").build() }, null));
        User userDenied = new User("userDenied", "roleDenied");
        roleMap.put("roleDenied", new RoleDescriptor("roleDenied", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        authorize(createAuthentication(userAllowed), action, request);
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(userDenied), action, request), action, "userDenied");
    }

    public void testDelayedActionsAreImmediatelyRejectedWithoutPermissions() {
        TransportRequest request = new SqlRequest();
        User user = new User("test user", "no_indices");
        roleMap.put("no_indices", new RoleDescriptor("no_indices", null, null, null));
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), SqlAction.NAME, request), SqlAction.NAME, "test user");
        verify(auditTrail).accessDenied(user, SqlAction.NAME, request, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testDelayedActionsAddResolver() {
        String action = SqlAction.NAME;
        MockIndicesRequest request = new MockIndicesRequest(IndicesOptions.strictExpandOpen(), "index");
        User userAllowed = new User("userAllowed", "roleAllowed");
        roleMap.put("roleAllowed", new RoleDescriptor("roleAllowed", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index").privileges("all").build() }, null));
        User userDenied = new User("userDenied", "roleDenied");
        roleMap.put("roleDenied", new RoleDescriptor("roleDenied", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        User userSome = new User("userSome", "roleSome");
        roleMap.put("roleSome", new RoleDescriptor("roleSome", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build(), IndicesPrivileges.builder().indices("b").privileges("all").build() }, null));
        mockEmptyMetaData();
        assertNull(getAccessControlResolver());
        try (StoredContext ctxRestore = threadContext.stashContext()) {
            authorize(createAuthentication(userAllowed), action, request);
            verify(auditTrail).accessGranted(userAllowed, action, request, null);
            assertNotNull(getAccessControlResolver());
            IndicesAccessControl iac = getAccessControlResolver().apply(IndicesOptions.strictExpandOpen(), new String[] { "index" });
            assertTrue(iac.isGranted());
            assertTrue(iac.getIndexPermissions("index").isGranted());
            verify(auditTrail).accessGranted(userAllowed, action, request, singleton("index"));
        }
        assertNull(getAccessControlResolver());
        try (StoredContext ctxRestore = threadContext.stashContext()) {
            authorize(createAuthentication(userDenied), action, request);
            verify(auditTrail).accessGranted(userDenied, action, request, null);
            assertNotNull(getAccessControlResolver());
            assertThrowsAuthorizationException(() -> getAccessControlResolver().apply(IndicesOptions.strictExpandOpen(), new String[] { "index" }), action, "userDenied");
            verify(auditTrail).accessDenied(userDenied, action, request, singleton("index"));
        }
        assertNull(getAccessControlResolver());
        try (StoredContext ctxRestore = threadContext.stashContext()) {
            authorize(createAuthentication(userSome), action, request);
            verify(auditTrail).accessGranted(userSome, action, request, null);
            assertNotNull(getAccessControlResolver());
            IndicesAccessControl iac = getAccessControlResolver().apply(IndicesOptions.strictExpandOpen(), new String[] { "a" });
            assertTrue(iac.isGranted());
            assertTrue(iac.getIndexPermissions("a").isGranted());
            iac = getAccessControlResolver().apply(IndicesOptions.strictExpandOpen(), new String[] { "b" });
            assertTrue(iac.isGranted());
            assertTrue(iac.getIndexPermissions("b").isGranted());
            verify(auditTrail).accessGranted(userSome, action, request, singleton("b"));
            iac = getAccessControlResolver().apply(IndicesOptions.strictExpandOpen(), new String[] { "a", "b" });
            assertTrue(iac.isGranted());
            assertTrue(iac.getIndexPermissions("a").isGranted());
            assertTrue(iac.getIndexPermissions("b").isGranted());
            verify(auditTrail).accessGranted(userSome, action, request, singleton("a"));
            verify(auditTrail).accessGranted(userSome, action, request, singleton("b"));
            assertThrowsAuthorizationException(() -> getAccessControlResolver().apply(IndicesOptions.strictExpandOpen(), new String[] { "index", "a", "b" }), action, "userSome");
            verify(auditTrail).accessDenied(userSome, action, request, new HashSet<>(Arrays.asList("index", "a", "b")));
        }
        assertNull(getAccessControlResolver());
    }

    public void testSameUserPermission() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ? new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request() : new AuthenticateRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAlphaOfLengthBetween(4, 12));
        assertThat(request, instanceOf(UserRequest.class));
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowNonMatchingUsername() {
        final User authUser = new User("admin", new String[] { "bar" });
        final User user = new User("joe", null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final String username = randomFrom("", "joe" + randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(3, 10));
        final TransportRequest request = changePasswordRequest ? new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() : new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAlphaOfLengthBetween(4, 12));
        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        when(authentication.getUser()).thenReturn(user);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType()).thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAlphaOfLengthBetween(4, 12));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        if (request instanceof ChangePasswordRequest) {
            ((ChangePasswordRequest) request).username("joe");
        } else {
            ((AuthenticateRequest) request).username("joe");
        }
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowOtherActions() {
        final User user = mock(User.class);
        final TransportRequest request = mock(TransportRequest.class);
        final String action = randomFrom(PutUserAction.NAME, DeleteUserAction.NAME, ClusterHealthAction.NAME, ClusterStateAction.NAME, ClusterStatsAction.NAME, GetLicenseAction.NAME);
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        final boolean runAs = randomBoolean();
        when(authentication.getUser()).thenReturn(user);
        when(user.authenticatedUser()).thenReturn(runAs ? new User("authUser") : user);
        when(user.isRunAs()).thenReturn(runAs);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(randomAlphaOfLengthBetween(4, 12));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verifyZeroInteractions(user, request, authentication);
    }

    public void testSameUserPermissionRunAsChecksAuthenticatedBy() {
        final User authUser = new User("admin", new String[] { "bar" });
        final String username = "joe";
        final User user = new User(username, null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ? new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() : new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType()).thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAlphaOfLengthBetween(4, 12));
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        when(authentication.getUser()).thenReturn(authUser);
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForOtherRealms() {
        final User user = new User("joe");
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(randomFrom(LdapRealm.LDAP_TYPE, FileRealm.TYPE, LdapRealm.AD_TYPE, PkiRealm.TYPE, randomAlphaOfLengthBetween(4, 12)));
        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verify(authenticatedBy).getType();
        verify(authentication).getAuthenticatedBy();
        verify(authentication, times(2)).getUser();
        verifyNoMoreInteractions(authenticatedBy, authentication);
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForLookedUpByOtherRealms() {
        final User authUser = new User("admin", new String[] { "bar" });
        final User user = new User("joe", null, authUser);
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType()).thenReturn(randomFrom(LdapRealm.LDAP_TYPE, FileRealm.TYPE, LdapRealm.AD_TYPE, PkiRealm.TYPE, randomAlphaOfLengthBetween(4, 12)));
        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verify(authentication).getLookedUpBy();
        verify(authentication, times(2)).getUser();
        verify(lookedUpBy).getType();
        verifyNoMoreInteractions(authentication, lookedUpBy, authenticatedBy);
    }

    private static Tuple<String, TransportRequest> randomCompositeRequest() {
        switch(randomIntBetween(0, 7)) {
            case 0:
                return Tuple.tuple(MultiGetAction.NAME, new MultiGetRequest().add("index", "type", "id"));
            case 1:
                return Tuple.tuple(MultiSearchAction.NAME, new MultiSearchRequest().add(new SearchRequest()));
            case 2:
                return Tuple.tuple(MultiTermVectorsAction.NAME, new MultiTermVectorsRequest().add("index", "type", "id"));
            case 3:
                return Tuple.tuple(BulkAction.NAME, new BulkRequest().add(new DeleteRequest("index", "type", "id")));
            case 4:
                return Tuple.tuple("indices:data/read/mpercolate", new MockCompositeIndicesRequest());
            case 5:
                return Tuple.tuple("indices:data/read/msearch/template", new MockCompositeIndicesRequest());
            case 6:
                return Tuple.tuple("indices:data/read/search/template", new MockCompositeIndicesRequest());
            case 7:
                return Tuple.tuple("indices:data/write/reindex", new MockCompositeIndicesRequest());
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static class MockCompositeIndicesRequest extends TransportRequest implements CompositeIndicesRequest {
    }

    public void testDoesNotUseRolesStoreForXPackUser() {
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        authorizationService.roles(XPackUser.INSTANCE, rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertThat(roles, equalTo(ReservedRolesStore.SUPERUSER_ROLE));
        verifyZeroInteractions(rolesStore);
    }

    public void testGetRolesForSystemUserThrowsException() {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> authorizationService.roles(SystemUser.INSTANCE, null));
        assertEquals("the user [_system] is the system user and we should never try to get its roles", iae.getMessage());
    }

    private static Authentication createAuthentication(User user) {
        RealmRef lookedUpBy = user.authenticatedUser() == user ? null : new RealmRef("looked", "up", "by");
        return new Authentication(user, new RealmRef("test", "test", "foo"), lookedUpBy);
    }

    private ClusterState mockEmptyMetaData() {
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);
        return state;
    }

    public void testProxyRequestFailsOnNonProxyAction() {
        TransportRequest request = TransportRequest.Empty.INSTANCE;
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, request);
        User user = new User("test user", "role");
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, () -> authorize(createAuthentication(user), "indices:some/action", transportRequest));
        assertThat(illegalStateException.getMessage(), startsWith("originalRequest is a proxy request for: [org.elasticsearch.transport.TransportRequest$"));
        assertThat(illegalStateException.getMessage(), endsWith("] but action: [indices:some/action] isn't"));
    }

    public void testProxyRequestFailsOnNonProxyRequest() {
        TransportRequest request = TransportRequest.Empty.INSTANCE;
        User user = new User("test user", "role");
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, () -> authorize(createAuthentication(user), TransportActionProxy.getProxyAction("indices:some/action"), request));
        assertThat(illegalStateException.getMessage(), startsWith("originalRequest is not a proxy request: [org.elasticsearch.transport.TransportRequest$"));
        assertThat(illegalStateException.getMessage(), endsWith("] but action: [internal:transport/proxy/indices:some/action] is a proxy action"));
    }

    public void testProxyRequestAuthenticationDenied() {
        TransportRequest proxiedRequest = new SearchRequest();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, proxiedRequest);
        String action = TransportActionProxy.getProxyAction(SearchTransportService.QUERY_ACTION_NAME);
        User user = new User("test user", "no_indices");
        roleMap.put("no_indices", new RoleDescriptor("no_indices", null, null, null));
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), action, transportRequest), action, "test user");
        verify(auditTrail).accessDenied(user, action, proxiedRequest, null);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testProxyRequestAuthenticationGrantedWithAllPrivileges() {
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        authorize(createAuthentication(user), action, transportRequest);
        verify(auditTrail).accessGranted(user, action, clearScrollRequest, null);
    }

    public void testProxyRequestAuthenticationGranted() {
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("read_cross_cluster").build() }, null));
        mockEmptyMetaData();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        authorize(createAuthentication(user), action, transportRequest);
        verify(auditTrail).accessGranted(user, action, clearScrollRequest, null);
    }

    public void testProxyRequestAuthenticationDeniedWithReadPrivileges() {
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null, new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("read").build() }, null));
        mockEmptyMetaData();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(user), action, transportRequest), action, "test user");
        verify(auditTrail).accessDenied(user, action, clearScrollRequest, null);
    }

    private BiFunction<IndicesOptions, String[], IndicesAccessControl> getAccessControlResolver() {
        return threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_RESOLVER_KEY);
    }
}