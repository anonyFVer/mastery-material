package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.UserRequest;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.IndicesAndAliasesResolver.ResolvedIndices;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.support.Automatons;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.elasticsearch.xpack.sql.plugin.cli.action.CliAction;
import org.elasticsearch.xpack.sql.plugin.jdbc.action.JdbcAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.support.Exceptions.authorizationError;

public class AuthorizationService extends AbstractComponent {

    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING = Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);

    public static final String INDICES_PERMISSIONS_KEY = "_indices_permissions";

    public static final String ORIGINATING_ACTION_KEY = "_originating_action_name";

    private static final Predicate<String> MONITOR_INDEX_PREDICATE = IndexPrivilege.MONITOR.predicate();

    private static final Predicate<String> SAME_USER_PRIVILEGE = Automatons.predicate(ChangePasswordAction.NAME, AuthenticateAction.NAME, HasPrivilegesAction.NAME);

    private static final String INDEX_SUB_REQUEST_PRIMARY = IndexAction.NAME + "[p]";

    private static final String INDEX_SUB_REQUEST_REPLICA = IndexAction.NAME + "[r]";

    private static final String DELETE_SUB_REQUEST_PRIMARY = DeleteAction.NAME + "[p]";

    private static final String DELETE_SUB_REQUEST_REPLICA = DeleteAction.NAME + "[r]";

    private final ClusterService clusterService;

    private final CompositeRolesStore rolesStore;

    private final AuditTrailService auditTrail;

    private final IndicesAndAliasesResolver indicesAndAliasesResolver;

    private final AuthenticationFailureHandler authcFailureHandler;

    private final ThreadContext threadContext;

    private final AnonymousUser anonymousUser;

    private final FieldPermissionsCache fieldPermissionsCache;

    private final boolean isAnonymousEnabled;

    private final boolean anonymousAuthzExceptionEnabled;

    public AuthorizationService(Settings settings, CompositeRolesStore rolesStore, ClusterService clusterService, AuditTrailService auditTrail, AuthenticationFailureHandler authcFailureHandler, ThreadPool threadPool, AnonymousUser anonymousUser) {
        super(settings);
        this.rolesStore = rolesStore;
        this.clusterService = clusterService;
        this.auditTrail = auditTrail;
        this.indicesAndAliasesResolver = new IndicesAndAliasesResolver(settings, clusterService);
        this.authcFailureHandler = authcFailureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.anonymousUser = anonymousUser;
        this.isAnonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.anonymousAuthzExceptionEnabled = ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.get(settings);
        this.fieldPermissionsCache = new FieldPermissionsCache(settings);
    }

    public void authorize(Authentication authentication, String action, TransportRequest request, Role userRole, Role runAsRole) throws ElasticsearchSecurityException {
        final TransportRequest originalRequest = request;
        if (request instanceof ConcreteShardRequest) {
            request = ((ConcreteShardRequest<?>) request).getRequest();
            assert TransportActionProxy.isProxyRequest(request) == false : "expected non-proxy request for action: " + action;
        } else {
            request = TransportActionProxy.unwrapRequest(request);
            if (TransportActionProxy.isProxyRequest(originalRequest) && TransportActionProxy.isProxyAction(action) == false) {
                throw new IllegalStateException("originalRequest is a proxy request for: [" + request + "] but action: [" + action + "] isn't");
            }
        }
        putTransientIfNonExisting(ORIGINATING_ACTION_KEY, action);
        if (SystemUser.is(authentication.getUser())) {
            if (SystemUser.isAuthorized(action) && SystemUser.is(authentication.getUser())) {
                setIndicesAccessControl(IndicesAccessControl.ALLOW_ALL);
                grant(authentication, action, request);
                return;
            }
            throw denial(authentication, action, request);
        }
        Role permission = userRole;
        final boolean isRunAs = authentication.getUser().isRunAs();
        if (isRunAs) {
            if (authentication.getLookedUpBy() == null) {
                throw denyRunAs(authentication, action, request);
            } else if (permission.runAs().check(authentication.getUser().principal())) {
                grantRunAs(authentication, action, request);
                permission = runAsRole;
            } else {
                throw denyRunAs(authentication, action, request);
            }
        }
        if (ClusterPrivilege.ACTION_MATCHER.test(action)) {
            ClusterPermission cluster = permission.cluster();
            if (cluster.check(action) || checkSameUserPermissions(action, request, authentication)) {
                setIndicesAccessControl(IndicesAccessControl.ALLOW_ALL);
                grant(authentication, action, request);
                return;
            }
            throw denial(authentication, action, request);
        }
        if (!IndexPrivilege.ACTION_MATCHER.test(action)) {
            throw denial(authentication, action, request);
        }
        if (isCompositeAction(action)) {
            if (request instanceof CompositeIndicesRequest == false) {
                throw new IllegalStateException("Composite actions must implement " + CompositeIndicesRequest.class.getSimpleName() + ", " + request.getClass().getSimpleName() + " doesn't");
            }
            if (permission.indices().check(action)) {
                grant(authentication, action, request);
                return;
            }
            throw denial(authentication, action, request);
        } else if (isTranslatedToBulkAction(action)) {
            if (request instanceof CompositeIndicesRequest == false) {
                throw new IllegalStateException("Bulk translated actions must implement " + CompositeIndicesRequest.class.getSimpleName() + ", " + request.getClass().getSimpleName() + " doesn't");
            }
            if (permission.indices().check(action)) {
                grant(authentication, action, request);
                return;
            }
            throw denial(authentication, action, request);
        } else if (TransportActionProxy.isProxyAction(action)) {
            if (TransportActionProxy.isProxyRequest(originalRequest) == false) {
                throw new IllegalStateException("originalRequest is not a proxy request: [" + originalRequest + "] but action: [" + action + "] is a proxy action");
            }
            if (permission.indices().check(action)) {
                grant(authentication, action, request);
                return;
            } else {
                throw denial(authentication, action, request);
            }
        }
        if (request instanceof IndicesRequest == false && request instanceof IndicesAliasesRequest == false) {
            if (isScrollRelatedAction(action)) {
                if (SearchScrollAction.NAME.equals(action) && permission.indices().check(action) == false) {
                    throw denial(authentication, action, request);
                } else {
                    grant(authentication, action, request);
                    return;
                }
            } else {
                assert false : "only scroll related requests are known indices api that don't support retrieving the indices they relate to";
                throw denial(authentication, action, request);
            }
        }
        final boolean allowsRemoteIndices = request instanceof IndicesRequest && IndicesAndAliasesResolver.allowsRemoteIndices((IndicesRequest) request);
        if (allowsRemoteIndices == false && permission.indices().check(action) == false) {
            throw denial(authentication, action, request);
        }
        final MetaData metaData = clusterService.state().metaData();
        final AuthorizedIndices authorizedIndices = new AuthorizedIndices(authentication.getUser(), permission, action, metaData);
        final ResolvedIndices resolvedIndices = resolveIndexNames(authentication, action, request, metaData, authorizedIndices);
        assert !resolvedIndices.isEmpty() : "every indices request needs to have its indices set thus the resolved indices must not be empty";
        if (resolvedIndices.getRemote().isEmpty() && permission.indices().check(action) == false) {
            throw denial(authentication, action, request);
        }
        if (resolvedIndices.isNoIndicesPlaceholder()) {
            setIndicesAccessControl(IndicesAccessControl.ALLOW_NO_INDICES);
            grant(authentication, action, request);
            return;
        }
        final Set<String> localIndices = new HashSet<>(resolvedIndices.getLocal());
        IndicesAccessControl indicesAccessControl = permission.authorize(action, localIndices, metaData, fieldPermissionsCache);
        if (!indicesAccessControl.isGranted()) {
            throw denial(authentication, action, request);
        } else if (indicesAccessControl.getIndexPermissions(SecurityLifecycleService.SECURITY_INDEX_NAME) != null && indicesAccessControl.getIndexPermissions(SecurityLifecycleService.SECURITY_INDEX_NAME).isGranted() && XPackUser.is(authentication.getUser()) == false && MONITOR_INDEX_PREDICATE.test(action) == false && isSuperuser(authentication.getUser()) == false) {
            logger.debug("user [{}] attempted to directly perform [{}] against the security index [{}]", authentication.getUser().principal(), action, SecurityLifecycleService.SECURITY_INDEX_NAME);
            throw denial(authentication, action, request);
        } else {
            setIndicesAccessControl(indicesAccessControl);
        }
        if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
            assert request instanceof CreateIndexRequest;
            Set<Alias> aliases = ((CreateIndexRequest) request).aliases();
            if (!aliases.isEmpty()) {
                Set<String> aliasesAndIndices = Sets.newHashSet(localIndices);
                for (Alias alias : aliases) {
                    aliasesAndIndices.add(alias.name());
                }
                indicesAccessControl = permission.authorize("indices:admin/aliases", aliasesAndIndices, metaData, fieldPermissionsCache);
                if (!indicesAccessControl.isGranted()) {
                    throw denial(authentication, "indices:admin/aliases", request);
                }
            }
        }
        grant(authentication, action, originalRequest);
    }

    private ResolvedIndices resolveIndexNames(Authentication authentication, String action, TransportRequest request, MetaData metaData, AuthorizedIndices authorizedIndices) {
        try {
            return indicesAndAliasesResolver.resolve(request, metaData, authorizedIndices);
        } catch (Exception e) {
            auditTrail.accessDenied(authentication.getUser(), action, request);
            throw e;
        }
    }

    private void setIndicesAccessControl(IndicesAccessControl accessControl) {
        if (threadContext.getTransient(INDICES_PERMISSIONS_KEY) == null) {
            threadContext.putTransient(INDICES_PERMISSIONS_KEY, accessControl);
        }
    }

    private void putTransientIfNonExisting(String key, Object value) {
        Object existing = threadContext.getTransient(key);
        if (existing == null) {
            threadContext.putTransient(key, value);
        }
    }

    public void roles(User user, ActionListener<Role> roleActionListener) {
        if (SystemUser.is(user)) {
            throw new IllegalArgumentException("the user [" + user.principal() + "] is the system user and we should never try to get its" + " roles");
        }
        if (XPackUser.is(user)) {
            assert XPackUser.INSTANCE.roles().length == 1 && ReservedRolesStore.SUPERUSER_ROLE.name().equals(XPackUser.INSTANCE.roles()[0]);
            roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
            return;
        }
        Set<String> roleNames = new HashSet<>();
        Collections.addAll(roleNames, user.roles());
        if (isAnonymousEnabled && anonymousUser.equals(user) == false) {
            if (anonymousUser.roles().length == 0) {
                throw new IllegalStateException("anonymous is only enabled when the anonymous user has roles");
            }
            Collections.addAll(roleNames, anonymousUser.roles());
        }
        if (roleNames.isEmpty()) {
            roleActionListener.onResponse(Role.EMPTY);
        } else if (roleNames.contains(ReservedRolesStore.SUPERUSER_ROLE.name())) {
            roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
        } else {
            rolesStore.roles(roleNames, fieldPermissionsCache, roleActionListener);
        }
    }

    private static boolean isCompositeAction(String action) {
        return action.equals(BulkAction.NAME) || action.equals(MultiGetAction.NAME) || action.equals(MultiTermVectorsAction.NAME) || action.equals(MultiSearchAction.NAME) || action.equals("indices:data/read/mpercolate") || action.equals("indices:data/read/msearch/template") || action.equals("indices:data/read/search/template") || action.equals("indices:data/write/reindex") || action.equals(SqlAction.NAME) || action.equals(JdbcAction.NAME) || action.equals(CliAction.NAME);
    }

    private static boolean isTranslatedToBulkAction(String action) {
        return action.equals(IndexAction.NAME) || action.equals(DeleteAction.NAME) || action.equals(INDEX_SUB_REQUEST_PRIMARY) || action.equals(INDEX_SUB_REQUEST_REPLICA) || action.equals(DELETE_SUB_REQUEST_PRIMARY) || action.equals(DELETE_SUB_REQUEST_REPLICA);
    }

    private static boolean isScrollRelatedAction(String action) {
        return action.equals(SearchScrollAction.NAME) || action.equals(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.QUERY_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME) || action.equals(ClearScrollAction.NAME) || action.equals(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
    }

    static boolean checkSameUserPermissions(String action, TransportRequest request, Authentication authentication) {
        final boolean actionAllowed = SAME_USER_PRIVILEGE.test(action);
        if (actionAllowed) {
            if (request instanceof UserRequest == false) {
                assert false : "right now only a user request should be allowed";
                return false;
            }
            UserRequest userRequest = (UserRequest) request;
            String[] usernames = userRequest.usernames();
            if (usernames == null || usernames.length != 1 || usernames[0] == null) {
                assert false : "this role should only be used for actions to apply to a single user";
                return false;
            }
            final String username = usernames[0];
            final boolean sameUsername = authentication.getUser().principal().equals(username);
            if (sameUsername && ChangePasswordAction.NAME.equals(action)) {
                return checkChangePasswordAction(authentication);
            }
            assert AuthenticateAction.NAME.equals(action) || HasPrivilegesAction.NAME.equals(action) || sameUsername == false : "Action '" + action + "' should not be possible when sameUsername=" + sameUsername;
            return sameUsername;
        }
        return false;
    }

    private static boolean checkChangePasswordAction(Authentication authentication) {
        final boolean isRunAs = authentication.getUser().isRunAs();
        final String realmType;
        if (isRunAs) {
            realmType = authentication.getLookedUpBy().getType();
        } else {
            realmType = authentication.getAuthenticatedBy().getType();
        }
        assert realmType != null;
        return ReservedRealm.TYPE.equals(realmType) || NativeRealm.TYPE.equals(realmType);
    }

    ElasticsearchSecurityException denial(Authentication authentication, String action, TransportRequest request) {
        auditTrail.accessDenied(authentication.getUser(), action, request);
        return denialException(authentication, action);
    }

    private ElasticsearchSecurityException denyRunAs(Authentication authentication, String action, TransportRequest request) {
        auditTrail.runAsDenied(authentication.getUser(), action, request);
        return denialException(authentication, action);
    }

    private void grant(Authentication authentication, String action, TransportRequest request) {
        auditTrail.accessGranted(authentication.getUser(), action, request);
    }

    private void grantRunAs(Authentication authentication, String action, TransportRequest request) {
        auditTrail.runAsGranted(authentication.getUser(), action, request);
    }

    private ElasticsearchSecurityException denialException(Authentication authentication, String action) {
        final User authUser = authentication.getUser().authenticatedUser();
        if (isAnonymousEnabled && anonymousUser.equals(authUser)) {
            if (anonymousAuthzExceptionEnabled == false) {
                throw authcFailureHandler.authenticationRequired(action, threadContext);
            }
        }
        if (authentication.getUser().isRunAs()) {
            return authorizationError("action [{}] is unauthorized for user [{}] run as [{}]", action, authUser.principal(), authentication.getUser().principal());
        }
        return authorizationError("action [{}] is unauthorized for user [{}]", action, authUser.principal());
    }

    static boolean isSuperuser(User user) {
        return Arrays.stream(user.roles()).anyMatch(ReservedRolesStore.SUPERUSER_ROLE.name()::equals);
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}