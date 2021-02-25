package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
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
import org.elasticsearch.xpack.security.authc.esnative.NativeRealmSettings;
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
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.security.user.XPackUser;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import static org.elasticsearch.xpack.security.SecurityField.setting;
import static org.elasticsearch.xpack.security.support.Exceptions.authorizationError;

public class AuthorizationService extends AbstractComponent {

    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING = Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);

    public static final String ORIGINATING_ACTION_KEY = "_originating_action_name";

    public static final String ROLE_NAMES_KEY = "_effective_role_names";

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
            if (SystemUser.isAuthorized(action)) {
                putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
                putTransientIfNonExisting(ROLE_NAMES_KEY, new String[] { SystemUser.ROLE_NAME });
                grant(authentication, action, request, new String[] { SystemUser.ROLE_NAME });
                return;
            }
            throw denial(authentication, action, request, new String[] { SystemUser.ROLE_NAME });
        }
        Role permission = userRole;
        final boolean isRunAs = authentication.getUser().isRunAs();
        if (isRunAs) {
            if (authentication.getLookedUpBy() == null) {
                throw denyRunAs(authentication, action, request, permission.names());
            } else if (permission.runAs().check(authentication.getUser().principal())) {
                grantRunAs(authentication, action, request, permission.names());
                permission = runAsRole;
            } else {
                throw denyRunAs(authentication, action, request, permission.names());
            }
        }
        putTransientIfNonExisting(ROLE_NAMES_KEY, permission.names());
        if (ClusterPrivilege.ACTION_MATCHER.test(action)) {
            ClusterPermission cluster = permission.cluster();
            if (cluster.check(action) || checkSameUserPermissions(action, request, authentication)) {
                putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_ALL);
                grant(authentication, action, request, permission.names());
                return;
            }
            throw denial(authentication, action, request, permission.names());
        }
        if (!IndexPrivilege.ACTION_MATCHER.test(action)) {
            throw denial(authentication, action, request, permission.names());
        }
        if (isCompositeAction(action)) {
            if (request instanceof CompositeIndicesRequest == false) {
                throw new IllegalStateException("Composite actions must implement " + CompositeIndicesRequest.class.getSimpleName() + ", " + request.getClass().getSimpleName() + " doesn't");
            }
            if (permission.indices().check(action)) {
                grant(authentication, action, request, permission.names());
                return;
            }
            throw denial(authentication, action, request, permission.names());
        } else if (isTranslatedToBulkAction(action)) {
            if (request instanceof CompositeIndicesRequest == false) {
                throw new IllegalStateException("Bulk translated actions must implement " + CompositeIndicesRequest.class.getSimpleName() + ", " + request.getClass().getSimpleName() + " doesn't");
            }
            if (permission.indices().check(action)) {
                grant(authentication, action, request, permission.names());
                return;
            }
            throw denial(authentication, action, request, permission.names());
        } else if (TransportActionProxy.isProxyAction(action)) {
            if (TransportActionProxy.isProxyRequest(originalRequest) == false) {
                throw new IllegalStateException("originalRequest is not a proxy request: [" + originalRequest + "] but action: [" + action + "] is a proxy action");
            }
            if (permission.indices().check(action)) {
                grant(authentication, action, request, permission.names());
                return;
            } else {
                throw denial(authentication, action, request, permission.names());
            }
        }
        if (request instanceof IndicesRequest == false && request instanceof IndicesAliasesRequest == false) {
            if (isScrollRelatedAction(action)) {
                if (SearchScrollAction.NAME.equals(action) && permission.indices().check(action) == false) {
                    throw denial(authentication, action, request, permission.names());
                } else {
                    grant(authentication, action, request, permission.names());
                    return;
                }
            } else {
                assert false : "only scroll related requests are known indices api that don't support retrieving the indices they relate to";
                throw denial(authentication, action, request, permission.names());
            }
        }
        final boolean allowsRemoteIndices = request instanceof IndicesRequest && IndicesAndAliasesResolver.allowsRemoteIndices((IndicesRequest) request);
        if (allowsRemoteIndices == false && permission.indices().check(action) == false) {
            throw denial(authentication, action, request, permission.names());
        }
        final MetaData metaData = clusterService.state().metaData();
        final AuthorizedIndices authorizedIndices = new AuthorizedIndices(authentication.getUser(), permission, action, metaData);
        final ResolvedIndices resolvedIndices = resolveIndexNames(authentication, action, request, metaData, authorizedIndices, permission);
        assert !resolvedIndices.isEmpty() : "every indices request needs to have its indices set thus the resolved indices must not be empty";
        if (resolvedIndices.getRemote().isEmpty() && permission.indices().check(action) == false) {
            throw denial(authentication, action, request, permission.names());
        }
        if (resolvedIndices.isNoIndicesPlaceholder()) {
            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, IndicesAccessControl.ALLOW_NO_INDICES);
            grant(authentication, action, request, permission.names());
            return;
        }
        final Set<String> localIndices = new HashSet<>(resolvedIndices.getLocal());
        IndicesAccessControl indicesAccessControl = permission.authorize(action, localIndices, metaData, fieldPermissionsCache);
        if (!indicesAccessControl.isGranted()) {
            throw denial(authentication, action, request, permission.names());
        } else if (hasSecurityIndexAccess(indicesAccessControl) && MONITOR_INDEX_PREDICATE.test(action) == false && isSuperuser(authentication.getUser()) == false) {
            logger.debug("user [{}] attempted to directly perform [{}] against the security index [{}]", authentication.getUser().principal(), action, SecurityLifecycleService.SECURITY_INDEX_NAME);
            throw denial(authentication, action, request, permission.names());
        } else {
            putTransientIfNonExisting(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);
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
                    throw denial(authentication, "indices:admin/aliases", request, permission.names());
                }
            }
        }
        if (action.equals(TransportShardBulkAction.ACTION_NAME)) {
            assert request instanceof BulkShardRequest : "Action " + action + " requires " + BulkShardRequest.class + " but was " + request.getClass();
            authorizeBulkItems(authentication, (BulkShardRequest) request, permission, metaData, localIndices, authorizedIndices);
        }
        grant(authentication, action, originalRequest, permission.names());
    }

    private boolean hasSecurityIndexAccess(IndicesAccessControl indicesAccessControl) {
        for (String index : SecurityLifecycleService.indexNames()) {
            final IndicesAccessControl.IndexAccessControl indexPermissions = indicesAccessControl.getIndexPermissions(index);
            if (indexPermissions != null && indexPermissions.isGranted()) {
                return true;
            }
        }
        return false;
    }

    private void authorizeBulkItems(Authentication authentication, BulkShardRequest request, Role permission, MetaData metaData, Set<String> indices, AuthorizedIndices authorizedIndices) {
        final Map<String, String> resolvedIndexNames = new HashMap<>();
        final Map<Tuple<String, String>, Boolean> indexActionAuthority = new HashMap<>();
        for (BulkItemRequest item : request.items()) {
            String resolvedIndex = resolvedIndexNames.computeIfAbsent(item.index(), key -> {
                final ResolvedIndices resolvedIndices = indicesAndAliasesResolver.resolveIndicesAndAliases(item.request(), metaData, authorizedIndices);
                if (resolvedIndices.getRemote().size() != 0) {
                    throw illegalArgument("Bulk item should not write to remote indices, but request writes to " + String.join(",", resolvedIndices.getRemote()));
                }
                if (resolvedIndices.getLocal().size() != 1) {
                    throw illegalArgument("Bulk item should write to exactly 1 index, but request writes to " + String.join(",", resolvedIndices.getLocal()));
                }
                final String resolved = resolvedIndices.getLocal().get(0);
                if (indices.contains(resolved) == false) {
                    throw illegalArgument("Found bulk item that writes to index " + resolved + " but the request writes to " + indices);
                }
                return resolved;
            });
            final String itemAction = getAction(item);
            final Tuple<String, String> indexAndAction = new Tuple<>(resolvedIndex, itemAction);
            final boolean granted = indexActionAuthority.computeIfAbsent(indexAndAction, key -> {
                final IndicesAccessControl itemAccessControl = permission.authorize(itemAction, Collections.singleton(resolvedIndex), metaData, fieldPermissionsCache);
                return itemAccessControl.isGranted();
            });
            if (granted == false) {
                item.abort(resolvedIndex, denial(authentication, itemAction, request, permission.names()));
            }
        }
    }

    private IllegalArgumentException illegalArgument(String message) {
        assert false : message;
        return new IllegalArgumentException(message);
    }

    private static String getAction(BulkItemRequest item) {
        final DocWriteRequest docWriteRequest = item.request();
        switch(docWriteRequest.opType()) {
            case INDEX:
            case CREATE:
                return IndexAction.NAME;
            case UPDATE:
                return UpdateAction.NAME;
            case DELETE:
                return DeleteAction.NAME;
        }
        throw new IllegalArgumentException("No equivalent action for opType [" + docWriteRequest.opType() + "]");
    }

    private ResolvedIndices resolveIndexNames(Authentication authentication, String action, TransportRequest request, MetaData metaData, AuthorizedIndices authorizedIndices, Role permission) {
        try {
            return indicesAndAliasesResolver.resolve(request, metaData, authorizedIndices);
        } catch (Exception e) {
            auditTrail.accessDenied(authentication.getUser(), action, request, permission.names());
            throw e;
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
            assert XPackUser.INSTANCE.roles().length == 1;
            roleActionListener.onResponse(XPackUser.ROLE);
            return;
        }
        if (XPackSecurityUser.is(user)) {
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
        } else if (roleNames.contains(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName())) {
            roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
        } else {
            rolesStore.roles(roleNames, fieldPermissionsCache, roleActionListener);
        }
    }

    private static boolean isCompositeAction(String action) {
        return action.equals(BulkAction.NAME) || action.equals(MultiGetAction.NAME) || action.equals(MultiTermVectorsAction.NAME) || action.equals(MultiSearchAction.NAME) || action.equals("indices:data/read/mpercolate") || action.equals("indices:data/read/msearch/template") || action.equals("indices:data/read/search/template") || action.equals("indices:data/write/reindex") || action.equals("indices:data/read/sql") || action.equals("indices:data/read/sql/translate") || action.equals("indices:admin/sql/tables") || action.equals("indices:admin/sql/columns");
    }

    private static boolean isTranslatedToBulkAction(String action) {
        return action.equals(IndexAction.NAME) || action.equals(DeleteAction.NAME) || action.equals(INDEX_SUB_REQUEST_PRIMARY) || action.equals(INDEX_SUB_REQUEST_REPLICA) || action.equals(DELETE_SUB_REQUEST_PRIMARY) || action.equals(DELETE_SUB_REQUEST_REPLICA);
    }

    private static boolean isScrollRelatedAction(String action) {
        return action.equals(SearchScrollAction.NAME) || action.equals(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.QUERY_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME) || action.equals(ClearScrollAction.NAME) || action.equals("indices:data/read/sql/close_cursor") || action.equals(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
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
        return ReservedRealm.TYPE.equals(realmType) || NativeRealmSettings.TYPE.equals(realmType);
    }

    ElasticsearchSecurityException denial(Authentication authentication, String action, TransportRequest request, String[] roleNames) {
        auditTrail.accessDenied(authentication.getUser(), action, request, roleNames);
        return denialException(authentication, action);
    }

    private ElasticsearchSecurityException denyRunAs(Authentication authentication, String action, TransportRequest request, String[] roleNames) {
        auditTrail.runAsDenied(authentication.getUser(), action, request, roleNames);
        return denialException(authentication, action);
    }

    private void grant(Authentication authentication, String action, TransportRequest request, String[] roleNames) {
        auditTrail.accessGranted(authentication.getUser(), action, request, roleNames);
    }

    private void grantRunAs(Authentication authentication, String action, TransportRequest request, String[] roleNames) {
        auditTrail.runAsGranted(authentication.getUser(), action, request, roleNames);
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
        return Arrays.stream(user.roles()).anyMatch(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()::equals);
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}