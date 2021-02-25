package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.common.GroupedActionListener;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.security.authz.permission.DefaultRole;
import org.elasticsearch.xpack.security.authz.permission.GlobalPermission;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.security.authz.permission.SuperuserRole;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import java.util.Arrays;
import java.util.Collection;
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

    private final ClusterService clusterService;

    private final CompositeRolesStore rolesStore;

    private final AuditTrailService auditTrail;

    private final IndicesAndAliasesResolver indicesAndAliasesResolver;

    private final AuthenticationFailureHandler authcFailureHandler;

    private final ThreadContext threadContext;

    private final AnonymousUser anonymousUser;

    private final boolean isAnonymousEnabled;

    private final boolean anonymousAuthzExceptionEnabled;

    public AuthorizationService(Settings settings, CompositeRolesStore rolesStore, ClusterService clusterService, AuditTrailService auditTrail, AuthenticationFailureHandler authcFailureHandler, ThreadPool threadPool, AnonymousUser anonymousUser) {
        super(settings);
        this.rolesStore = rolesStore;
        this.clusterService = clusterService;
        this.auditTrail = auditTrail;
        this.indicesAndAliasesResolver = new IndicesAndAliasesResolver(new IndexNameExpressionResolver(settings));
        this.authcFailureHandler = authcFailureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.anonymousUser = anonymousUser;
        this.isAnonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.anonymousAuthzExceptionEnabled = ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.get(settings);
    }

    public void authorize(Authentication authentication, String action, TransportRequest request, Collection<Role> userRoles, Collection<Role> runAsRoles) throws ElasticsearchSecurityException {
        final TransportRequest originalRequest = request;
        if (request instanceof ConcreteShardRequest) {
            request = ((ConcreteShardRequest<?>) request).getRequest();
        }
        setOriginatingAction(action);
        if (SystemUser.is(authentication.getRunAsUser())) {
            if (SystemUser.isAuthorized(action) && SystemUser.is(authentication.getUser())) {
                setIndicesAccessControl(IndicesAccessControl.ALLOW_ALL);
                grant(authentication, action, request);
                return;
            }
            throw denial(authentication, action, request);
        }
        Collection<Role> roles = userRoles;
        GlobalPermission permission = permission(roles);
        final boolean isRunAs = authentication.isRunAs();
        if (permission.isEmpty()) {
            if (isRunAs) {
                throw denyRunAs(authentication, action, request);
            } else {
                throw denial(authentication, action, request);
            }
        }
        if (isRunAs) {
            if (authentication.getLookedUpBy() == null) {
                throw denyRunAs(authentication, action, request);
            }
            RunAsPermission runAs = permission.runAs();
            if (runAs != null && runAs.check(authentication.getRunAsUser().principal())) {
                grantRunAs(authentication, action, request);
                roles = runAsRoles;
                permission = permission(roles);
                if (permission.isEmpty()) {
                    throw denial(authentication, action, request);
                }
            } else {
                throw denyRunAs(authentication, action, request);
            }
        }
        if (ClusterPrivilege.ACTION_MATCHER.test(action)) {
            ClusterPermission cluster = permission.cluster();
            if (cluster != null && cluster.check(action, request, authentication)) {
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
        }
        if (request instanceof IndicesRequest == false && request instanceof IndicesAliasesRequest == false) {
            if (isScrollRelatedAction(action)) {
                grant(authentication, action, request);
                return;
            }
            assert false : "only scroll related requests are known indices api that don't support retrieving the indices they relate to";
            throw denial(authentication, action, request);
        }
        if (permission.indices() == null || permission.indices().isEmpty()) {
            throw denial(authentication, action, request);
        }
        MetaData metaData = clusterService.state().metaData();
        AuthorizedIndices authorizedIndices = new AuthorizedIndices(authentication.getRunAsUser(), roles, action, metaData);
        Set<String> indexNames = resolveIndexNames(authentication, action, request, metaData, authorizedIndices);
        assert !indexNames.isEmpty() : "every indices request needs to have its indices set thus the resolved indices must not be empty";
        if (indexNames.size() == 1 && indexNames.contains(IndicesAndAliasesResolver.NO_INDEX_PLACEHOLDER)) {
            setIndicesAccessControl(IndicesAccessControl.ALLOW_NO_INDICES);
            grant(authentication, action, request);
            return;
        }
        IndicesAccessControl indicesAccessControl = permission.authorize(action, indexNames, metaData);
        if (!indicesAccessControl.isGranted()) {
            throw denial(authentication, action, request);
        } else if (indicesAccessControl.getIndexPermissions(SecurityTemplateService.SECURITY_INDEX_NAME) != null && indicesAccessControl.getIndexPermissions(SecurityTemplateService.SECURITY_INDEX_NAME).isGranted() && XPackUser.is(authentication.getRunAsUser()) == false && MONITOR_INDEX_PREDICATE.test(action) == false && Arrays.binarySearch(authentication.getRunAsUser().roles(), SuperuserRole.NAME) < 0) {
            logger.debug("user [{}] attempted to directly perform [{}] against the security index [{}]", authentication.getRunAsUser().principal(), action, SecurityTemplateService.SECURITY_INDEX_NAME);
            throw denial(authentication, action, request);
        } else {
            setIndicesAccessControl(indicesAccessControl);
        }
        if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
            assert request instanceof CreateIndexRequest;
            Set<Alias> aliases = ((CreateIndexRequest) request).aliases();
            if (!aliases.isEmpty()) {
                Set<String> aliasesAndIndices = Sets.newHashSet(indexNames);
                for (Alias alias : aliases) {
                    aliasesAndIndices.add(alias.name());
                }
                indicesAccessControl = permission.authorize("indices:admin/aliases", aliasesAndIndices, metaData);
                if (!indicesAccessControl.isGranted()) {
                    throw denial(authentication, "indices:admin/aliases", request);
                }
            }
        }
        grant(authentication, action, originalRequest);
    }

    private Set<String> resolveIndexNames(Authentication authentication, String action, TransportRequest request, MetaData metaData, AuthorizedIndices authorizedIndices) {
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

    private void setOriginatingAction(String action) {
        String originatingAction = threadContext.getTransient(ORIGINATING_ACTION_KEY);
        if (originatingAction == null) {
            threadContext.putTransient(ORIGINATING_ACTION_KEY, action);
        }
    }

    GlobalPermission permission(Collection<Role> roles) {
        GlobalPermission.Compound.Builder rolesBuilder = GlobalPermission.Compound.builder();
        for (Role role : roles) {
            rolesBuilder.add(role);
        }
        return rolesBuilder.build();
    }

    public void roles(User user, ActionListener<Collection<Role>> roleActionListener) {
        if (SystemUser.is(user)) {
            throw new IllegalArgumentException("the user [" + user.principal() + "] is the system user and we should never try to get its" + " roles");
        }
        if (XPackUser.is(user)) {
            assert XPackUser.INSTANCE.roles().length == 1 && SuperuserRole.NAME.equals(XPackUser.INSTANCE.roles()[0]);
            roleActionListener.onResponse(Collections.singleton(SuperuserRole.INSTANCE));
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
        final Collection<Role> defaultRoles = Collections.singletonList(DefaultRole.INSTANCE);
        if (roleNames.isEmpty()) {
            roleActionListener.onResponse(defaultRoles);
        } else {
            final GroupedActionListener<Role> listener = new GroupedActionListener<>(roleActionListener, roleNames.size(), defaultRoles);
            for (String roleName : roleNames) {
                rolesStore.roles(roleName, listener);
            }
        }
    }

    private static String IndexActionSubRequestPrimary = IndexAction.NAME + "[p]";

    private static String IndexActionSubRequestReplica = IndexAction.NAME + "[r]";

    private static String DeleteActionSubRequestPrimary = DeleteAction.NAME + "[p]";

    private static String DeleteActionSubRequestReplica = DeleteAction.NAME + "[r]";

    private static boolean isCompositeAction(String action) {
        return action.equals(BulkAction.NAME) || action.equals(IndexAction.NAME) || action.equals(DeleteAction.NAME) || action.equals(IndexActionSubRequestPrimary) || action.equals(IndexActionSubRequestReplica) || action.equals(DeleteActionSubRequestPrimary) || action.equals(DeleteActionSubRequestReplica) || action.equals(MultiGetAction.NAME) || action.equals(MultiTermVectorsAction.NAME) || action.equals(MultiSearchAction.NAME) || action.equals("indices:data/read/mpercolate") || action.equals("indices:data/read/msearch/template") || action.equals("indices:data/read/search/template") || action.equals("indices:data/write/reindex");
    }

    private static boolean isScrollRelatedAction(String action) {
        return action.equals(SearchScrollAction.NAME) || action.equals(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.QUERY_SCROLL_ACTION_NAME) || action.equals(SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME) || action.equals(ClearScrollAction.NAME) || action.equals(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
    }

    private ElasticsearchSecurityException denial(Authentication authentication, String action, TransportRequest request) {
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
        final User user = authentication.getUser();
        if (isAnonymousEnabled && anonymousUser.equals(user)) {
            if (anonymousAuthzExceptionEnabled == false) {
                throw authcFailureHandler.authenticationRequired(action, threadContext);
            }
        }
        if (user != authentication.getRunAsUser()) {
            return authorizationError("action [{}] is unauthorized for user [{}] run as [{}]", action, user.principal(), authentication.getRunAsUser().principal());
        }
        return authorizationError("action [{}] is unauthorized for user [{}]", action, user.principal());
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}