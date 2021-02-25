package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.ROLE_NAMES_KEY;

public final class SecuritySearchOperationListener implements SearchOperationListener {

    private final ThreadContext threadContext;

    private final XPackLicenseState licenseState;

    private final AuditTrailService auditTrailService;

    public SecuritySearchOperationListener(ThreadContext threadContext, XPackLicenseState licenseState, AuditTrailService auditTrail) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
        this.auditTrailService = auditTrail;
    }

    @Override
    public void onNewScrollContext(SearchContext searchContext) {
        if (licenseState.isAuthAllowed()) {
            searchContext.scrollContext().putInContext(Authentication.AUTHENTICATION_KEY, Authentication.getAuthentication(threadContext));
        }
    }

    @Override
    public void validateSearchContext(SearchContext searchContext, TransportRequest request) {
        if (licenseState.isAuthAllowed()) {
            if (searchContext.scrollContext() != null) {
                final Authentication originalAuth = searchContext.scrollContext().getFromContext(Authentication.AUTHENTICATION_KEY);
                final Authentication current = Authentication.getAuthentication(threadContext);
                final String action = threadContext.getTransient(ORIGINATING_ACTION_KEY);
                ensureAuthenticatedUserIsSame(originalAuth, current, auditTrailService, searchContext.id(), action, request, threadContext.getTransient(ROLE_NAMES_KEY));
            }
        }
    }

    static void ensureAuthenticatedUserIsSame(Authentication original, Authentication current, AuditTrailService auditTrailService, long id, String action, TransportRequest request, String[] roleNames) {
        final boolean samePrincipal = original.getUser().principal().equals(current.getUser().principal());
        final boolean sameRealmType;
        if (original.getUser().isRunAs()) {
            if (current.getUser().isRunAs()) {
                sameRealmType = original.getLookedUpBy().getType().equals(current.getLookedUpBy().getType());
            } else {
                sameRealmType = original.getLookedUpBy().getType().equals(current.getAuthenticatedBy().getType());
            }
        } else if (current.getUser().isRunAs()) {
            sameRealmType = original.getAuthenticatedBy().getType().equals(current.getLookedUpBy().getType());
        } else {
            sameRealmType = original.getAuthenticatedBy().getType().equals(current.getAuthenticatedBy().getType());
        }
        final boolean sameUser = samePrincipal && sameRealmType;
        if (sameUser == false) {
            auditTrailService.accessDenied(current.getUser(), action, request, roleNames);
            throw new SearchContextMissingException(id);
        }
    }
}