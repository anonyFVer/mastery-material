package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import static java.util.Collections.unmodifiableList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class AuditTrailServiceTests extends ESTestCase {

    private List<AuditTrail> auditTrails;

    private AuditTrailService service;

    private AuthenticationToken token;

    private TransportMessage message;

    private RestRequest restRequest;

    private XPackLicenseState licenseState;

    private boolean isAuditingAllowed;

    @Before
    public void init() throws Exception {
        List<AuditTrail> auditTrailsBuilder = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            auditTrailsBuilder.add(mock(AuditTrail.class));
        }
        auditTrails = unmodifiableList(auditTrailsBuilder);
        licenseState = mock(XPackLicenseState.class);
        service = new AuditTrailService(Settings.EMPTY, auditTrails, licenseState);
        isAuditingAllowed = randomBoolean();
        when(licenseState.isAuditingAllowed()).thenReturn(isAuditingAllowed);
        token = mock(AuthenticationToken.class);
        message = mock(TransportMessage.class);
        restRequest = mock(RestRequest.class);
    }

    public void testAuthenticationFailed() throws Exception {
        service.authenticationFailed(token, "_action", message);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(token, "_action", message);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedNoToken() throws Exception {
        service.authenticationFailed("_action", message);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed("_action", message);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        service.authenticationFailed(restRequest);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(restRequest);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRest() throws Exception {
        service.authenticationFailed(token, restRequest);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed(token, restRequest);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRealm() throws Exception {
        service.authenticationFailed("_realm", token, "_action", message);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed("_realm", token, "_action", message);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        service.authenticationFailed("_realm", token, restRequest);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationFailed("_realm", token, restRequest);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAnonymousAccess() throws Exception {
        service.anonymousAccessDenied("_action", message);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).anonymousAccessDenied("_action", message);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAccessGranted() throws Exception {
        User user = new User("_username", "r1");
        service.accessGranted(user, "_action", message);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).accessGranted(user, "_action", message);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAccessDenied() throws Exception {
        User user = new User("_username", "r1");
        service.accessDenied(user, "_action", message);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).accessDenied(user, "_action", message);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testConnectionGranted() throws Exception {
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = randomBoolean() ? SecurityIpFilterRule.ACCEPT_ALL : IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        service.connectionGranted(inetAddress, "client", rule);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).connectionGranted(inetAddress, "client", rule);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testConnectionDenied() throws Exception {
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        service.connectionDenied(inetAddress, "client", rule);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).connectionDenied(inetAddress, "client", rule);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationSuccessRest() throws Exception {
        User user = new User("_username", "r1");
        String realm = "_realm";
        service.authenticationSuccess(realm, user, restRequest);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationSuccess(realm, user, restRequest);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        User user = new User("_username", "r1");
        String realm = "_realm";
        service.authenticationSuccess(realm, user, "_action", message);
        verify(licenseState).isAuditingAllowed();
        if (isAuditingAllowed) {
            for (AuditTrail auditTrail : auditTrails) {
                verify(auditTrail).authenticationSuccess(realm, user, "_action", message);
            }
        } else {
            verifyZeroInteractions(auditTrails.toArray((Object[]) new AuditTrail[auditTrails.size()]));
        }
    }
}