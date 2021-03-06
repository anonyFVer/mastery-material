package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.junit.Before;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import static org.hamcrest.Matchers.is;

public class AuthorizationUtilsTests extends ESTestCase {

    private ThreadContext threadContext;

    @Before
    public void setupContext() {
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testSystemUserSwitchNonInternalAction() {
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, randomFrom("indices:foo", "cluster:bar")), is(false));
    }

    public void testSystemUserSwitchWithSystemUser() {
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, new Authentication(SystemUser.INSTANCE, new RealmRef("test", "test", "foo"), null));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(false));
    }

    public void testSystemUserSwitchWithNullUser() {
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(true));
    }

    public void testSystemUserSwitchWithNonSystemUser() {
        User user = new User(randomAlphaOfLength(6), new String[] {});
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        threadContext.putTransient(AuthorizationService.ORIGINATING_ACTION_KEY, randomFrom("indices:foo", "cluster:bar"));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(true));
    }

    public void testSystemUserSwitchWithNonSystemUserAndInternalAction() {
        User user = new User(randomAlphaOfLength(6), new String[] {});
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        threadContext.putTransient(AuthorizationService.ORIGINATING_ACTION_KEY, randomFrom("internal:foo/bar"));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(false));
    }

    public void testShouldSetUser() {
        assertFalse(AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadContext));
        threadContext.putTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME, randomAlphaOfLength(4));
        assertTrue(AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadContext));
        User user = new User(randomAlphaOfLength(6), new String[] {});
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), null);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        assertFalse(AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadContext));
        threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(AuthenticationField.AUTHENTICATION_KEY, authentication);
        assertFalse(AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadContext));
        threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME, randomAlphaOfLength(4));
        assertFalse(AuthorizationUtils.shouldSetUserBasedOnActionOrigin(threadContext));
    }

    public void testSwitchAndExecuteXpackSecurityUser() throws Exception {
        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        final String headerName = randomAlphaOfLengthBetween(4, 16);
        final String headerValue = randomAlphaOfLengthBetween(4, 16);
        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<Void> listener = ActionListener.wrap(v -> {
            assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getHeader(headerName));
            assertEquals(XPackSecurityUser.INSTANCE, securityContext.getAuthentication().getUser());
            latch.countDown();
        }, e -> fail(e.getMessage()));
        final Consumer<ThreadContext.StoredContext> consumer = original -> {
            assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getHeader(headerName));
            assertEquals(XPackSecurityUser.INSTANCE, securityContext.getAuthentication().getUser());
            latch.countDown();
            listener.onResponse(null);
        };
        threadContext.putHeader(headerName, headerValue);
        threadContext.putTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME, ClientHelper.SECURITY_ORIGIN);
        AuthorizationUtils.switchUserBasedOnActionOriginAndExecute(threadContext, securityContext, consumer);
        latch.await();
    }

    public void testSwitchAndExecuteXpackUser() throws Exception {
        SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        final String headerName = randomAlphaOfLengthBetween(4, 16);
        final String headerValue = randomAlphaOfLengthBetween(4, 16);
        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<Void> listener = ActionListener.wrap(v -> {
            assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getHeader(headerName));
            assertEquals(XPackUser.INSTANCE, securityContext.getAuthentication().getUser());
            latch.countDown();
        }, e -> fail(e.getMessage()));
        final Consumer<ThreadContext.StoredContext> consumer = original -> {
            assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getHeader(headerName));
            assertEquals(XPackUser.INSTANCE, securityContext.getAuthentication().getUser());
            latch.countDown();
            listener.onResponse(null);
        };
        threadContext.putHeader(headerName, headerValue);
        threadContext.putTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME, randomFrom(ClientHelper.ML_ORIGIN, ClientHelper.WATCHER_ORIGIN, ClientHelper.DEPRECATION_ORIGIN, ClientHelper.MONITORING_ORIGIN, ClientHelper.PERSISTENT_TASK_ORIGIN, ClientHelper.INDEX_LIFECYCLE_ORIGIN));
        AuthorizationUtils.switchUserBasedOnActionOriginAndExecute(threadContext, securityContext, consumer);
        latch.await();
    }
}