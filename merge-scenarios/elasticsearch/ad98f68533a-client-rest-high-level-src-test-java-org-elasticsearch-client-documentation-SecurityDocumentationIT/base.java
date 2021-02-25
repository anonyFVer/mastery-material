package org.elasticsearch.client.documentation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.ClearRealmCacheRequest;
import org.elasticsearch.client.security.ClearRealmCacheResponse;
import org.elasticsearch.client.security.ClearRolesCacheRequest;
import org.elasticsearch.client.security.ClearRolesCacheResponse;
import org.elasticsearch.client.security.CreateApiKeyRequest;
import org.elasticsearch.client.security.CreateApiKeyResponse;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.CreateTokenResponse;
import org.elasticsearch.client.security.DeletePrivilegesRequest;
import org.elasticsearch.client.security.DeletePrivilegesResponse;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.DeleteRoleMappingResponse;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteRoleResponse;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.DeleteUserResponse;
import org.elasticsearch.client.security.DisableUserRequest;
import org.elasticsearch.client.security.EnableUserRequest;
import org.elasticsearch.client.security.ExpressionRoleMapping;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.GetApiKeyResponse;
import org.elasticsearch.client.security.GetPrivilegesRequest;
import org.elasticsearch.client.security.GetPrivilegesResponse;
import org.elasticsearch.client.security.GetRoleMappingsRequest;
import org.elasticsearch.client.security.GetRoleMappingsResponse;
import org.elasticsearch.client.security.GetRolesRequest;
import org.elasticsearch.client.security.GetRolesResponse;
import org.elasticsearch.client.security.GetSslCertificatesResponse;
import org.elasticsearch.client.security.GetUserPrivilegesResponse;
import org.elasticsearch.client.security.GetUsersRequest;
import org.elasticsearch.client.security.GetUsersResponse;
import org.elasticsearch.client.security.HasPrivilegesRequest;
import org.elasticsearch.client.security.HasPrivilegesResponse;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.InvalidateApiKeyResponse;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.client.security.InvalidateTokenResponse;
import org.elasticsearch.client.security.PutPrivilegesRequest;
import org.elasticsearch.client.security.PutPrivilegesResponse;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.PutRoleMappingResponse;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutRoleResponse;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.TemplateRoleName;
import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.client.security.support.CertificateInfo;
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AnyRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.ClusterPrivilegeName;
import org.elasticsearch.client.security.user.privileges.Role.IndexPrivilegeName;
import org.elasticsearch.client.security.user.privileges.UserIndicesPrivileges;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.hamcrest.Matchers;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SecurityDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testGetUsers() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        String[] usernames = new String[] { "user1", "user2", "user3" };
        addUser(client, usernames[0], randomAlphaOfLengthBetween(6, 10));
        addUser(client, usernames[1], randomAlphaOfLengthBetween(6, 10));
        addUser(client, usernames[2], randomAlphaOfLengthBetween(6, 10));
        {
            GetUsersRequest request = new GetUsersRequest(usernames[0]);
            GetUsersResponse response = client.security().getUsers(request, RequestOptions.DEFAULT);
            List<User> users = new ArrayList<>(1);
            users.addAll(response.getUsers());
            assertNotNull(response);
            assertThat(users.size(), equalTo(1));
            assertThat(users.get(0).getUsername(), is(usernames[0]));
        }
        {
            GetUsersRequest request = new GetUsersRequest(usernames);
            GetUsersResponse response = client.security().getUsers(request, RequestOptions.DEFAULT);
            List<User> users = new ArrayList<>(3);
            users.addAll(response.getUsers());
            users.sort(Comparator.comparing(User::getUsername));
            assertNotNull(response);
            assertThat(users.size(), equalTo(3));
            assertThat(users.get(0).getUsername(), equalTo(usernames[0]));
            assertThat(users.get(1).getUsername(), equalTo(usernames[1]));
            assertThat(users.get(2).getUsername(), equalTo(usernames[2]));
            assertThat(users.size(), equalTo(3));
        }
        {
            GetUsersRequest request = new GetUsersRequest();
            GetUsersResponse response = client.security().getUsers(request, RequestOptions.DEFAULT);
            List<User> users = new ArrayList<>(3);
            users.addAll(response.getUsers());
            assertNotNull(response);
            assertThat(users.size(), equalTo(9));
        }
        {
            GetUsersRequest request = new GetUsersRequest(usernames[0]);
            ActionListener<GetUsersResponse> listener;
            listener = new ActionListener<GetUsersResponse>() {

                @Override
                public void onResponse(GetUsersResponse getRolesResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<GetUsersResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().getUsersAsync(request, RequestOptions.DEFAULT, listener);
            final GetUsersResponse response = future.get(30, TimeUnit.SECONDS);
            List<User> users = new ArrayList<>(1);
            users.addAll(response.getUsers());
            assertNotNull(response);
            assertThat(users.size(), equalTo(1));
            assertThat(users.get(0).getUsername(), equalTo(usernames[0]));
        }
    }

    public void testPutUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
            User user = new User("example", Collections.singletonList("superuser"));
            PutUserRequest request = PutUserRequest.withPassword(user, password, true, RefreshPolicy.NONE);
            PutUserResponse response = client.security().putUser(request, RequestOptions.DEFAULT);
            boolean isCreated = response.isCreated();
            assertTrue(isCreated);
        }
        {
            byte[] salt = new byte[32];
            random().nextBytes(salt);
            char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
            User user = new User("example2", Collections.singletonList("superuser"));
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            PBEKeySpec keySpec = new PBEKeySpec(password, salt, 10000, 256);
            final byte[] pbkdfEncoded = secretKeyFactory.generateSecret(keySpec).getEncoded();
            char[] passwordHash = ("{PBKDF2}10000$" + Base64.getEncoder().encodeToString(salt) + "$" + Base64.getEncoder().encodeToString(pbkdfEncoded)).toCharArray();
            PutUserRequest request = PutUserRequest.withPasswordHash(user, passwordHash, true, RefreshPolicy.NONE);
            try {
                client.security().putUser(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchStatusException e) {
                assertThat(e.getDetailedMessage(), containsString("PBKDF2"));
            }
        }
        {
            User user = new User("example", Arrays.asList("superuser", "another-role"));
            PutUserRequest request = PutUserRequest.updateUser(user, true, RefreshPolicy.NONE);
            ActionListener<PutUserResponse> listener = new ActionListener<PutUserResponse>() {

                @Override
                public void onResponse(PutUserResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().putUserAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        addUser(client, "testUser", "testPassword");
        {
            DeleteUserRequest deleteUserRequest = new DeleteUserRequest("testUser");
            DeleteUserResponse deleteUserResponse = client.security().deleteUser(deleteUserRequest, RequestOptions.DEFAULT);
            boolean found = deleteUserResponse.isAcknowledged();
            assertTrue(found);
            deleteUserResponse = client.security().deleteUser(deleteUserRequest, RequestOptions.DEFAULT);
            assertFalse(deleteUserResponse.isAcknowledged());
        }
        {
            DeleteUserRequest deleteUserRequest = new DeleteUserRequest("testUser", RefreshPolicy.IMMEDIATE);
            ActionListener<DeleteUserResponse> listener;
            listener = new ActionListener<DeleteUserResponse>() {

                @Override
                public void onResponse(DeleteUserResponse deleteUserResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().deleteUserAsync(deleteUserRequest, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    private void addUser(RestHighLevelClient client, String userName, String password) throws IOException {
        User user = new User(userName, Collections.singletonList(userName));
        PutUserRequest request = PutUserRequest.withPassword(user, password.toCharArray(), true, RefreshPolicy.NONE);
        PutUserResponse response = client.security().putUser(request, RequestOptions.DEFAULT);
        assertTrue(response.isCreated());
    }

    public void testPutRoleMapping() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        {
            final RoleMapperExpression rules = AnyRoleMapperExpression.builder().addExpression(FieldRoleMapperExpression.ofUsername("*")).addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com")).build();
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true, Collections.singletonList("superuser"), Collections.emptyList(), rules, null, RefreshPolicy.NONE);
            final PutRoleMappingResponse response = client.security().putRoleMapping(request, RequestOptions.DEFAULT);
            boolean isCreated = response.isCreated();
            assertTrue(isCreated);
        }
        {
            final RoleMapperExpression rules = AnyRoleMapperExpression.builder().addExpression(FieldRoleMapperExpression.ofUsername("*")).addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com")).build();
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true, Collections.emptyList(), Collections.singletonList(new TemplateRoleName("{\"source\":\"{{username}}\"}", TemplateRoleName.Format.STRING)), rules, null, RefreshPolicy.NONE);
            ActionListener<PutRoleMappingResponse> listener = new ActionListener<PutRoleMappingResponse>() {

                @Override
                public void onResponse(PutRoleMappingResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<PutRoleMappingResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().putRoleMappingAsync(request, RequestOptions.DEFAULT, listener);
            assertThat(future.get(), notNullValue());
            assertThat(future.get().isCreated(), is(false));
        }
    }

    public void testGetRoleMappings() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        final TemplateRoleName monitoring = new TemplateRoleName("{\"source\":\"monitoring\"}", TemplateRoleName.Format.STRING);
        final TemplateRoleName template = new TemplateRoleName("{\"source\":\"{{username}}\"}", TemplateRoleName.Format.STRING);
        final RoleMapperExpression rules1 = AnyRoleMapperExpression.builder().addExpression(FieldRoleMapperExpression.ofUsername("*")).addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com")).build();
        final PutRoleMappingRequest putRoleMappingRequest1 = new PutRoleMappingRequest("mapping-example-1", true, Collections.emptyList(), Arrays.asList(monitoring, template), rules1, null, RefreshPolicy.NONE);
        final PutRoleMappingResponse putRoleMappingResponse1 = client.security().putRoleMapping(putRoleMappingRequest1, RequestOptions.DEFAULT);
        boolean isCreated1 = putRoleMappingResponse1.isCreated();
        assertTrue(isCreated1);
        final RoleMapperExpression rules2 = AnyRoleMapperExpression.builder().addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com")).build();
        final Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("k1", "v1");
        final PutRoleMappingRequest putRoleMappingRequest2 = new PutRoleMappingRequest("mapping-example-2", true, Arrays.asList("superuser"), Collections.emptyList(), rules2, metadata2, RefreshPolicy.NONE);
        final PutRoleMappingResponse putRoleMappingResponse2 = client.security().putRoleMapping(putRoleMappingRequest2, RequestOptions.DEFAULT);
        boolean isCreated2 = putRoleMappingResponse2.isCreated();
        assertTrue(isCreated2);
        {
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest("mapping-example-1");
            final GetRoleMappingsResponse response = client.security().getRoleMappings(request, RequestOptions.DEFAULT);
            List<ExpressionRoleMapping> mappings = response.getMappings();
            assertNotNull(mappings);
            assertThat(mappings.size(), is(1));
            assertThat(mappings.get(0).isEnabled(), is(true));
            assertThat(mappings.get(0).getName(), is("mapping-example-1"));
            assertThat(mappings.get(0).getExpression(), equalTo(rules1));
            assertThat(mappings.get(0).getMetadata(), equalTo(Collections.emptyMap()));
            assertThat(mappings.get(0).getRoles(), iterableWithSize(0));
            assertThat(mappings.get(0).getRoleTemplates(), iterableWithSize(2));
            assertThat(mappings.get(0).getRoleTemplates(), containsInAnyOrder(monitoring, template));
        }
        {
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest("mapping-example-1", "mapping-example-2");
            final GetRoleMappingsResponse response = client.security().getRoleMappings(request, RequestOptions.DEFAULT);
            List<ExpressionRoleMapping> mappings = response.getMappings();
            assertNotNull(mappings);
            assertThat(mappings.size(), is(2));
            for (ExpressionRoleMapping roleMapping : mappings) {
                assertThat(roleMapping.isEnabled(), is(true));
                assertThat(roleMapping.getName(), isIn(new String[] { "mapping-example-1", "mapping-example-2" }));
                if (roleMapping.getName().equals("mapping-example-1")) {
                    assertThat(roleMapping.getMetadata(), equalTo(Collections.emptyMap()));
                    assertThat(roleMapping.getExpression(), equalTo(rules1));
                    assertThat(roleMapping.getRoles(), emptyIterable());
                    assertThat(roleMapping.getRoleTemplates(), contains(monitoring, template));
                } else {
                    assertThat(roleMapping.getMetadata(), equalTo(metadata2));
                    assertThat(roleMapping.getExpression(), equalTo(rules2));
                    assertThat(roleMapping.getRoles(), contains("superuser"));
                    assertThat(roleMapping.getRoleTemplates(), emptyIterable());
                }
            }
        }
        {
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest();
            final GetRoleMappingsResponse response = client.security().getRoleMappings(request, RequestOptions.DEFAULT);
            List<ExpressionRoleMapping> mappings = response.getMappings();
            assertNotNull(mappings);
            assertThat(mappings.size(), is(2));
            for (ExpressionRoleMapping roleMapping : mappings) {
                assertThat(roleMapping.isEnabled(), is(true));
                assertThat(roleMapping.getName(), isIn(new String[] { "mapping-example-1", "mapping-example-2" }));
                if (roleMapping.getName().equals("mapping-example-1")) {
                    assertThat(roleMapping.getMetadata(), equalTo(Collections.emptyMap()));
                    assertThat(roleMapping.getExpression(), equalTo(rules1));
                    assertThat(roleMapping.getRoles(), emptyIterable());
                    assertThat(roleMapping.getRoleTemplates(), containsInAnyOrder(monitoring, template));
                } else {
                    assertThat(roleMapping.getMetadata(), equalTo(metadata2));
                    assertThat(roleMapping.getExpression(), equalTo(rules2));
                    assertThat(roleMapping.getRoles(), contains("superuser"));
                    assertThat(roleMapping.getRoleTemplates(), emptyIterable());
                }
            }
        }
        {
            final GetRoleMappingsRequest request = new GetRoleMappingsRequest();
            ActionListener<GetRoleMappingsResponse> listener = new ActionListener<GetRoleMappingsResponse>() {

                @Override
                public void onResponse(GetRoleMappingsResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().getRoleMappingsAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testEnableUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
        User enable_user = new User("enable_user", Collections.singletonList("superuser"));
        PutUserRequest putUserRequest = PutUserRequest.withPassword(enable_user, password, true, RefreshPolicy.IMMEDIATE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());
        {
            EnableUserRequest request = new EnableUserRequest("enable_user", RefreshPolicy.NONE);
            boolean response = client.security().enableUser(request, RequestOptions.DEFAULT);
            assertTrue(response);
        }
        {
            EnableUserRequest request = new EnableUserRequest("enable_user", RefreshPolicy.NONE);
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {

                @Override
                public void onResponse(Boolean response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().enableUserAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDisableUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
        User disable_user = new User("disable_user", Collections.singletonList("superuser"));
        PutUserRequest putUserRequest = PutUserRequest.withPassword(disable_user, password, true, RefreshPolicy.IMMEDIATE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());
        {
            DisableUserRequest request = new DisableUserRequest("disable_user", RefreshPolicy.NONE);
            boolean response = client.security().disableUser(request, RequestOptions.DEFAULT);
            assertTrue(response);
        }
        {
            DisableUserRequest request = new DisableUserRequest("disable_user", RefreshPolicy.NONE);
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {

                @Override
                public void onResponse(Boolean response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().disableUserAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetRoles() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        addRole("my_role");
        addRole("my_role2");
        addRole("my_role3");
        {
            GetRolesRequest request = new GetRolesRequest("my_role");
            GetRolesResponse response = client.security().getRoles(request, RequestOptions.DEFAULT);
            List<Role> roles = response.getRoles();
            assertNotNull(response);
            assertThat(roles.size(), equalTo(1));
            assertThat(roles.get(0).getName(), equalTo("my_role"));
            assertThat(roles.get(0).getClusterPrivileges().contains("all"), equalTo(true));
        }
        {
            GetRolesRequest request = new GetRolesRequest("my_role", "my_role2");
            GetRolesResponse response = client.security().getRoles(request, RequestOptions.DEFAULT);
            List<Role> roles = response.getRoles();
            assertNotNull(response);
            assertThat(roles.size(), equalTo(2));
            assertThat(roles.get(0).getClusterPrivileges().contains("all"), equalTo(true));
            assertThat(roles.get(1).getClusterPrivileges().contains("all"), equalTo(true));
        }
        {
            GetRolesRequest request = new GetRolesRequest();
            GetRolesResponse response = client.security().getRoles(request, RequestOptions.DEFAULT);
            List<Role> roles = response.getRoles();
            assertNotNull(response);
            assertThat(roles.size(), equalTo(30));
        }
        {
            GetRolesRequest request = new GetRolesRequest("my_role");
            ActionListener<GetRolesResponse> listener;
            listener = new ActionListener<GetRolesResponse>() {

                @Override
                public void onResponse(GetRolesResponse getRolesResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<GetRolesResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().getRolesAsync(request, RequestOptions.DEFAULT, listener);
            final GetRolesResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertThat(response.getRoles().size(), equalTo(1));
            assertThat(response.getRoles().get(0).getName(), equalTo("my_role"));
            assertThat(response.getRoles().get(0).getClusterPrivileges().contains("all"), equalTo(true));
        }
    }

    public void testAuthenticate() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            AuthenticateResponse response = client.security().authenticate(RequestOptions.DEFAULT);
            User user = response.getUser();
            boolean enabled = response.enabled();
            final String authenticationRealmName = response.getAuthenticationRealm().getName();
            final String authenticationRealmType = response.getAuthenticationRealm().getType();
            final String lookupRealmName = response.getLookupRealm().getName();
            final String lookupRealmType = response.getLookupRealm().getType();
            assertThat(user.getUsername(), is("test_user"));
            assertThat(user.getRoles(), contains(new String[] { "superuser" }));
            assertThat(user.getFullName(), nullValue());
            assertThat(user.getEmail(), nullValue());
            assertThat(user.getMetadata().isEmpty(), is(true));
            assertThat(enabled, is(true));
            assertThat(authenticationRealmName, is("default_file"));
            assertThat(authenticationRealmType, is("file"));
            assertThat(lookupRealmName, is("default_file"));
            assertThat(lookupRealmType, is("file"));
        }
        {
            ActionListener<AuthenticateResponse> listener = new ActionListener<AuthenticateResponse>() {

                @Override
                public void onResponse(AuthenticateResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().authenticateAsync(RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testHasPrivileges() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            HasPrivilegesRequest request = new HasPrivilegesRequest(Sets.newHashSet("monitor", "manage"), Sets.newHashSet(IndicesPrivileges.builder().indices("logstash-2018-10-05").privileges("read", "write").allowRestrictedIndices(false).build(), IndicesPrivileges.builder().indices("logstash-2018-*").privileges("read").allowRestrictedIndices(true).build()), null);
            HasPrivilegesResponse response = client.security().hasPrivileges(request, RequestOptions.DEFAULT);
            boolean hasMonitor = response.hasClusterPrivilege("monitor");
            boolean hasWrite = response.hasIndexPrivilege("logstash-2018-10-05", "write");
            boolean hasRead = response.hasIndexPrivilege("logstash-2018-*", "read");
            assertThat(response.getUsername(), is("test_user"));
            assertThat(response.hasAllRequested(), is(true));
            assertThat(hasMonitor, is(true));
            assertThat(hasWrite, is(true));
            assertThat(hasRead, is(true));
            assertThat(response.getApplicationPrivileges().entrySet(), emptyIterable());
        }
        {
            HasPrivilegesRequest request = new HasPrivilegesRequest(Collections.singleton("monitor"), null, null);
            ActionListener<HasPrivilegesResponse> listener = new ActionListener<HasPrivilegesResponse>() {

                @Override
                public void onResponse(HasPrivilegesResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().hasPrivilegesAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetUserPrivileges() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            GetUserPrivilegesResponse response = client.security().getUserPrivileges(RequestOptions.DEFAULT);
            assertNotNull(response);
            final Set<String> cluster = response.getClusterPrivileges();
            final Set<UserIndicesPrivileges> index = response.getIndicesPrivileges();
            final Set<ApplicationResourcePrivileges> application = response.getApplicationPrivileges();
            final Set<String> runAs = response.getRunAsPrivilege();
            assertNotNull(cluster);
            assertThat(cluster, contains("all"));
            assertNotNull(index);
            assertThat(index.size(), is(1));
            final UserIndicesPrivileges indexPrivilege = index.iterator().next();
            assertThat(indexPrivilege.getIndices(), contains("*"));
            assertThat(indexPrivilege.getPrivileges(), contains("all"));
            assertThat(indexPrivilege.getFieldSecurity().size(), is(0));
            assertThat(indexPrivilege.getQueries().size(), is(0));
            assertNotNull(application);
            assertThat(application.size(), is(1));
            assertNotNull(runAs);
            assertThat(runAs, contains("*"));
        }
        {
            ActionListener<GetUserPrivilegesResponse> listener = new ActionListener<GetUserPrivilegesResponse>() {

                @Override
                public void onResponse(GetUserPrivilegesResponse getUserPrivilegesResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().getUserPrivilegesAsync(RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testClearRealmCache() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            ClearRealmCacheRequest request = new ClearRealmCacheRequest(Collections.emptyList(), Collections.emptyList());
            ClearRealmCacheResponse response = client.security().clearRealmCache(request, RequestOptions.DEFAULT);
            assertNotNull(response);
            assertThat(response.getNodes(), not(empty()));
            List<ClearRealmCacheResponse.Node> nodes = response.getNodes();
        }
        {
            ClearRealmCacheRequest request = new ClearRealmCacheRequest(Collections.emptyList(), Collections.emptyList());
            ActionListener<ClearRealmCacheResponse> listener = new ActionListener<ClearRealmCacheResponse>() {

                @Override
                public void onResponse(ClearRealmCacheResponse clearRealmCacheResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().clearRealmCacheAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testClearRolesCache() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            ClearRolesCacheRequest request = new ClearRolesCacheRequest("my_role");
            ClearRolesCacheResponse response = client.security().clearRolesCache(request, RequestOptions.DEFAULT);
            assertNotNull(response);
            assertThat(response.getNodes(), not(empty()));
            List<ClearRolesCacheResponse.Node> nodes = response.getNodes();
        }
        {
            ClearRolesCacheRequest request = new ClearRolesCacheRequest("my_role");
            ActionListener<ClearRolesCacheResponse> listener = new ActionListener<ClearRolesCacheResponse>() {

                @Override
                public void onResponse(ClearRolesCacheResponse clearRolesCacheResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().clearRolesCacheAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetSslCertificates() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            GetSslCertificatesResponse response = client.security().getSslCertificates(RequestOptions.DEFAULT);
            assertNotNull(response);
            List<CertificateInfo> certificates = response.getCertificates();
            assertThat(certificates.size(), Matchers.equalTo(9));
            final Iterator<CertificateInfo> it = certificates.iterator();
            CertificateInfo c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("c0ea4216e8ff0fd8"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("b8b96c37e332cccb"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.crt"));
            assertThat(c.getFormat(), Matchers.equalTo("PEM"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("d3850b2b1995ad5f"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("b8b96c37e332cccb"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("b9d497f2924bbe29"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("580db8ad52bb168a4080e1df122a3f56"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("7268203b"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("3151a81eec8d4e34c56a8466a8510bcfbe63cc31"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSerialNumber(), Matchers.equalTo("223c736a"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
        }
        {
            ActionListener<GetSslCertificatesResponse> listener = new ActionListener<GetSslCertificatesResponse>() {

                @Override
                public void onResponse(GetSslCertificatesResponse getSslCertificatesResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().getSslCertificatesAsync(RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testChangePassword() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
        char[] newPassword = new char[] { 'n', 'e', 'w', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
        User user = new User("change_password_user", Collections.singletonList("superuser"), Collections.emptyMap(), null, null);
        PutUserRequest putUserRequest = PutUserRequest.withPassword(user, password, true, RefreshPolicy.NONE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());
        {
            ChangePasswordRequest request = new ChangePasswordRequest("change_password_user", newPassword, RefreshPolicy.NONE);
            boolean response = client.security().changePassword(request, RequestOptions.DEFAULT);
            assertTrue(response);
        }
        {
            ChangePasswordRequest request = new ChangePasswordRequest("change_password_user", password, RefreshPolicy.NONE);
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {

                @Override
                public void onResponse(Boolean response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().changePasswordAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteRoleMapping() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        {
            final RoleMapperExpression rules = FieldRoleMapperExpression.ofUsername("*");
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true, Collections.singletonList("superuser"), Collections.emptyList(), rules, null, RefreshPolicy.NONE);
            final PutRoleMappingResponse response = client.security().putRoleMapping(request, RequestOptions.DEFAULT);
            boolean isCreated = response.isCreated();
            assertTrue(isCreated);
        }
        {
            final DeleteRoleMappingRequest request = new DeleteRoleMappingRequest("mapping-example", RefreshPolicy.NONE);
            final DeleteRoleMappingResponse response = client.security().deleteRoleMapping(request, RequestOptions.DEFAULT);
            boolean isFound = response.isFound();
            assertTrue(isFound);
        }
        {
            final DeleteRoleMappingRequest request = new DeleteRoleMappingRequest("mapping-example", RefreshPolicy.NONE);
            ActionListener<DeleteRoleMappingResponse> listener = new ActionListener<DeleteRoleMappingResponse>() {

                @Override
                public void onResponse(DeleteRoleMappingResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().deleteRoleMappingAsync(request, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteRole() throws Exception {
        RestHighLevelClient client = highLevelClient();
        addRole("testrole");
        {
            DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest("testrole");
            DeleteRoleResponse deleteRoleResponse = client.security().deleteRole(deleteRoleRequest, RequestOptions.DEFAULT);
            boolean found = deleteRoleResponse.isFound();
            assertTrue(found);
            deleteRoleResponse = client.security().deleteRole(deleteRoleRequest, RequestOptions.DEFAULT);
            assertFalse(deleteRoleResponse.isFound());
        }
        {
            DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest("testrole");
            ActionListener<DeleteRoleResponse> listener;
            listener = new ActionListener<DeleteRoleResponse>() {

                @Override
                public void onResponse(DeleteRoleResponse deleteRoleResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().deleteRoleAsync(deleteRoleRequest, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutRole() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            final Role role = Role.builder().name("testPutRole").clusterPrivileges(randomSubsetOf(1, Role.ClusterPrivilegeName.ALL_ARRAY)).build();
            final PutRoleRequest request = new PutRoleRequest(role, RefreshPolicy.NONE);
            final PutRoleResponse response = client.security().putRole(request, RequestOptions.DEFAULT);
            boolean isCreated = response.isCreated();
            assertTrue(isCreated);
        }
        {
            final Role role = Role.builder().name("testPutRole").clusterPrivileges(randomSubsetOf(1, Role.ClusterPrivilegeName.ALL_ARRAY)).build();
            final PutRoleRequest request = new PutRoleRequest(role, RefreshPolicy.NONE);
            ActionListener<PutRoleResponse> listener = new ActionListener<PutRoleResponse>() {

                @Override
                public void onResponse(PutRoleResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<PutRoleResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().putRoleAsync(request, RequestOptions.DEFAULT, listener);
            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertThat(future.get().isCreated(), is(false));
        }
    }

    private void addRole(String roleName) throws IOException {
        final Role role = Role.builder().name(roleName).clusterPrivileges("all").build();
        final PutRoleRequest request = new PutRoleRequest(role, RefreshPolicy.IMMEDIATE);
        highLevelClient().security().putRole(request, RequestOptions.DEFAULT);
    }

    public void testCreateToken() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            User token_user = new User("token_user", Collections.singletonList("kibana_user"));
            PutUserRequest putUserRequest = PutUserRequest.withPassword(token_user, "password".toCharArray(), true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
            assertTrue(putUserResponse.isCreated());
        }
        {
            final char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
            CreateTokenRequest createTokenRequest = CreateTokenRequest.passwordGrant("token_user", password);
            CreateTokenResponse createTokenResponse = client.security().createToken(createTokenRequest, RequestOptions.DEFAULT);
            String accessToken = createTokenResponse.getAccessToken();
            String refreshToken = createTokenResponse.getRefreshToken();
            assertNotNull(accessToken);
            assertNotNull(refreshToken);
            assertNotNull(createTokenResponse.getExpiresIn());
            createTokenRequest = CreateTokenRequest.refreshTokenGrant(refreshToken);
            CreateTokenResponse refreshResponse = client.security().createToken(createTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(refreshResponse.getAccessToken());
            assertNotNull(refreshResponse.getRefreshToken());
        }
        {
            CreateTokenRequest createTokenRequest = CreateTokenRequest.clientCredentialsGrant();
            ActionListener<CreateTokenResponse> listener;
            listener = new ActionListener<CreateTokenResponse>() {

                @Override
                public void onResponse(CreateTokenResponse createTokenResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<CreateTokenResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().createTokenAsync(createTokenRequest, RequestOptions.DEFAULT, listener);
            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertNotNull(future.get().getAccessToken());
            assertNull(future.get().getRefreshToken());
        }
    }

    public void testInvalidateToken() throws Exception {
        RestHighLevelClient client = highLevelClient();
        String accessToken;
        String refreshToken;
        {
            final char[] password = "password".toCharArray();
            User user = new User("user", Collections.singletonList("kibana_user"));
            PutUserRequest putUserRequest = PutUserRequest.withPassword(user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
            assertTrue(putUserResponse.isCreated());
            User this_user = new User("this_user", Collections.singletonList("kibana_user"));
            PutUserRequest putThisUserRequest = PutUserRequest.withPassword(this_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putThisUserResponse = client.security().putUser(putThisUserRequest, RequestOptions.DEFAULT);
            assertTrue(putThisUserResponse.isCreated());
            User that_user = new User("that_user", Collections.singletonList("kibana_user"));
            PutUserRequest putThatUserRequest = PutUserRequest.withPassword(that_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putThatUserResponse = client.security().putUser(putThatUserRequest, RequestOptions.DEFAULT);
            assertTrue(putThatUserResponse.isCreated());
            User other_user = new User("other_user", Collections.singletonList("kibana_user"));
            PutUserRequest putOtherUserRequest = PutUserRequest.withPassword(other_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putOtherUserResponse = client.security().putUser(putOtherUserRequest, RequestOptions.DEFAULT);
            assertTrue(putOtherUserResponse.isCreated());
            User extra_user = new User("extra_user", Collections.singletonList("kibana_user"));
            PutUserRequest putExtraUserRequest = PutUserRequest.withPassword(extra_user, password, true, RefreshPolicy.IMMEDIATE);
            PutUserResponse putExtraUserResponse = client.security().putUser(putExtraUserRequest, RequestOptions.DEFAULT);
            assertTrue(putExtraUserResponse.isCreated());
            final CreateTokenRequest createTokenRequest = CreateTokenRequest.passwordGrant("user", password);
            final CreateTokenResponse tokenResponse = client.security().createToken(createTokenRequest, RequestOptions.DEFAULT);
            accessToken = tokenResponse.getAccessToken();
            refreshToken = tokenResponse.getRefreshToken();
            final CreateTokenRequest createThisTokenRequest = CreateTokenRequest.passwordGrant("this_user", password);
            final CreateTokenResponse thisTokenResponse = client.security().createToken(createThisTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(thisTokenResponse);
            final CreateTokenRequest createThatTokenRequest = CreateTokenRequest.passwordGrant("that_user", password);
            final CreateTokenResponse thatTokenResponse = client.security().createToken(createThatTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(thatTokenResponse);
            final CreateTokenRequest createOtherTokenRequest = CreateTokenRequest.passwordGrant("other_user", password);
            final CreateTokenResponse otherTokenResponse = client.security().createToken(createOtherTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(otherTokenResponse);
            final CreateTokenRequest createExtraTokenRequest = CreateTokenRequest.passwordGrant("extra_user", password);
            final CreateTokenResponse extraTokenResponse = client.security().createToken(createExtraTokenRequest, RequestOptions.DEFAULT);
            assertNotNull(extraTokenResponse);
        }
        {
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.accessToken(accessToken);
            InvalidateTokenResponse invalidateTokenResponse = client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            final List<ElasticsearchException> errors = invalidateTokenResponse.getErrors();
            final int invalidatedTokens = invalidateTokenResponse.getInvalidatedTokens();
            final int previouslyInvalidatedTokens = invalidateTokenResponse.getPreviouslyInvalidatedTokens();
            assertTrue(errors.isEmpty());
            assertThat(invalidatedTokens, equalTo(1));
            assertThat(previouslyInvalidatedTokens, equalTo(0));
        }
        {
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.refreshToken(refreshToken);
            InvalidateTokenResponse invalidateTokenResponse = client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            assertTrue(invalidateTokenResponse.getErrors().isEmpty());
            assertThat(invalidateTokenResponse.getInvalidatedTokens(), equalTo(1));
            assertThat(invalidateTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        }
        {
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.userTokens("other_user");
            InvalidateTokenResponse invalidateTokenResponse = client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            assertTrue(invalidateTokenResponse.getErrors().isEmpty());
            assertThat(invalidateTokenResponse.getInvalidatedTokens(), equalTo(2));
            assertThat(invalidateTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        }
        {
            InvalidateTokenRequest invalidateTokenRequest = new InvalidateTokenRequest(null, null, "default_native", "extra_user");
            InvalidateTokenResponse invalidateTokenResponse = client.security().invalidateToken(invalidateTokenRequest, RequestOptions.DEFAULT);
            assertTrue(invalidateTokenResponse.getErrors().isEmpty());
            assertThat(invalidateTokenResponse.getInvalidatedTokens(), equalTo(2));
            assertThat(invalidateTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        }
        {
            InvalidateTokenRequest invalidateTokenRequest = InvalidateTokenRequest.realmTokens("default_native");
            ActionListener<InvalidateTokenResponse> listener;
            listener = new ActionListener<InvalidateTokenResponse>() {

                @Override
                public void onResponse(InvalidateTokenResponse invalidateTokenResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<InvalidateTokenResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().invalidateTokenAsync(invalidateTokenRequest, RequestOptions.DEFAULT, listener);
            final InvalidateTokenResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertTrue(response.getErrors().isEmpty());
            assertThat(response.getInvalidatedTokens(), equalTo(4));
            assertThat(response.getPreviouslyInvalidatedTokens(), equalTo(0));
        }
    }

    public void testGetPrivileges() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        final ApplicationPrivilege readTestappPrivilege = new ApplicationPrivilege("testapp", "read", Arrays.asList("action:login", "data:read/*"), null);
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        final ApplicationPrivilege writeTestappPrivilege = new ApplicationPrivilege("testapp", "write", Arrays.asList("action:login", "data:write/*"), metadata);
        final ApplicationPrivilege allTestappPrivilege = new ApplicationPrivilege("testapp", "all", Arrays.asList("action:login", "data:write/*", "manage:*"), null);
        final Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("key2", "value2");
        final ApplicationPrivilege readTestapp2Privilege = new ApplicationPrivilege("testapp2", "read", Arrays.asList("action:login", "data:read/*"), metadata2);
        final ApplicationPrivilege writeTestapp2Privilege = new ApplicationPrivilege("testapp2", "write", Arrays.asList("action:login", "data:write/*"), null);
        final ApplicationPrivilege allTestapp2Privilege = new ApplicationPrivilege("testapp2", "all", Arrays.asList("action:login", "data:write/*", "manage:*"), null);
        {
            List<ApplicationPrivilege> applicationPrivileges = new ArrayList<>();
            applicationPrivileges.add(readTestappPrivilege);
            applicationPrivileges.add(writeTestappPrivilege);
            applicationPrivileges.add(allTestappPrivilege);
            applicationPrivileges.add(readTestapp2Privilege);
            applicationPrivileges.add(writeTestapp2Privilege);
            applicationPrivileges.add(allTestapp2Privilege);
            PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(applicationPrivileges, RefreshPolicy.IMMEDIATE);
            PutPrivilegesResponse putPrivilegesResponse = client.security().putPrivileges(putPrivilegesRequest, RequestOptions.DEFAULT);
            assertNotNull(putPrivilegesResponse);
            assertThat(putPrivilegesResponse.wasCreated("testapp", "write"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "read"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "all"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp2", "all"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp2", "write"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp2", "read"), is(true));
        }
        {
            GetPrivilegesRequest request = new GetPrivilegesRequest("testapp", "write");
            GetPrivilegesResponse response = client.security().getPrivileges(request, RequestOptions.DEFAULT);
            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(1));
            assertThat(response.getPrivileges().contains(writeTestappPrivilege), equalTo(true));
        }
        {
            GetPrivilegesRequest request = GetPrivilegesRequest.getApplicationPrivileges("testapp");
            GetPrivilegesResponse response = client.security().getPrivileges(request, RequestOptions.DEFAULT);
            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(3));
            final GetPrivilegesResponse exptectedResponse = new GetPrivilegesResponse(Arrays.asList(readTestappPrivilege, writeTestappPrivilege, allTestappPrivilege));
            assertThat(response, equalTo(exptectedResponse));
            Set<ApplicationPrivilege> privileges = response.getPrivileges();
            for (ApplicationPrivilege privilege : privileges) {
                assertThat(privilege.getApplication(), equalTo("testapp"));
                if (privilege.getName().equals("read")) {
                    assertThat(privilege.getActions(), containsInAnyOrder("action:login", "data:read/*"));
                    assertThat(privilege.getMetadata().isEmpty(), equalTo(true));
                } else if (privilege.getName().equals("write")) {
                    assertThat(privilege.getActions(), containsInAnyOrder("action:login", "data:write/*"));
                    assertThat(privilege.getMetadata().isEmpty(), equalTo(false));
                    assertThat(privilege.getMetadata().get("key1"), equalTo("value1"));
                } else if (privilege.getName().equals("all")) {
                    assertThat(privilege.getActions(), containsInAnyOrder("action:login", "data:write/*", "manage:*"));
                    assertThat(privilege.getMetadata().isEmpty(), equalTo(true));
                }
            }
        }
        {
            GetPrivilegesRequest request = GetPrivilegesRequest.getAllPrivileges();
            GetPrivilegesResponse response = client.security().getPrivileges(request, RequestOptions.DEFAULT);
            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(6));
            final GetPrivilegesResponse exptectedResponse = new GetPrivilegesResponse(Arrays.asList(readTestappPrivilege, writeTestappPrivilege, allTestappPrivilege, readTestapp2Privilege, writeTestapp2Privilege, allTestapp2Privilege));
            assertThat(response, equalTo(exptectedResponse));
        }
        {
            GetPrivilegesRequest request = new GetPrivilegesRequest("testapp", "read");
            ActionListener<GetPrivilegesResponse> listener = new ActionListener<GetPrivilegesResponse>() {

                @Override
                public void onResponse(GetPrivilegesResponse getPrivilegesResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<GetPrivilegesResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().getPrivilegesAsync(request, RequestOptions.DEFAULT, listener);
            final GetPrivilegesResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertThat(response.getPrivileges().size(), equalTo(1));
            assertThat(response.getPrivileges().contains(readTestappPrivilege), equalTo(true));
        }
    }

    public void testPutPrivileges() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            final List<ApplicationPrivilege> privileges = new ArrayList<>();
            privileges.add(ApplicationPrivilege.builder().application("app01").privilege("all").actions(List.of("action:login")).metadata(Collections.singletonMap("k1", "v1")).build());
            privileges.add(ApplicationPrivilege.builder().application("app01").privilege("write").actions(List.of("action:write")).build());
            final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, RefreshPolicy.IMMEDIATE);
            final PutPrivilegesResponse putPrivilegesResponse = client.security().putPrivileges(putPrivilegesRequest, RequestOptions.DEFAULT);
            final String applicationName = "app01";
            final String privilegeName = "all";
            final boolean status = putPrivilegesResponse.wasCreated(applicationName, privilegeName);
            assertThat(status, is(true));
        }
        {
            final List<ApplicationPrivilege> privileges = new ArrayList<>();
            privileges.add(ApplicationPrivilege.builder().application("app01").privilege("all").actions(List.of("action:login")).metadata(Collections.singletonMap("k1", "v1")).build());
            final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, RefreshPolicy.IMMEDIATE);
            ActionListener<PutPrivilegesResponse> listener = new ActionListener<PutPrivilegesResponse>() {

                @Override
                public void onResponse(PutPrivilegesResponse response) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<PutPrivilegesResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().putPrivilegesAsync(putPrivilegesRequest, RequestOptions.DEFAULT, listener);
            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertThat(future.get().wasCreated("app01", "all"), is(false));
        }
    }

    public void testDeletePrivilege() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            List<ApplicationPrivilege> applicationPrivileges = new ArrayList<>();
            applicationPrivileges.add(ApplicationPrivilege.builder().application("testapp").privilege("read").actions("action:login", "data:read/*").build());
            applicationPrivileges.add(ApplicationPrivilege.builder().application("testapp").privilege("write").actions("action:login", "data:write/*").build());
            applicationPrivileges.add(ApplicationPrivilege.builder().application("testapp").privilege("all").actions("action:login", "data:write/*").build());
            PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(applicationPrivileges, RefreshPolicy.IMMEDIATE);
            PutPrivilegesResponse putPrivilegesResponse = client.security().putPrivileges(putPrivilegesRequest, RequestOptions.DEFAULT);
            assertNotNull(putPrivilegesResponse);
            assertThat(putPrivilegesResponse.wasCreated("testapp", "write"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "read"), is(true));
            assertThat(putPrivilegesResponse.wasCreated("testapp", "all"), is(true));
        }
        {
            DeletePrivilegesRequest request = new DeletePrivilegesRequest("testapp", "read", "write");
            DeletePrivilegesResponse response = client.security().deletePrivileges(request, RequestOptions.DEFAULT);
            String application = response.getApplication();
            boolean found = response.isFound("read");
            assertThat(application, equalTo("testapp"));
            assertTrue(response.isFound("write"));
            assertTrue(found);
            response = client.security().deletePrivileges(request, RequestOptions.DEFAULT);
            assertFalse(response.isFound("write"));
        }
        {
            DeletePrivilegesRequest deletePrivilegesRequest = new DeletePrivilegesRequest("testapp", "all");
            ActionListener<DeletePrivilegesResponse> listener;
            listener = new ActionListener<DeletePrivilegesResponse>() {

                @Override
                public void onResponse(DeletePrivilegesResponse deletePrivilegesResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);
            client.security().deletePrivilegesAsync(deletePrivilegesRequest, RequestOptions.DEFAULT, listener);
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testCreateApiKey() throws Exception {
        RestHighLevelClient client = highLevelClient();
        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL).indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        {
            final String name = randomAlphaOfLength(5);
            CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy);
            CreateApiKeyResponse createApiKeyResponse = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            SecureString apiKey = createApiKeyResponse.getKey();
            Instant apiKeyExpiration = createApiKeyResponse.getExpiration();
            assertThat(createApiKeyResponse.getName(), equalTo(name));
            assertNotNull(apiKey);
            assertNotNull(apiKeyExpiration);
        }
        {
            final String name = randomAlphaOfLength(5);
            CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy);
            ActionListener<CreateApiKeyResponse> listener;
            listener = new ActionListener<CreateApiKeyResponse>() {

                @Override
                public void onResponse(CreateApiKeyResponse createApiKeyResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().createApiKeyAsync(createApiKeyRequest, RequestOptions.DEFAULT, listener);
            assertNotNull(future.get(30, TimeUnit.SECONDS));
            assertThat(future.get().getName(), equalTo(name));
            assertNotNull(future.get().getKey());
            assertNotNull(future.get().getExpiration());
        }
    }

    public void testGetApiKey() throws Exception {
        RestHighLevelClient client = highLevelClient();
        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL).indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("k1", roles, expiration, refreshPolicy);
        CreateApiKeyResponse createApiKeyResponse1 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
        assertThat(createApiKeyResponse1.getName(), equalTo("k1"));
        assertNotNull(createApiKeyResponse1.getKey());
        final ApiKey expectedApiKeyInfo = new ApiKey(createApiKeyResponse1.getName(), createApiKeyResponse1.getId(), Instant.now(), Instant.now().plusMillis(expiration.getMillis()), false, "test_user", "default_file");
        {
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(createApiKeyResponse1.getId());
            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }
        {
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyName(createApiKeyResponse1.getName());
            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }
        {
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingRealmName("default_file");
            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }
        {
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingUserName("test_user");
            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }
        {
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingRealmAndUserName("default_file", "test_user");
            GetApiKeyResponse getApiKeyResponse = client.security().getApiKey(getApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(getApiKeyResponse.getApiKeyInfos(), is(notNullValue()));
            assertThat(getApiKeyResponse.getApiKeyInfos().size(), is(1));
            verifyApiKey(getApiKeyResponse.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }
        {
            GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(createApiKeyResponse1.getId());
            ActionListener<GetApiKeyResponse> listener;
            listener = new ActionListener<GetApiKeyResponse>() {

                @Override
                public void onResponse(GetApiKeyResponse getApiKeyResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<GetApiKeyResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().getApiKeyAsync(getApiKeyRequest, RequestOptions.DEFAULT, listener);
            final GetApiKeyResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            assertThat(response.getApiKeyInfos(), is(notNullValue()));
            assertThat(response.getApiKeyInfos().size(), is(1));
            verifyApiKey(response.getApiKeyInfos().get(0), expectedApiKeyInfo);
        }
    }

    private void verifyApiKey(final ApiKey actual, final ApiKey expected) {
        assertThat(actual.getId(), is(expected.getId()));
        assertThat(actual.getName(), is(expected.getName()));
        assertThat(actual.getUsername(), is(expected.getUsername()));
        assertThat(actual.getRealm(), is(expected.getRealm()));
        assertThat(actual.isInvalidated(), is(expected.isInvalidated()));
        assertThat(actual.getExpiration(), is(greaterThan(Instant.now())));
    }

    public void testInvalidateApiKey() throws Exception {
        RestHighLevelClient client = highLevelClient();
        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL).indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = TimeValue.timeValueHours(24);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("k1", roles, expiration, refreshPolicy);
        CreateApiKeyResponse createApiKeyResponse1 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
        assertThat(createApiKeyResponse1.getName(), equalTo("k1"));
        assertNotNull(createApiKeyResponse1.getKey());
        {
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(createApiKeyResponse1.getId());
            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest, RequestOptions.DEFAULT);
            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();
            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse1.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }
        {
            createApiKeyRequest = new CreateApiKeyRequest("k2", roles, expiration, refreshPolicy);
            CreateApiKeyResponse createApiKeyResponse2 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse2.getName(), equalTo("k2"));
            assertNotNull(createApiKeyResponse2.getKey());
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyName(createApiKeyResponse2.getName());
            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest, RequestOptions.DEFAULT);
            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();
            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse2.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }
        {
            createApiKeyRequest = new CreateApiKeyRequest("k3", roles, expiration, refreshPolicy);
            CreateApiKeyResponse createApiKeyResponse3 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse3.getName(), equalTo("k3"));
            assertNotNull(createApiKeyResponse3.getKey());
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmName("default_file");
            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest, RequestOptions.DEFAULT);
            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();
            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse3.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }
        {
            createApiKeyRequest = new CreateApiKeyRequest("k4", roles, expiration, refreshPolicy);
            CreateApiKeyResponse createApiKeyResponse4 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse4.getName(), equalTo("k4"));
            assertNotNull(createApiKeyResponse4.getKey());
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingUserName("test_user");
            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest, RequestOptions.DEFAULT);
            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();
            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse4.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }
        {
            createApiKeyRequest = new CreateApiKeyRequest("k5", roles, expiration, refreshPolicy);
            CreateApiKeyResponse createApiKeyResponse5 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse5.getName(), equalTo("k5"));
            assertNotNull(createApiKeyResponse5.getKey());
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingRealmAndUserName("default_file", "test_user");
            InvalidateApiKeyResponse invalidateApiKeyResponse = client.security().invalidateApiKey(invalidateApiKeyRequest, RequestOptions.DEFAULT);
            final List<ElasticsearchException> errors = invalidateApiKeyResponse.getErrors();
            final List<String> invalidatedApiKeyIds = invalidateApiKeyResponse.getInvalidatedApiKeys();
            final List<String> previouslyInvalidatedApiKeyIds = invalidateApiKeyResponse.getPreviouslyInvalidatedApiKeys();
            assertTrue(errors.isEmpty());
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse5.getId());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(previouslyInvalidatedApiKeyIds.size(), equalTo(0));
        }
        {
            createApiKeyRequest = new CreateApiKeyRequest("k6", roles, expiration, refreshPolicy);
            CreateApiKeyResponse createApiKeyResponse6 = client.security().createApiKey(createApiKeyRequest, RequestOptions.DEFAULT);
            assertThat(createApiKeyResponse6.getName(), equalTo("k6"));
            assertNotNull(createApiKeyResponse6.getKey());
            InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(createApiKeyResponse6.getId());
            ActionListener<InvalidateApiKeyResponse> listener;
            listener = new ActionListener<InvalidateApiKeyResponse>() {

                @Override
                public void onResponse(InvalidateApiKeyResponse invalidateApiKeyResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };
            assertNotNull(listener);
            final PlainActionFuture<InvalidateApiKeyResponse> future = new PlainActionFuture<>();
            listener = future;
            client.security().invalidateApiKeyAsync(invalidateApiKeyRequest, RequestOptions.DEFAULT, listener);
            final InvalidateApiKeyResponse response = future.get(30, TimeUnit.SECONDS);
            assertNotNull(response);
            final List<String> invalidatedApiKeyIds = response.getInvalidatedApiKeys();
            List<String> expectedInvalidatedApiKeyIds = Arrays.asList(createApiKeyResponse6.getId());
            assertTrue(response.getErrors().isEmpty());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(expectedInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
            assertThat(response.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        }
    }
}