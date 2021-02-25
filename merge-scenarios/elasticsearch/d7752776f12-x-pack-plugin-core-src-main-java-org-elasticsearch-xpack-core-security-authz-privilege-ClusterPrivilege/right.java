package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

public interface ClusterPrivilege {

    ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder);
}