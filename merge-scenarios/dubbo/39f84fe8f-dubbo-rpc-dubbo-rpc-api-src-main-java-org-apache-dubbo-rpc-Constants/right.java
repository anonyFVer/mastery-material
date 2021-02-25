package org.apache.dubbo.rpc;

public interface Constants {

    String LOCAL_KEY = "local";

    String STUB_KEY = "stub";

    String MOCK_KEY = "mock";

    String DEPRECATED_KEY = "deprecated";

    String $ECHO = "$echo";

    String RETURN_PREFIX = "return ";

    String THROW_PREFIX = "throw";

    String FAIL_PREFIX = "fail:";

    String FORCE_PREFIX = "force:";

    String MERGER_KEY = "merger";

    String IS_SERVER_KEY = "isserver";

    String FORCE_USE_TAG = "dubbo.force.tag";

    String TPS_LIMIT_RATE_KEY = "tps";

    String TPS_LIMIT_INTERVAL_KEY = "tps.interval";

    long DEFAULT_TPS_LIMIT_INTERVAL = 60 * 1000;

    String AUTO_ATTACH_INVOCATIONID_KEY = "invocationid.autoattach";

    boolean DEFAULT_STUB_EVENT = false;

    String STUB_EVENT_METHODS_KEY = "dubbo.stub.event.methods";

    String PROXY_KEY = "proxy";

    String EXECUTES_KEY = "executes";

    String ACCESS_LOG_KEY = "accesslog";

    String ACTIVES_KEY = "actives";

    String ID_KEY = "id";

    String ASYNC_KEY = "async";

    String RETURN_KEY = "return";

    String TOKEN_KEY = "token";

    String INTERFACES = "interfaces";

    String GENERIC_KEY = "generic";

    String LOCAL_PROTOCOL = "injvm";

    String DEFAULT_REMOTING_SERVER = "netty";

    String SCOPE_KEY = "scope";

    String SCOPE_LOCAL = "local";

    String SCOPE_REMOTE = "remote";

    String INPUT_KEY = "input";

    String OUTPUT_KEY = "output";
}