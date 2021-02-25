package org.apache.dubbo.remoting;

public interface Constants {

    String BUFFER_KEY = "buffer";

    int DEFAULT_BUFFER_SIZE = 8 * 1024;

    int MAX_BUFFER_SIZE = 16 * 1024;

    int MIN_BUFFER_SIZE = 1 * 1024;

    String IDLE_TIMEOUT_KEY = "idle.timeout";

    int DEFAULT_IDLE_TIMEOUT = 600 * 1000;

    String ACCEPTS_KEY = "accepts";

    int DEFAULT_ACCEPTS = 0;

    String CONNECT_QUEUE_CAPACITY = "connect.queue.capacity";

    String CONNECT_QUEUE_WARNING_SIZE = "connect.queue.warning.size";

    int DEFAULT_CONNECT_QUEUE_WARNING_SIZE = 1000;

    String CHARSET_KEY = "charset";

    String DEFAULT_CHARSET = "UTF-8";

    int HEARTBEAT_CHECK_TICK = 3;

    long LEAST_HEARTBEAT_DURATION = 1000;

    int TICKS_PER_WHEEL = 128;

    String PAYLOAD_KEY = "payload";

    int DEFAULT_PAYLOAD = 8 * 1024 * 1024;

    String CONNECT_TIMEOUT_KEY = "connect.timeout";

    int DEFAULT_CONNECT_TIMEOUT = 3000;

    String SERIALIZATION_KEY = "serialization";

    String DEFAULT_REMOTING_SERIALIZATION = "hessian2";

    String CODEC_KEY = "codec";

    String SERVER_KEY = "server";

    String CLIENT_KEY = "client";

    String DEFAULT_REMOTING_CLIENT = "netty";

    String TRANSPORTER_KEY = "transporter";

    String DEFAULT_TRANSPORTER = "netty";

    String EXCHANGER_KEY = "exchanger";

    String DEFAULT_EXCHANGER = "header";

    String DISPACTHER_KEY = "dispacther";

    int DEFAULT_IO_THREADS = Math.min(Runtime.getRuntime().availableProcessors() + 1, 32);

    String BIND_IP_KEY = "bind.ip";

    String BIND_PORT_KEY = "bind.port";

    String SENT_KEY = "sent";

    String DISPATCHER_KEY = "dispatcher";

    String CHANNEL_ATTRIBUTE_READONLY_KEY = "channel.readonly";

    String CHANNEL_READONLYEVENT_SENT_KEY = "channel.readonly.sent";

    String CHANNEL_SEND_READONLYEVENT_KEY = "channel.readonly.send";

    String RECONNECT_KEY = "reconnect";

    int DEFAULT_RECONNECT_PERIOD = 2000;

    String SEND_RECONNECT_KEY = "send.reconnect";

    String CHECK_KEY = "check";

    String PROMPT_KEY = "prompt";

    String DEFAULT_PROMPT = "dubbo>";

    String TELNET = "telnet";

    String HEARTBEAT_KEY = "heartbeat";

    int DEFAULT_HEARTBEAT = 60 * 1000;

    String HEARTBEAT_TIMEOUT_KEY = "heartbeat.timeout";

    String CONNECTIONS_KEY = "connections";

    String DUBBO_VERSION_KEY = "dubbo";
}