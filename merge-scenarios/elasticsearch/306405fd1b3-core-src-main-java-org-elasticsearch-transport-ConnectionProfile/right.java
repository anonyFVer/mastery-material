package org.elasticsearch.transport;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConnectionProfile {

    public static ConnectionProfile buildSingleChannelProfile(TransportRequestOptions.Type channelType, @Nullable TimeValue connectTimeout, @Nullable TimeValue handshakeTimeout) {
        Builder builder = new Builder();
        builder.addConnections(1, channelType);
        final EnumSet<TransportRequestOptions.Type> otherTypes = EnumSet.allOf(TransportRequestOptions.Type.class);
        otherTypes.remove(channelType);
        builder.addConnections(0, otherTypes.stream().toArray(TransportRequestOptions.Type[]::new));
        if (connectTimeout != null) {
            builder.setConnectTimeout(connectTimeout);
        }
        if (handshakeTimeout != null) {
            builder.setHandshakeTimeout(handshakeTimeout);
        }
        return builder.build();
    }

    private final List<ConnectionTypeHandle> handles;

    private final int numConnections;

    private final TimeValue connectTimeout;

    private final TimeValue handshakeTimeout;

    private ConnectionProfile(List<ConnectionTypeHandle> handles, int numConnections, TimeValue connectTimeout, TimeValue handshakeTimeout) {
        this.handles = handles;
        this.numConnections = numConnections;
        this.connectTimeout = connectTimeout;
        this.handshakeTimeout = handshakeTimeout;
    }

    public static class Builder {

        private final List<ConnectionTypeHandle> handles = new ArrayList<>();

        private final Set<TransportRequestOptions.Type> addedTypes = EnumSet.noneOf(TransportRequestOptions.Type.class);

        private int offset = 0;

        private TimeValue connectTimeout;

        private TimeValue handshakeTimeout;

        public void setConnectTimeout(TimeValue connectTimeout) {
            if (connectTimeout.millis() < 0) {
                throw new IllegalArgumentException("connectTimeout must be non-negative but was: " + connectTimeout);
            }
            this.connectTimeout = connectTimeout;
        }

        public void setHandshakeTimeout(TimeValue handshakeTimeout) {
            if (handshakeTimeout.millis() < 0) {
                throw new IllegalArgumentException("handshakeTimeout must be non-negative but was: " + handshakeTimeout);
            }
            this.handshakeTimeout = handshakeTimeout;
        }

        public void addConnections(int numConnections, TransportRequestOptions.Type... types) {
            if (types == null || types.length == 0) {
                throw new IllegalArgumentException("types must not be null");
            }
            for (TransportRequestOptions.Type type : types) {
                if (addedTypes.contains(type)) {
                    throw new IllegalArgumentException("type [" + type + "] is already registered");
                }
            }
            addedTypes.addAll(Arrays.asList(types));
            handles.add(new ConnectionTypeHandle(offset, numConnections, EnumSet.copyOf(Arrays.asList(types))));
            offset += numConnections;
        }

        public ConnectionProfile build() {
            EnumSet<TransportRequestOptions.Type> types = EnumSet.allOf(TransportRequestOptions.Type.class);
            types.removeAll(addedTypes);
            if (types.isEmpty() == false) {
                throw new IllegalStateException("not all types are added for this connection profile - missing types: " + types);
            }
            return new ConnectionProfile(Collections.unmodifiableList(handles), offset, connectTimeout, handshakeTimeout);
        }
    }

    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    public TimeValue getHandshakeTimeout() {
        return handshakeTimeout;
    }

    public int getNumConnections() {
        return numConnections;
    }

    public int getNumConnectionsPerType(TransportRequestOptions.Type type) {
        for (ConnectionTypeHandle handle : handles) {
            if (handle.getTypes().contains(type)) {
                return handle.length;
            }
        }
        throw new AssertionError("no handle found for type: " + type);
    }

    List<ConnectionTypeHandle> getHandles() {
        return Collections.unmodifiableList(handles);
    }

    static final class ConnectionTypeHandle {

        public final int length;

        public final int offset;

        private final Set<TransportRequestOptions.Type> types;

        private final AtomicInteger counter = new AtomicInteger();

        private ConnectionTypeHandle(int offset, int length, Set<TransportRequestOptions.Type> types) {
            this.length = length;
            this.offset = offset;
            this.types = types;
        }

        <T> T getChannel(T[] channels) {
            if (length == 0) {
                throw new IllegalStateException("can't select channel size is 0");
            }
            assert channels.length >= offset + length : "illegal size: " + channels.length + " expected >= " + (offset + length);
            return channels[offset + Math.floorMod(counter.incrementAndGet(), length)];
        }

        Set<TransportRequestOptions.Type> getTypes() {
            return types;
        }
    }
}