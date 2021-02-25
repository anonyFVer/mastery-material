package io.netty.handler.codec.spdy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class SpdySession {

    private final Map<Integer, StreamState> activeStreams = new ConcurrentHashMap<Integer, StreamState>();

    SpdySession() {
    }

    public int numActiveStreams() {
        return activeStreams.size();
    }

    public boolean noActiveStreams() {
        return activeStreams.isEmpty();
    }

    public boolean isActiveStream(int streamID) {
        return activeStreams.containsKey(new Integer(streamID));
    }

    public void acceptStream(int streamID, boolean remoteSideClosed, boolean localSideClosed) {
        if (!remoteSideClosed || !localSideClosed) {
            activeStreams.put(new Integer(streamID), new StreamState(remoteSideClosed, localSideClosed));
        }
        return;
    }

    public void removeStream(int streamID) {
        activeStreams.remove(new Integer(streamID));
        return;
    }

    public boolean isRemoteSideClosed(int streamID) {
        StreamState state = activeStreams.get(new Integer(streamID));
        return state == null || state.isRemoteSideClosed();
    }

    public void closeRemoteSide(int streamID) {
        Integer StreamID = new Integer(streamID);
        StreamState state = activeStreams.get(StreamID);
        if (state != null) {
            state.closeRemoteSide();
            if (state.isLocalSideClosed()) {
                activeStreams.remove(StreamID);
            }
        }
    }

    public boolean isLocalSideClosed(int streamID) {
        StreamState state = activeStreams.get(new Integer(streamID));
        return state == null || state.isLocalSideClosed();
    }

    public void closeLocalSide(int streamID) {
        Integer StreamID = new Integer(streamID);
        StreamState state = activeStreams.get(StreamID);
        if (state != null) {
            state.closeLocalSide();
            if (state.isRemoteSideClosed()) {
                activeStreams.remove(StreamID);
            }
        }
    }

    public boolean hasReceivedReply(int streamID) {
        StreamState state = activeStreams.get(new Integer(streamID));
        return state != null && state.hasReceivedReply();
    }

    public void receivedReply(int streamID) {
        StreamState state = activeStreams.get(new Integer(streamID));
        if (state != null) {
            state.receivedReply();
        }
    }

    private static final class StreamState {

        private boolean remoteSideClosed;

        private boolean localSideClosed;

        private boolean receivedReply;

        public StreamState(boolean remoteSideClosed, boolean localSideClosed) {
            this.remoteSideClosed = remoteSideClosed;
            this.localSideClosed = localSideClosed;
        }

        public boolean isRemoteSideClosed() {
            return remoteSideClosed;
        }

        public void closeRemoteSide() {
            remoteSideClosed = true;
        }

        public boolean isLocalSideClosed() {
            return localSideClosed;
        }

        public void closeLocalSide() {
            localSideClosed = true;
        }

        public boolean hasReceivedReply() {
            return receivedReply;
        }

        public void receivedReply() {
            receivedReply = true;
        }
    }
}