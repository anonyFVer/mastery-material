package org.elasticsearch.transport;

public abstract class TransportResponse extends TransportMessage {

    public static class Empty extends TransportResponse {

        public static final Empty INSTANCE = new Empty();

        @Override
        public String toString() {
            return "Empty{}";
        }
    }
}