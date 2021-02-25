package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import java.io.IOException;

public abstract class TransportResponse extends TransportMessage {

    public TransportResponse() {
    }

    public TransportResponse(StreamInput in) throws IOException {
        super(in);
    }

    public static class Empty extends TransportResponse {

        public static final Empty INSTANCE = new Empty();
    }
}