package io.netty.handler.codec.http;

import io.netty.buffer.ChannelBuffer;
import io.netty.channel.ChannelInboundHandlerContext;
import io.netty.channel.ChannelOutboundHandlerContext;
import io.netty.channel.CombinedChannelHandler;
import io.netty.util.internal.QueueFactory;
import java.util.Queue;

public class HttpClientCodec extends CombinedChannelHandler {

    final Queue<HttpMethod> queue = QueueFactory.createQueue(HttpMethod.class);

    volatile boolean done;

    public HttpClientCodec() {
        this(4096, 8192, 8192);
    }

    public HttpClientCodec(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
        init(new Decoder(maxInitialLineLength, maxHeaderSize, maxChunkSize), new Encoder());
    }

    private final class Encoder extends HttpRequestEncoder {

        @Override
        public void encode(ChannelOutboundHandlerContext<Object> ctx, Object msg, ChannelBuffer out) throws Exception {
            if (msg instanceof HttpRequest && !done) {
                queue.offer(((HttpRequest) msg).getMethod());
            }
            super.encode(ctx, msg, out);
        }
    }

    private final class Decoder extends HttpResponseDecoder {

        Decoder(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
            super(maxInitialLineLength, maxHeaderSize, maxChunkSize);
        }

        @Override
        public Object decode(ChannelInboundHandlerContext<Byte> ctx, ChannelBuffer buffer) throws Exception {
            if (done) {
                return buffer.readBytes(actualReadableBytes());
            } else {
                return super.decode(ctx, buffer);
            }
        }

        @Override
        protected boolean isContentAlwaysEmpty(HttpMessage msg) {
            final int statusCode = ((HttpResponse) msg).getStatus().getCode();
            if (statusCode == 100) {
                return true;
            }
            HttpMethod method = queue.poll();
            char firstChar = method.getName().charAt(0);
            switch(firstChar) {
                case 'H':
                    if (HttpMethod.HEAD.equals(method)) {
                        return true;
                    }
                    break;
                case 'C':
                    if (statusCode == 200) {
                        if (HttpMethod.CONNECT.equals(method)) {
                            done = true;
                            queue.clear();
                            return true;
                        }
                    }
                    break;
            }
            return super.isContentAlwaysEmpty(msg);
        }
    }
}