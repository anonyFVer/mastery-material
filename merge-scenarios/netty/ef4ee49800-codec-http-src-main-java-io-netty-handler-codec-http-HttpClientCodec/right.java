package io.netty.handler.codec.http;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import io.netty.buffer.ChannelBuffer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDownstreamHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.ChannelUpstreamHandler;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.util.internal.QueueFactory;

public class HttpClientCodec implements ChannelUpstreamHandler, ChannelDownstreamHandler {

    final Queue<HttpMethod> queue = QueueFactory.createQueue(HttpMethod.class);

    volatile boolean done;

    private final HttpRequestEncoder encoder = new Encoder();

    private final HttpResponseDecoder decoder;

    private final AtomicLong requestResponseCounter = new AtomicLong(0);

    private final boolean failOnMissingResponse;

    public HttpClientCodec() {
        this(4096, 8192, 8192, false);
    }

    public HttpClientCodec(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
        this(maxInitialLineLength, maxHeaderSize, maxChunkSize, false);
    }

    public HttpClientCodec(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize, boolean failOnMissingResponse) {
        decoder = new Decoder(maxInitialLineLength, maxHeaderSize, maxChunkSize);
        this.failOnMissingResponse = failOnMissingResponse;
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        decoder.handleUpstream(ctx, e);
    }

    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        encoder.handleDownstream(ctx, e);
    }

    private final class Encoder extends HttpRequestEncoder {

        Encoder() {
        }

        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            if (msg instanceof HttpRequest && !done) {
                queue.offer(((HttpRequest) msg).getMethod());
            }
            Object obj = super.encode(ctx, channel, msg);
            if (failOnMissingResponse) {
                if (msg instanceof HttpRequest && !((HttpRequest) msg).isChunked()) {
                    requestResponseCounter.incrementAndGet();
                } else if (msg instanceof HttpChunk && ((HttpChunk) msg).isLast()) {
                    requestResponseCounter.incrementAndGet();
                }
            }
            return obj;
        }
    }

    private final class Decoder extends HttpResponseDecoder {

        Decoder(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
            super(maxInitialLineLength, maxHeaderSize, maxChunkSize);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, State state) throws Exception {
            if (done) {
                return buffer.readBytes(actualReadableBytes());
            } else {
                Object msg = super.decode(ctx, channel, buffer, state);
                if (failOnMissingResponse) {
                    decrement(msg);
                }
                return msg;
            }
        }

        private void decrement(Object msg) {
            if (msg == null) {
                return;
            }
            if (msg instanceof HttpMessage && !((HttpMessage) msg).isChunked()) {
                requestResponseCounter.decrementAndGet();
            } else if (msg instanceof HttpChunk && ((HttpChunk) msg).isLast()) {
                requestResponseCounter.decrementAndGet();
            } else if (msg instanceof Object[]) {
                requestResponseCounter.decrementAndGet();
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

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            super.channelClosed(ctx, e);
            if (failOnMissingResponse) {
                long missingResponses = requestResponseCounter.get();
                if (missingResponses > 0) {
                    throw new PrematureChannelClosureException("Channel closed but still missing " + missingResponses + " response(s)");
                }
            }
        }
    }
}