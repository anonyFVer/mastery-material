package io.netty.example.http.upload;

import io.netty.buffer.ChannelBuffer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ExceptionEvent;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelUpstreamHandler;
import io.netty.handler.codec.http.HttpChunk;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.CharsetUtil;

public class HttpUploadClientHandler extends SimpleChannelUpstreamHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(HttpUploadClientHandler.class);

    private volatile boolean readingChunks;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!readingChunks) {
            HttpResponse response = (HttpResponse) e.getMessage();
            logger.info("STATUS: " + response.getStatus());
            logger.info("VERSION: " + response.getProtocolVersion());
            if (!response.getHeaderNames().isEmpty()) {
                for (String name : response.getHeaderNames()) {
                    for (String value : response.getHeaders(name)) {
                        logger.info("HEADER: " + name + " = " + value);
                    }
                }
            }
            if (response.getStatus().getCode() == 200 && response.isChunked()) {
                readingChunks = true;
                logger.info("CHUNKED CONTENT {");
            } else {
                ChannelBuffer content = response.getContent();
                if (content.readable()) {
                    logger.info("CONTENT {");
                    logger.info(content.toString(CharsetUtil.UTF_8));
                    logger.info("} END OF CONTENT");
                }
            }
        } else {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if (chunk.isLast()) {
                readingChunks = false;
                logger.info("} END OF CHUNKED CONTENT");
            } else {
                logger.info(chunk.getContent().toString(CharsetUtil.UTF_8));
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}