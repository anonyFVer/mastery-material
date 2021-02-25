package io.netty.example.securechat;

import java.util.logging.Level;
import java.util.logging.Logger;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.ExceptionEvent;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelUpstreamHandler;
import io.netty.handler.ssl.SslHandler;

public class SecureChatClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = Logger.getLogger(SecureChatClientHandler.class.getName());

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            logger.info(e.toString());
        }
        super.handleUpstream(ctx, e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        sslHandler.handshake();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        System.err.println(e.getMessage());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log(Level.WARNING, "Unexpected exception from downstream.", e.cause());
        e.channel().close();
    }
}