package io.netty.example.qotm;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ExceptionEvent;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelUpstreamHandler;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class QuoteOfTheMomentClientHandler extends SimpleChannelUpstreamHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(QuoteOfTheMomentClientHandler.class);

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        String msg = (String) e.getMessage();
        if (msg.startsWith("QOTM: ")) {
            logger.info("Quote of the Moment: " + msg.substring(6));
            e.getChannel().close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.error("Exception caught", e.getCause());
        e.getChannel().close();
    }
}