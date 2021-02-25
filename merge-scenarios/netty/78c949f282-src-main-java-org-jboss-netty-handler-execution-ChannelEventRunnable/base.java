package org.jboss.netty.handler.execution;

import java.util.concurrent.Executor;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.util.EstimatableObjectWrapper;

public class ChannelEventRunnable implements Runnable, EstimatableObjectWrapper {

    private final ChannelHandlerContext ctx;

    private final ChannelEvent e;

    int estimatedSize;

    public ChannelEventRunnable(ChannelHandlerContext ctx, ChannelEvent e) {
        this.ctx = ctx;
        this.e = e;
    }

    public ChannelHandlerContext getContext() {
        return ctx;
    }

    public ChannelEvent getEvent() {
        return e;
    }

    @Override
    public void run() {
        ctx.sendUpstream(e);
    }

    @Override
    public Object unwrap() {
        return e;
    }
}