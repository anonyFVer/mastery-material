package io.netty.channel;

import io.netty.buffer.ChannelBuffer;
import io.netty.channel.DefaultChannelHandlerContext.MessageBridge;
import io.netty.channel.DefaultChannelHandlerContext.StreamBridge;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Executor;

public class DefaultChannelPipeline implements ChannelPipeline {

    static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultChannelPipeline.class);

    final Channel channel;

    private final Channel.Unsafe unsafe;

    final ChannelBufferHolder<Object> directOutbound;

    private final DefaultChannelHandlerContext head;

    private volatile DefaultChannelHandlerContext tail;

    private final Map<String, DefaultChannelHandlerContext> name2ctx = new HashMap<String, DefaultChannelHandlerContext>(4);

    private boolean firedChannelActive;

    private boolean fireInboundBufferUpdatedOnActivation;

    final Map<EventExecutor, EventExecutor> childExecutors = new IdentityHashMap<EventExecutor, EventExecutor>();

    public DefaultChannelPipeline(Channel channel) {
        if (channel == null) {
            throw new NullPointerException("channel");
        }
        this.channel = channel;
        HeadHandler headHandler = new HeadHandler();
        head = new DefaultChannelHandlerContext(this, null, null, null, generateName(headHandler), headHandler);
        tail = head;
        directOutbound = head.out;
        unsafe = channel.unsafe();
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public ChannelPipeline addFirst(String name, ChannelHandler handler) {
        return addFirst(null, name, handler);
    }

    @Override
    public synchronized ChannelPipeline addFirst(EventExecutor executor, final String name, final ChannelHandler handler) {
        checkDuplicateName(name);
        final DefaultChannelHandlerContext nextCtx = head.next;
        final DefaultChannelHandlerContext newCtx = new DefaultChannelHandlerContext(this, executor, head, nextCtx, name, handler);
        if (!newCtx.channel().isRegistered() || newCtx.executor().inEventLoop()) {
            addFirst0(name, handler, nextCtx, newCtx);
        } else {
            ChannelPipelineModificationRunnable runnable = new ChannelPipelineModificationRunnable() {

                @Override
                protected void runTask() {
                    checkDuplicateName(name);
                    addFirst0(name, handler, nextCtx, newCtx);
                }
            };
            newCtx.executor().execute(runnable);
            runnable.await();
        }
        return this;
    }

    private void addFirst0(final String name, ChannelHandler handler, DefaultChannelHandlerContext nextCtx, DefaultChannelHandlerContext newCtx) {
        callBeforeAdd(newCtx);
        if (nextCtx != null) {
            nextCtx.prev = newCtx;
        }
        head.next = newCtx;
        name2ctx.put(name, newCtx);
        callAfterAdd(newCtx);
    }

    @Override
    public ChannelPipeline addLast(String name, ChannelHandler handler) {
        return addLast(null, name, handler);
    }

    @Override
    public synchronized ChannelPipeline addLast(EventExecutor executor, final String name, final ChannelHandler handler) {
        checkDuplicateName(name);
        final DefaultChannelHandlerContext oldTail = tail;
        final DefaultChannelHandlerContext newTail = new DefaultChannelHandlerContext(this, executor, oldTail, null, name, handler);
        if (!newTail.channel().isRegistered() || newTail.executor().inEventLoop()) {
            addLast0(name, handler, oldTail, newTail);
        } else {
            ChannelPipelineModificationRunnable runnable = new ChannelPipelineModificationRunnable() {

                @Override
                protected void runTask() {
                    checkDuplicateName(name);
                    addLast0(name, handler, oldTail, newTail);
                }
            };
            newTail.executor().execute(runnable);
            runnable.await();
        }
        return this;
    }

    private void addLast0(final String name, ChannelHandler handler, DefaultChannelHandlerContext oldTail, DefaultChannelHandlerContext newTail) {
        callBeforeAdd(newTail);
        oldTail.next = newTail;
        tail = newTail;
        name2ctx.put(name, newTail);
        callAfterAdd(newTail);
    }

    @Override
    public ChannelPipeline addBefore(String baseName, String name, ChannelHandler handler) {
        return addBefore(null, baseName, name, handler);
    }

    @Override
    public synchronized ChannelPipeline addBefore(EventExecutor executor, String baseName, final String name, final ChannelHandler handler) {
        final DefaultChannelHandlerContext ctx = getContextOrDie(baseName);
        checkDuplicateName(name);
        final DefaultChannelHandlerContext newCtx = new DefaultChannelHandlerContext(this, executor, ctx.prev, ctx, name, handler);
        if (!newCtx.channel().isRegistered() || newCtx.executor().inEventLoop()) {
            addBefore0(name, handler, ctx, newCtx);
        } else {
            ChannelPipelineModificationRunnable runnable = new ChannelPipelineModificationRunnable() {

                @Override
                protected void runTask() {
                    checkDuplicateName(name);
                    addBefore0(name, handler, ctx, newCtx);
                }
            };
            newCtx.executor().execute(runnable);
            runnable.await();
        }
        return this;
    }

    private void addBefore0(final String name, ChannelHandler handler, DefaultChannelHandlerContext ctx, DefaultChannelHandlerContext newCtx) {
        callBeforeAdd(newCtx);
        ctx.prev.next = newCtx;
        ctx.prev = newCtx;
        name2ctx.put(name, newCtx);
        callAfterAdd(newCtx);
    }

    @Override
    public ChannelPipeline addAfter(String baseName, String name, ChannelHandler handler) {
        return addAfter(null, baseName, name, handler);
    }

    @Override
    public synchronized ChannelPipeline addAfter(EventExecutor executor, String baseName, final String name, final ChannelHandler handler) {
        final DefaultChannelHandlerContext ctx = getContextOrDie(baseName);
        if (ctx == tail) {
            addLast(name, handler);
        } else {
            checkDuplicateName(name);
            final DefaultChannelHandlerContext newCtx = new DefaultChannelHandlerContext(this, executor, ctx, ctx.next, name, handler);
            if (!newCtx.channel().isRegistered() || newCtx.executor().inEventLoop()) {
                addAfter0(name, handler, ctx, newCtx);
            } else {
                ChannelPipelineModificationRunnable runnable = new ChannelPipelineModificationRunnable() {

                    @Override
                    protected void runTask() {
                        checkDuplicateName(name);
                        addAfter0(name, handler, ctx, newCtx);
                    }
                };
                newCtx.executor().execute(runnable);
                runnable.await();
            }
        }
        return this;
    }

    private void addAfter0(final String name, ChannelHandler handler, DefaultChannelHandlerContext ctx, DefaultChannelHandlerContext newCtx) {
        checkDuplicateName(name);
        callBeforeAdd(newCtx);
        ctx.next.prev = newCtx;
        ctx.next = newCtx;
        name2ctx.put(name, newCtx);
        callAfterAdd(newCtx);
    }

    @Override
    public ChannelPipeline addFirst(ChannelHandler... handlers) {
        return addFirst(null, handlers);
    }

    @Override
    public ChannelPipeline addFirst(EventExecutor executor, ChannelHandler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }
        if (handlers.length == 0 || handlers[0] == null) {
            return this;
        }
        int size;
        for (size = 1; size < handlers.length; size++) {
            if (handlers[size] == null) {
                break;
            }
        }
        for (int i = size - 1; i >= 0; i--) {
            ChannelHandler h = handlers[i];
            addFirst(executor, generateName(h), h);
        }
        return this;
    }

    @Override
    public ChannelPipeline addLast(ChannelHandler... handlers) {
        return addLast(null, handlers);
    }

    @Override
    public ChannelPipeline addLast(EventExecutor executor, ChannelHandler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }
        for (ChannelHandler h : handlers) {
            if (h == null) {
                break;
            }
            addLast(executor, generateName(h), h);
        }
        return this;
    }

    private static String generateName(ChannelHandler handler) {
        String type = handler.getClass().getSimpleName();
        StringBuilder buf = new StringBuilder(type.length() + 10);
        buf.append(type);
        buf.append("-0");
        buf.append(Long.toHexString(System.identityHashCode(handler) & 0xFFFFFFFFL | 0x100000000L));
        buf.setCharAt(buf.length() - 9, 'x');
        return buf.toString();
    }

    @Override
    public synchronized void remove(ChannelHandler handler) {
        remove(getContextOrDie(handler));
    }

    @Override
    public synchronized ChannelHandler remove(String name) {
        return remove(getContextOrDie(name)).handler();
    }

    @Override
    public synchronized <T extends ChannelHandler> T remove(Class<T> handlerType) {
        return (T) remove(getContextOrDie(handlerType)).handler();
    }

    private DefaultChannelHandlerContext remove(final DefaultChannelHandlerContext ctx) {
        if (head == tail) {
            return null;
        } else if (ctx == head) {
            throw new Error();
        } else if (ctx == tail) {
            removeLast();
        } else {
            if (!ctx.channel().isRegistered() || ctx.executor().inEventLoop()) {
                remove0(ctx);
            } else {
                ChannelPipelineModificationRunnable runnable = new ChannelPipelineModificationRunnable() {

                    @Override
                    protected void runTask() {
                        remove0(ctx);
                    }
                };
                ctx.executor().execute(runnable);
                runnable.await();
            }
        }
        return ctx;
    }

    private void remove0(DefaultChannelHandlerContext ctx) {
        callBeforeRemove(ctx);
        DefaultChannelHandlerContext prev = ctx.prev;
        DefaultChannelHandlerContext next = ctx.next;
        prev.next = next;
        next.prev = prev;
        name2ctx.remove(ctx.name());
        callAfterRemove(ctx);
    }

    @Override
    public synchronized ChannelHandler removeFirst() {
        if (head == tail) {
            throw new NoSuchElementException();
        }
        return remove(head.next).handler();
    }

    @Override
    public synchronized ChannelHandler removeLast() {
        if (head == tail) {
            throw new NoSuchElementException();
        }
        final DefaultChannelHandlerContext oldTail = tail;
        if (!oldTail.channel().isRegistered() || oldTail.executor().inEventLoop()) {
            removeLast0(oldTail);
        } else {
            ChannelPipelineModificationRunnable runnable = new ChannelPipelineModificationRunnable() {

                @Override
                protected void runTask() {
                    removeLast0(oldTail);
                }
            };
            oldTail.executor().execute(runnable);
            runnable.await();
        }
        return oldTail.handler();
    }

    private void removeLast0(DefaultChannelHandlerContext oldTail) {
        callBeforeRemove(oldTail);
        oldTail.prev.next = null;
        tail = oldTail.prev;
        name2ctx.remove(oldTail.name());
        callBeforeRemove(oldTail);
    }

    @Override
    public synchronized void replace(ChannelHandler oldHandler, String newName, ChannelHandler newHandler) {
        replace(getContextOrDie(oldHandler), newName, newHandler);
    }

    @Override
    public synchronized ChannelHandler replace(String oldName, String newName, ChannelHandler newHandler) {
        return replace(getContextOrDie(oldName), newName, newHandler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <T extends ChannelHandler> T replace(Class<T> oldHandlerType, String newName, ChannelHandler newHandler) {
        return (T) replace(getContextOrDie(oldHandlerType), newName, newHandler);
    }

    private ChannelHandler replace(final DefaultChannelHandlerContext ctx, final String newName, final ChannelHandler newHandler) {
        if (ctx == head) {
            throw new IllegalArgumentException();
        } else if (ctx == tail) {
            removeLast();
            addLast(newName, newHandler);
        } else {
            boolean sameName = ctx.name().equals(newName);
            if (!sameName) {
                checkDuplicateName(newName);
            }
            DefaultChannelHandlerContext prev = ctx.prev;
            DefaultChannelHandlerContext next = ctx.next;
            final DefaultChannelHandlerContext newCtx = new DefaultChannelHandlerContext(this, ctx.executor, prev, next, newName, newHandler);
            if (!newCtx.channel().isRegistered() || newCtx.executor().inEventLoop()) {
                replace0(ctx, newName, newHandler, newCtx);
            } else {
                ChannelPipelineModificationRunnable runnable = new ChannelPipelineModificationRunnable() {

                    @Override
                    protected void runTask() {
                        replace0(ctx, newName, newHandler, newCtx);
                    }
                };
                newCtx.executor().execute(runnable);
                runnable.await();
            }
        }
        return ctx.handler();
    }

    private void replace0(DefaultChannelHandlerContext ctx, String newName, ChannelHandler newHandler, DefaultChannelHandlerContext newCtx) {
        boolean sameName = ctx.name().equals(newName);
        DefaultChannelHandlerContext prev = ctx.prev;
        DefaultChannelHandlerContext next = ctx.next;
        callBeforeRemove(ctx);
        callBeforeAdd(newCtx);
        prev.next = newCtx;
        next.prev = newCtx;
        if (!sameName) {
            name2ctx.remove(ctx.name());
        }
        name2ctx.put(newName, newCtx);
        ChannelHandlerLifeCycleException removeException = null;
        ChannelHandlerLifeCycleException addException = null;
        boolean removed = false;
        try {
            callAfterRemove(ctx);
            removed = true;
        } catch (ChannelHandlerLifeCycleException e) {
            removeException = e;
        }
        boolean added = false;
        try {
            callAfterAdd(newCtx);
            added = true;
        } catch (ChannelHandlerLifeCycleException e) {
            addException = e;
        }
        if (!removed && !added) {
            logger.warn(removeException.getMessage(), removeException);
            logger.warn(addException.getMessage(), addException);
            throw new ChannelHandlerLifeCycleException("Both " + ctx.handler().getClass().getName() + ".afterRemove() and " + newCtx.handler().getClass().getName() + ".afterAdd() failed; see logs.");
        } else if (!removed) {
            throw removeException;
        } else if (!added) {
            throw addException;
        }
    }

    private static void callBeforeAdd(ChannelHandlerContext ctx) {
        ChannelHandler handler = ctx.handler();
        if (handler instanceof AbstractChannelHandler) {
            AbstractChannelHandler h = (AbstractChannelHandler) handler;
            if (!h.isSharable() && h.added) {
                throw new ChannelHandlerLifeCycleException("Only a @Sharable handler can be added or removed multiple times.");
            }
            h.added = true;
        }
        try {
            handler.beforeAdd(ctx);
        } catch (Throwable t) {
            throw new ChannelHandlerLifeCycleException(handler.getClass().getName() + ".beforeAdd() has thrown an exception; not adding.", t);
        }
    }

    private void callAfterAdd(ChannelHandlerContext ctx) {
        try {
            ctx.handler().afterAdd(ctx);
        } catch (Throwable t) {
            boolean removed = false;
            try {
                remove((DefaultChannelHandlerContext) ctx);
                removed = true;
            } catch (Throwable t2) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to remove a handler: " + ctx.name(), t2);
                }
            }
            if (removed) {
                throw new ChannelHandlerLifeCycleException(ctx.handler().getClass().getName() + ".afterAdd() has thrown an exception; removed.", t);
            } else {
                throw new ChannelHandlerLifeCycleException(ctx.handler().getClass().getName() + ".afterAdd() has thrown an exception; also failed to remove.", t);
            }
        }
    }

    private static void callBeforeRemove(ChannelHandlerContext ctx) {
        try {
            ctx.handler().beforeRemove(ctx);
        } catch (Throwable t) {
            throw new ChannelHandlerLifeCycleException(ctx.handler().getClass().getName() + ".beforeRemove() has thrown an exception; not removing.", t);
        }
    }

    private static void callAfterRemove(ChannelHandlerContext ctx) {
        try {
            ctx.handler().afterRemove(ctx);
        } catch (Throwable t) {
            throw new ChannelHandlerLifeCycleException(ctx.handler().getClass().getName() + ".afterRemove() has thrown an exception.", t);
        }
    }

    @Override
    public synchronized ChannelHandler first() {
        DefaultChannelHandlerContext first = head.next;
        if (first == null) {
            return null;
        }
        return first.handler();
    }

    @Override
    public synchronized ChannelHandler last() {
        DefaultChannelHandlerContext last = tail;
        if (last == head || last == null) {
            return null;
        }
        return last.handler();
    }

    @Override
    public synchronized ChannelHandler get(String name) {
        DefaultChannelHandlerContext ctx = name2ctx.get(name);
        if (ctx == null) {
            return null;
        } else {
            return ctx.handler();
        }
    }

    @Override
    public synchronized <T extends ChannelHandler> T get(Class<T> handlerType) {
        ChannelHandlerContext ctx = context(handlerType);
        if (ctx == null) {
            return null;
        } else {
            return (T) ctx.handler();
        }
    }

    @Override
    public synchronized ChannelHandlerContext context(String name) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        return name2ctx.get(name);
    }

    @Override
    public synchronized ChannelHandlerContext context(ChannelHandler handler) {
        if (handler == null) {
            throw new NullPointerException("handler");
        }
        if (name2ctx.isEmpty()) {
            return null;
        }
        DefaultChannelHandlerContext ctx = head;
        for (; ; ) {
            if (ctx.handler() == handler) {
                return ctx;
            }
            ctx = ctx.next;
            if (ctx == null) {
                break;
            }
        }
        return null;
    }

    @Override
    public synchronized ChannelHandlerContext context(Class<? extends ChannelHandler> handlerType) {
        if (handlerType == null) {
            throw new NullPointerException("handlerType");
        }
        if (name2ctx.isEmpty()) {
            return null;
        }
        DefaultChannelHandlerContext ctx = head.next;
        for (; ; ) {
            if (handlerType.isAssignableFrom(ctx.handler().getClass())) {
                return ctx;
            }
            ctx = ctx.next;
            if (ctx == null) {
                break;
            }
        }
        return null;
    }

    @Override
    public List<String> names() {
        List<String> list = new ArrayList<String>();
        if (name2ctx.isEmpty()) {
            return list;
        }
        DefaultChannelHandlerContext ctx = head.next;
        for (; ; ) {
            list.add(ctx.name());
            ctx = ctx.next;
            if (ctx == null) {
                break;
            }
        }
        return list;
    }

    @Override
    public Map<String, ChannelHandler> toMap() {
        Map<String, ChannelHandler> map = new LinkedHashMap<String, ChannelHandler>();
        if (name2ctx.isEmpty()) {
            return map;
        }
        DefaultChannelHandlerContext ctx = head.next;
        for (; ; ) {
            map.put(ctx.name(), ctx.handler());
            ctx = ctx.next;
            if (ctx == null) {
                break;
            }
        }
        return map;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(getClass().getSimpleName());
        buf.append('{');
        DefaultChannelHandlerContext ctx = head.next;
        for (; ; ) {
            buf.append('(');
            buf.append(ctx.name());
            buf.append(" = ");
            buf.append(ctx.handler().getClass().getName());
            buf.append(')');
            ctx = ctx.next;
            if (ctx == null) {
                break;
            }
            buf.append(", ");
        }
        buf.append('}');
        return buf.toString();
    }

    @Override
    public Queue<Object> inboundMessageBuffer() {
        if (channel.type() != ChannelType.MESSAGE) {
            throw new NoSuchBufferException("The first inbound buffer of this channel must be a message buffer.");
        }
        return nextInboundMessageBuffer(SingleThreadEventExecutor.currentEventLoop(), head.next);
    }

    @Override
    public ChannelBuffer inboundByteBuffer() {
        if (channel.type() != ChannelType.STREAM) {
            throw new NoSuchBufferException("The first inbound buffer of this channel must be a byte buffer.");
        }
        return nextInboundByteBuffer(SingleThreadEventExecutor.currentEventLoop(), head.next);
    }

    @Override
    public Queue<Object> outboundMessageBuffer() {
        return nextOutboundMessageBuffer(SingleThreadEventExecutor.currentEventLoop(), tail);
    }

    @Override
    public ChannelBuffer outboundByteBuffer() {
        return nextOutboundByteBuffer(SingleThreadEventExecutor.currentEventLoop(), tail);
    }

    static boolean hasNextInboundByteBuffer(DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                return false;
            }
            if (ctx.inByteBridge != null) {
                return true;
            }
            ctx = ctx.next;
        }
    }

    static boolean hasNextInboundMessageBuffer(DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                return false;
            }
            if (ctx.inMsgBridge != null) {
                return true;
            }
            ctx = ctx.next;
        }
    }

    static ChannelBuffer nextInboundByteBuffer(Executor currentExecutor, DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                throw new NoSuchBufferException();
            }
            if (ctx.inByteBridge != null) {
                if (currentExecutor == ctx.executor()) {
                    return ctx.in.byteBuffer();
                } else {
                    StreamBridge bridge = ctx.inByteBridge.get();
                    if (bridge == null) {
                        bridge = new StreamBridge();
                        if (!ctx.inByteBridge.compareAndSet(null, bridge)) {
                            bridge = ctx.inByteBridge.get();
                        }
                    }
                    return bridge.byteBuf;
                }
            }
            ChannelBufferHolder<Object> in = ctx.in;
            if (in != null && !in.isBypass() && in.hasByteBuffer()) {
                return in.byteBuffer();
            }
            ctx = ctx.next;
        }
    }

    static Queue<Object> nextInboundMessageBuffer(Executor currentExecutor, DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                throw new NoSuchBufferException();
            }
            if (ctx.inMsgBridge != null) {
                if (currentExecutor == ctx.executor()) {
                    return ctx.in.messageBuffer();
                } else {
                    MessageBridge bridge = ctx.inMsgBridge.get();
                    if (bridge == null) {
                        bridge = new MessageBridge();
                        if (!ctx.inMsgBridge.compareAndSet(null, bridge)) {
                            bridge = ctx.inMsgBridge.get();
                        }
                    }
                    return bridge.msgBuf;
                }
            }
            ctx = ctx.next;
        }
    }

    boolean hasNextOutboundByteBuffer(DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                return false;
            }
            if (ctx.outByteBridge != null) {
                return true;
            }
            ctx = ctx.prev;
        }
    }

    boolean hasNextOutboundMessageBuffer(DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                return false;
            }
            if (ctx.outMsgBridge != null) {
                return true;
            }
            ctx = ctx.prev;
        }
    }

    ChannelBuffer nextOutboundByteBuffer(Executor currentExecutor, DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                throw new NoSuchBufferException();
            }
            if (ctx.outByteBridge != null) {
                if (currentExecutor == ctx.executor()) {
                    return ctx.out.byteBuffer();
                } else {
                    StreamBridge bridge = ctx.outByteBridge.get();
                    if (bridge == null) {
                        bridge = new StreamBridge();
                        if (!ctx.outByteBridge.compareAndSet(null, bridge)) {
                            bridge = ctx.outByteBridge.get();
                        }
                    }
                    return bridge.byteBuf;
                }
            }
            ctx = ctx.prev;
        }
    }

    Queue<Object> nextOutboundMessageBuffer(Executor currentExecutor, DefaultChannelHandlerContext ctx) {
        for (; ; ) {
            if (ctx == null) {
                throw new NoSuchBufferException();
            }
            if (ctx.outMsgBridge != null) {
                if (currentExecutor == ctx.executor()) {
                    return ctx.out.messageBuffer();
                } else {
                    MessageBridge bridge = ctx.outMsgBridge.get();
                    if (bridge == null) {
                        bridge = new MessageBridge();
                        if (!ctx.outMsgBridge.compareAndSet(null, bridge)) {
                            bridge = ctx.outMsgBridge.get();
                        }
                    }
                    return bridge.msgBuf;
                }
            }
            ctx = ctx.prev;
        }
    }

    @Override
    public void fireChannelRegistered() {
        DefaultChannelHandlerContext ctx = firstInboundContext();
        if (ctx != null) {
            fireChannelRegistered(ctx);
        }
    }

    static void fireChannelRegistered(DefaultChannelHandlerContext ctx) {
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            ctx.fireChannelRegisteredTask.run();
        } else {
            executor.execute(ctx.fireChannelRegisteredTask);
        }
    }

    @Override
    public void fireChannelUnregistered() {
        DefaultChannelHandlerContext ctx = firstInboundContext();
        if (ctx != null) {
            fireChannelUnregistered(ctx);
        }
    }

    static void fireChannelUnregistered(DefaultChannelHandlerContext ctx) {
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            ctx.fireChannelUnregisteredTask.run();
        } else {
            executor.execute(ctx.fireChannelUnregisteredTask);
        }
    }

    @Override
    public void fireChannelActive() {
        DefaultChannelHandlerContext ctx = firstInboundContext();
        if (ctx != null) {
            firedChannelActive = true;
            fireChannelActive(ctx);
            if (fireInboundBufferUpdatedOnActivation) {
                fireInboundBufferUpdatedOnActivation = false;
                fireInboundBufferUpdated(ctx);
            }
        }
    }

    static void fireChannelActive(DefaultChannelHandlerContext ctx) {
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            ctx.fireChannelActiveTask.run();
        } else {
            executor.execute(ctx.fireChannelActiveTask);
        }
    }

    @Override
    public void fireChannelInactive() {
        DefaultChannelHandlerContext ctx = firstInboundContext();
        if (ctx != null) {
            fireChannelInactive(ctx);
        }
    }

    static void fireChannelInactive(DefaultChannelHandlerContext ctx) {
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            ctx.fireChannelInactiveTask.run();
        } else {
            executor.execute(ctx.fireChannelInactiveTask);
        }
    }

    @Override
    public void fireExceptionCaught(Throwable cause) {
        DefaultChannelHandlerContext ctx = firstInboundContext();
        if (ctx != null) {
            fireExceptionCaught(ctx, cause);
        } else {
            logTerminalException(cause);
        }
    }

    static void logTerminalException(Throwable cause) {
        logger.warn("An exceptionCaught() event was fired, and it reached at the end of the " + "pipeline.  It usually means the last inbound handler in the pipeline did not " + "handle the exception.", cause);
    }

    void fireExceptionCaught(final DefaultChannelHandlerContext ctx, final Throwable cause) {
        if (cause == null) {
            throw new NullPointerException("cause");
        }
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            try {
                ((ChannelInboundHandler<Object>) ctx.handler()).exceptionCaught(ctx, cause);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by a user handler's " + "exceptionCaught() method while handling the following exception:", cause);
                }
                notifyHandlerException(t);
            }
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    fireExceptionCaught(ctx, cause);
                }
            });
        }
    }

    @Override
    public void fireUserEventTriggered(Object event) {
        DefaultChannelHandlerContext ctx = firstInboundContext();
        if (ctx != null) {
            fireUserEventTriggered(ctx, event);
        }
    }

    void fireUserEventTriggered(final DefaultChannelHandlerContext ctx, final Object event) {
        if (event == null) {
            throw new NullPointerException("event");
        }
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            try {
                ((ChannelInboundHandler<Object>) ctx.handler()).userEventTriggered(ctx, event);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    fireUserEventTriggered(ctx, event);
                }
            });
        }
    }

    @Override
    public void fireInboundBufferUpdated() {
        if (!firedChannelActive) {
            fireInboundBufferUpdatedOnActivation = true;
            return;
        }
        DefaultChannelHandlerContext ctx = firstInboundContext();
        if (ctx != null) {
            fireInboundBufferUpdated(ctx);
        }
    }

    static void fireInboundBufferUpdated(DefaultChannelHandlerContext ctx) {
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            ctx.curCtxFireInboundBufferUpdatedTask.run();
        } else {
            executor.execute(ctx.curCtxFireInboundBufferUpdatedTask);
        }
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return bind(localAddress, channel.newFuture());
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return connect(remoteAddress, channel.newFuture());
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return connect(remoteAddress, localAddress, channel.newFuture());
    }

    @Override
    public ChannelFuture disconnect() {
        return disconnect(channel.newFuture());
    }

    @Override
    public ChannelFuture close() {
        return close(channel.newFuture());
    }

    @Override
    public ChannelFuture deregister() {
        return deregister(channel.newFuture());
    }

    @Override
    public ChannelFuture flush() {
        return flush(channel.newFuture());
    }

    @Override
    public ChannelFuture write(Object message) {
        return write(message, channel.newFuture());
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelFuture future) {
        return bind(firstOutboundContext(), localAddress, future);
    }

    ChannelFuture bind(final DefaultChannelHandlerContext ctx, final SocketAddress localAddress, final ChannelFuture future) {
        if (localAddress == null) {
            throw new NullPointerException("localAddress");
        }
        validateFuture(future);
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            try {
                ((ChannelOutboundHandler<Object>) ctx.handler()).bind(ctx, localAddress, future);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    bind(ctx, localAddress, future);
                }
            });
        }
        return future;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelFuture future) {
        return connect(remoteAddress, null, future);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelFuture future) {
        return connect(firstOutboundContext(), remoteAddress, localAddress, future);
    }

    ChannelFuture connect(final DefaultChannelHandlerContext ctx, final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelFuture future) {
        if (remoteAddress == null) {
            throw new NullPointerException("remoteAddress");
        }
        validateFuture(future);
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            try {
                ((ChannelOutboundHandler<Object>) ctx.handler()).connect(ctx, remoteAddress, localAddress, future);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    connect(ctx, remoteAddress, localAddress, future);
                }
            });
        }
        return future;
    }

    @Override
    public ChannelFuture disconnect(ChannelFuture future) {
        return disconnect(firstOutboundContext(), future);
    }

    ChannelFuture disconnect(final DefaultChannelHandlerContext ctx, final ChannelFuture future) {
        validateFuture(future);
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            try {
                ((ChannelOutboundHandler<Object>) ctx.handler()).disconnect(ctx, future);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    disconnect(ctx, future);
                }
            });
        }
        return future;
    }

    @Override
    public ChannelFuture close(ChannelFuture future) {
        return close(firstOutboundContext(), future);
    }

    ChannelFuture close(final DefaultChannelHandlerContext ctx, final ChannelFuture future) {
        validateFuture(future);
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            try {
                ((ChannelOutboundHandler<Object>) ctx.handler()).close(ctx, future);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    close(ctx, future);
                }
            });
        }
        return future;
    }

    @Override
    public ChannelFuture deregister(final ChannelFuture future) {
        return deregister(firstOutboundContext(), future);
    }

    ChannelFuture deregister(final DefaultChannelHandlerContext ctx, final ChannelFuture future) {
        validateFuture(future);
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            try {
                ((ChannelOutboundHandler<Object>) ctx.handler()).deregister(ctx, future);
            } catch (Throwable t) {
                notifyHandlerException(t);
            }
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    deregister(ctx, future);
                }
            });
        }
        return future;
    }

    @Override
    public ChannelFuture flush(ChannelFuture future) {
        return flush(firstOutboundContext(), future);
    }

    ChannelFuture flush(final DefaultChannelHandlerContext ctx, final ChannelFuture future) {
        validateFuture(future);
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            flush0(ctx, future);
        } else {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    flush(ctx, future);
                }
            });
        }
        return future;
    }

    private void flush0(final DefaultChannelHandlerContext ctx, ChannelFuture future) {
        try {
            ctx.flushBridge();
            ((ChannelOutboundHandler<Object>) ctx.handler()).flush(ctx, future);
        } catch (Throwable t) {
            notifyHandlerException(t);
        } finally {
            ChannelBufferHolder<Object> outbound = ctx.outbound();
            if (!outbound.isBypass() && outbound.isEmpty() && outbound.hasByteBuffer()) {
                outbound.byteBuffer().discardReadBytes();
            }
        }
    }

    @Override
    public ChannelFuture write(Object message, ChannelFuture future) {
        return write(tail, message, future);
    }

    ChannelFuture write(DefaultChannelHandlerContext ctx, final Object message, final ChannelFuture future) {
        if (message == null) {
            throw new NullPointerException("message");
        }
        validateFuture(future);
        EventExecutor executor;
        ChannelBufferHolder<Object> out;
        boolean msgBuf = false;
        for (; ; ) {
            if (ctx == null) {
                throw new NoSuchBufferException();
            }
            if (ctx.canHandleOutbound()) {
                out = ctx.outbound();
                if (out.hasMessageBuffer()) {
                    msgBuf = true;
                    executor = ctx.executor();
                    break;
                } else if (message instanceof ChannelBuffer) {
                    executor = ctx.executor();
                    break;
                }
            }
            ctx = ctx.prev;
        }
        if (executor.inEventLoop()) {
            if (msgBuf) {
                out.messageBuffer().add(message);
            } else {
                ChannelBuffer buf = (ChannelBuffer) message;
                out.byteBuffer().writeBytes(buf, buf.readerIndex(), buf.readableBytes());
            }
            flush0(ctx, future);
            return future;
        } else {
            final DefaultChannelHandlerContext ctx0 = ctx;
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    write(ctx0, message, future);
                }
            });
        }
        return future;
    }

    private void validateFuture(ChannelFuture future) {
        if (future == null) {
            throw new NullPointerException("future");
        }
        if (future.channel() != channel) {
            throw new IllegalArgumentException(String.format("future.channel does not match: %s (expected: %s)", future.channel(), channel));
        }
        if (future.isDone()) {
            throw new IllegalArgumentException("future already done");
        }
        if (future instanceof ChannelFuture.Unsafe) {
            throw new IllegalArgumentException("internal use only future not allowed");
        }
    }

    private DefaultChannelHandlerContext firstInboundContext() {
        return nextInboundContext(head.next);
    }

    private DefaultChannelHandlerContext firstOutboundContext() {
        return nextOutboundContext(tail);
    }

    static DefaultChannelHandlerContext nextInboundContext(DefaultChannelHandlerContext ctx) {
        if (ctx == null) {
            return null;
        }
        DefaultChannelHandlerContext realCtx = ctx;
        while (!realCtx.canHandleInbound()) {
            realCtx = realCtx.next;
            if (realCtx == null) {
                return null;
            }
        }
        return realCtx;
    }

    static DefaultChannelHandlerContext nextOutboundContext(DefaultChannelHandlerContext ctx) {
        if (ctx == null) {
            return null;
        }
        DefaultChannelHandlerContext realCtx = ctx;
        while (!realCtx.canHandleOutbound()) {
            realCtx = realCtx.prev;
            if (realCtx == null) {
                return null;
            }
        }
        return realCtx;
    }

    protected void notifyHandlerException(Throwable cause) {
        if (!(cause instanceof ChannelPipelineException)) {
            cause = new ChannelPipelineException(cause);
        }
        if (inExceptionCaught(cause)) {
            if (logger.isWarnEnabled()) {
                logger.warn("An exception was thrown by a user handler " + "while handling an exceptionCaught event", cause);
            }
            return;
        }
        fireExceptionCaught(cause);
    }

    private static boolean inExceptionCaught(Throwable cause) {
        if (cause == null) {
            return false;
        }
        StackTraceElement[] trace = cause.getStackTrace();
        if (trace != null) {
            for (StackTraceElement t : trace) {
                if ("exceptionCaught".equals(t.getMethodName())) {
                    return true;
                }
            }
        }
        return inExceptionCaught(cause.getCause());
    }

    private void checkDuplicateName(String name) {
        if (name2ctx.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate handler name: " + name);
        }
    }

    private DefaultChannelHandlerContext getContextOrDie(String name) {
        DefaultChannelHandlerContext ctx = (DefaultChannelHandlerContext) context(name);
        if (ctx == null || ctx == head) {
            throw new NoSuchElementException(name);
        } else {
            return ctx;
        }
    }

    private DefaultChannelHandlerContext getContextOrDie(ChannelHandler handler) {
        DefaultChannelHandlerContext ctx = (DefaultChannelHandlerContext) context(handler);
        if (ctx == null || ctx == head) {
            throw new NoSuchElementException(handler.getClass().getName());
        } else {
            return ctx;
        }
    }

    private DefaultChannelHandlerContext getContextOrDie(Class<? extends ChannelHandler> handlerType) {
        DefaultChannelHandlerContext ctx = (DefaultChannelHandlerContext) context(handlerType);
        if (ctx == null || ctx == head) {
            throw new NoSuchElementException(handlerType.getName());
        } else {
            return ctx;
        }
    }

    @SuppressWarnings("rawtypes")
    private final class HeadHandler implements ChannelOutboundHandler {

        @Override
        public ChannelBufferHolder newOutboundBuffer(ChannelOutboundHandlerContext ctx) throws Exception {
            switch(channel.type()) {
                case STREAM:
                    return ChannelBufferHolders.byteBuffer();
                case MESSAGE:
                    return ChannelBufferHolders.messageBuffer();
                default:
                    throw new Error();
            }
        }

        @Override
        public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void afterRemove(ChannelHandlerContext ctx) throws Exception {
        }

        @Override
        public void bind(ChannelOutboundHandlerContext ctx, SocketAddress localAddress, ChannelFuture future) throws Exception {
            unsafe.bind(localAddress, future);
        }

        @Override
        public void connect(ChannelOutboundHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelFuture future) throws Exception {
            unsafe.connect(remoteAddress, localAddress, future);
        }

        @Override
        public void disconnect(ChannelOutboundHandlerContext ctx, ChannelFuture future) throws Exception {
            unsafe.disconnect(future);
        }

        @Override
        public void close(ChannelOutboundHandlerContext ctx, ChannelFuture future) throws Exception {
            unsafe.close(future);
        }

        @Override
        public void deregister(ChannelOutboundHandlerContext ctx, ChannelFuture future) throws Exception {
            unsafe.deregister(future);
        }

        @Override
        public void flush(ChannelOutboundHandlerContext ctx, ChannelFuture future) throws Exception {
            unsafe.flush(future);
        }
    }

    private abstract class ChannelPipelineModificationRunnable implements Runnable {

        private ChannelException cause;

        @Override
        public final void run() {
            try {
                runTask();
            } catch (Throwable t) {
                if (t instanceof ChannelException) {
                    cause = (ChannelException) t;
                } else {
                    this.cause = new ChannelException(t);
                }
            } finally {
                synchronized (ChannelPipelineModificationRunnable.this) {
                    notifyAll();
                }
            }
        }

        protected abstract void runTask();

        void await() {
            try {
                wait();
                if (cause != null) {
                    throw cause;
                }
            } catch (InterruptedException e) {
                throw new ChannelException(e);
            }
        }
    }
}