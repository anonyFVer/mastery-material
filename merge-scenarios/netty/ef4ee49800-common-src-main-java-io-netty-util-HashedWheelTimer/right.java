package io.netty.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.internal.ConcurrentIdentityHashMap;
import io.netty.util.internal.DetectionUtil;
import io.netty.util.internal.ReusableIterator;
import io.netty.util.internal.SharedResourceMisuseDetector;

public class HashedWheelTimer implements Timer {

    static final InternalLogger logger = InternalLoggerFactory.getInstance(HashedWheelTimer.class);

    private static final SharedResourceMisuseDetector misuseDetector = new SharedResourceMisuseDetector(HashedWheelTimer.class);

    private final Worker worker = new Worker();

    final Thread workerThread;

    final AtomicBoolean shutdown = new AtomicBoolean();

    private final long roundDuration;

    final long tickDuration;

    final Set<HashedWheelTimeout>[] wheel;

    final ReusableIterator<HashedWheelTimeout>[] iterators;

    final int mask;

    final ReadWriteLock lock = new ReentrantReadWriteLock();

    volatile int wheelCursor;

    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel) {
        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        wheel = createWheel(ticksPerWheel);
        iterators = createIterators(wheel);
        mask = wheel.length - 1;
        this.tickDuration = tickDuration = unit.toMillis(tickDuration);
        if (tickDuration == Long.MAX_VALUE || tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException("tickDuration is too long: " + tickDuration + ' ' + unit);
        }
        roundDuration = tickDuration * wheel.length;
        workerThread = threadFactory.newThread(worker);
        misuseDetector.increase();
    }

    @SuppressWarnings("unchecked")
    private static Set<HashedWheelTimeout>[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException("ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }
        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        Set<HashedWheelTimeout>[] wheel = new Set[ticksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new MapBackedSet<HashedWheelTimeout>(new ConcurrentIdentityHashMap<HashedWheelTimeout, Boolean>(16, 0.95f, 4));
        }
        return wheel;
    }

    @SuppressWarnings("unchecked")
    private static ReusableIterator<HashedWheelTimeout>[] createIterators(Set<HashedWheelTimeout>[] wheel) {
        ReusableIterator<HashedWheelTimeout>[] iterators = new ReusableIterator[wheel.length];
        for (int i = 0; i < wheel.length; i++) {
            iterators[i] = (ReusableIterator<HashedWheelTimeout>) wheel[i].iterator();
        }
        return iterators;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    public synchronized void start() {
        if (shutdown.get()) {
            throw new IllegalStateException("cannot be started once stopped");
        }
        if (!workerThread.isAlive()) {
            workerThread.start();
        }
    }

    @Override
    public synchronized Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(HashedWheelTimer.class.getSimpleName() + ".stop() cannot be called from " + TimerTask.class.getSimpleName());
        }
        if (!shutdown.compareAndSet(false, true)) {
            return Collections.emptySet();
        }
        boolean interrupted = false;
        while (workerThread.isAlive()) {
            workerThread.interrupt();
            try {
                workerThread.join(100);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        misuseDetector.decrease();
        Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();
        for (Set<HashedWheelTimeout> bucket : wheel) {
            unprocessedTimeouts.addAll(bucket);
            bucket.clear();
        }
        return Collections.unmodifiableSet(unprocessedTimeouts);
    }

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        final long currentTime = System.currentTimeMillis();
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (!workerThread.isAlive()) {
            start();
        }
        delay = unit.toMillis(delay);
        HashedWheelTimeout timeout = new HashedWheelTimeout(task, currentTime + delay);
        scheduleTimeout(timeout, delay);
        return timeout;
    }

    void scheduleTimeout(HashedWheelTimeout timeout, long delay) {
        if (delay < tickDuration) {
            delay = tickDuration;
        }
        final long lastRoundDelay = delay % roundDuration;
        final long lastTickDelay = delay % tickDuration;
        final long relativeIndex = lastRoundDelay / tickDuration + (lastTickDelay != 0 ? 1 : 0);
        final long remainingRounds = delay / roundDuration - (delay % roundDuration == 0 ? 1 : 0);
        lock.readLock().lock();
        try {
            int stopIndex = (int) (wheelCursor + relativeIndex & mask);
            timeout.stopIndex = stopIndex;
            timeout.remainingRounds = remainingRounds;
            wheel[stopIndex].add(timeout);
        } finally {
            lock.readLock().unlock();
        }
    }

    private final class Worker implements Runnable {

        private long startTime;

        private long tick;

        Worker() {
        }

        @Override
        public void run() {
            List<HashedWheelTimeout> expiredTimeouts = new ArrayList<HashedWheelTimeout>();
            startTime = System.currentTimeMillis();
            tick = 1;
            while (!shutdown.get()) {
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    fetchExpiredTimeouts(expiredTimeouts, deadline);
                    notifyExpiredTimeouts(expiredTimeouts);
                }
            }
        }

        private void fetchExpiredTimeouts(List<HashedWheelTimeout> expiredTimeouts, long deadline) {
            lock.writeLock().lock();
            try {
                int newWheelCursor = wheelCursor = wheelCursor + 1 & mask;
                ReusableIterator<HashedWheelTimeout> i = iterators[newWheelCursor];
                fetchExpiredTimeouts(expiredTimeouts, i, deadline);
            } finally {
                lock.writeLock().unlock();
            }
        }

        private void fetchExpiredTimeouts(List<HashedWheelTimeout> expiredTimeouts, ReusableIterator<HashedWheelTimeout> i, long deadline) {
            List<HashedWheelTimeout> slipped = null;
            i.rewind();
            while (i.hasNext()) {
                HashedWheelTimeout timeout = i.next();
                if (timeout.remainingRounds <= 0) {
                    i.remove();
                    if (timeout.deadline <= deadline) {
                        expiredTimeouts.add(timeout);
                    } else {
                        if (slipped == null) {
                            slipped = new ArrayList<HashedWheelTimer.HashedWheelTimeout>();
                        }
                        slipped.add(timeout);
                    }
                } else {
                    timeout.remainingRounds--;
                }
            }
            if (slipped != null) {
                for (HashedWheelTimeout timeout : slipped) {
                    scheduleTimeout(timeout, timeout.deadline - deadline);
                }
            }
        }

        private void notifyExpiredTimeouts(List<HashedWheelTimeout> expiredTimeouts) {
            for (int i = expiredTimeouts.size() - 1; i >= 0; i--) {
                expiredTimeouts.get(i).expire();
            }
            expiredTimeouts.clear();
        }

        private long waitForNextTick() {
            long deadline = startTime + tickDuration * tick;
            for (; ; ) {
                final long currentTime = System.currentTimeMillis();
                long sleepTime = tickDuration * tick - (currentTime - startTime);
                if (DetectionUtil.isWindows()) {
                    sleepTime = (sleepTime / 10) * 10;
                }
                if (sleepTime <= 0) {
                    break;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    if (shutdown.get()) {
                        return -1;
                    }
                }
            }
            tick++;
            return deadline;
        }
    }

    private final class HashedWheelTimeout implements Timeout {

        private static final int ST_INIT = 0;

        private static final int ST_CANCELLED = 1;

        private static final int ST_EXPIRED = 2;

        private final TimerTask task;

        final long deadline;

        volatile int stopIndex;

        volatile long remainingRounds;

        private final AtomicInteger state = new AtomicInteger(ST_INIT);

        HashedWheelTimeout(TimerTask task, long deadline) {
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer getTimer() {
            return HashedWheelTimer.this;
        }

        @Override
        public TimerTask getTask() {
            return task;
        }

        @Override
        public void cancel() {
            if (!state.compareAndSet(ST_INIT, ST_CANCELLED)) {
                return;
            }
            wheel[stopIndex].remove(this);
        }

        @Override
        public boolean isCancelled() {
            return state.get() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state.get() != ST_INIT;
        }

        public void expire() {
            if (!state.compareAndSet(ST_INIT, ST_EXPIRED)) {
                return;
            }
            try {
                task.run(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + ".", t);
                }
            }
        }

        @Override
        public String toString() {
            long currentTime = System.currentTimeMillis();
            long remaining = deadline - currentTime;
            StringBuilder buf = new StringBuilder(192);
            buf.append(getClass().getSimpleName());
            buf.append('(');
            buf.append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining);
                buf.append(" ms later, ");
            } else if (remaining < 0) {
                buf.append(-remaining);
                buf.append(" ms ago, ");
            } else {
                buf.append("now, ");
            }
            if (isCancelled()) {
                buf.append(", cancelled");
            }
            return buf.append(')').toString();
        }
    }
}