package com.bumptech.glide.load.engine;

import android.os.Looper;
import android.os.MessageQueue;
import android.util.Log;
import com.bumptech.glide.load.Key;
import com.bumptech.glide.load.engine.cache.DiskCache;
import com.bumptech.glide.load.engine.cache.DiskCacheAdapter;
import com.bumptech.glide.load.engine.cache.MemoryCache;
import com.bumptech.glide.request.ResourceCallback;
import com.bumptech.glide.util.LogTime;
import com.bumptech.glide.util.Util;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class Engine implements EngineJobListener, MemoryCache.ResourceRemovedListener, EngineResource.ResourceListener {

    private static final String TAG = "Engine";

    private final Map<Key, EngineJob> jobs;

    private final EngineKeyFactory keyFactory;

    private final MemoryCache cache;

    private final EngineJobFactory engineJobFactory;

    private final Map<Key, WeakReference<EngineResource<?>>> activeResources;

    private final ResourceRecycler resourceRecycler;

    private final LazyDiskCacheProvider diskCacheProvider;

    private ReferenceQueue<EngineResource<?>> resourceReferenceQueue;

    public static class LoadStatus {

        private final EngineJob engineJob;

        private final ResourceCallback cb;

        public LoadStatus(ResourceCallback cb, EngineJob engineJob) {
            this.cb = cb;
            this.engineJob = engineJob;
        }

        public void cancel() {
            engineJob.removeCallback(cb);
        }
    }

    public Engine(MemoryCache memoryCache, DiskCache.Factory diskCacheFactory, ExecutorService diskCacheService, ExecutorService sourceService) {
        this(memoryCache, diskCacheFactory, diskCacheService, sourceService, null, null, null, null, null);
    }

    Engine(MemoryCache cache, DiskCache.Factory diskCacheFactory, ExecutorService diskCacheService, ExecutorService sourceService, Map<Key, EngineJob> jobs, EngineKeyFactory keyFactory, Map<Key, WeakReference<EngineResource<?>>> activeResources, EngineJobFactory engineJobFactory, ResourceRecycler resourceRecycler) {
        this.cache = cache;
        this.diskCacheProvider = new LazyDiskCacheProvider(diskCacheFactory);
        if (activeResources == null) {
            activeResources = new HashMap<>();
        }
        this.activeResources = activeResources;
        if (keyFactory == null) {
            keyFactory = new EngineKeyFactory();
        }
        this.keyFactory = keyFactory;
        if (jobs == null) {
            jobs = new HashMap<>();
        }
        this.jobs = jobs;
        if (engineJobFactory == null) {
            engineJobFactory = new EngineJobFactory(diskCacheService, sourceService, this);
        }
        this.engineJobFactory = engineJobFactory;
        if (resourceRecycler == null) {
            resourceRecycler = new ResourceRecycler();
        }
        this.resourceRecycler = resourceRecycler;
        cache.setResourceRemovedListener(this);
    }

    public <Z, R> LoadStatus load(RequestContext<?, R> requestContext, int width, int height, ResourceCallback cb) {
        Util.assertMainThread();
        long startTime = LogTime.getLogTime();
        requestContext.setDimens(width, height);
        EngineKey key = keyFactory.buildKey(requestContext, width, height);
        EngineResource<?> cached = loadFromCache(key, requestContext.isMemoryCacheable());
        if (cached != null) {
            cb.onResourceReady(cached);
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                logWithTimeAndKey("Loaded resource from cache", startTime, key);
            }
            return null;
        }
        EngineResource<?> active = loadFromActiveResources(key, requestContext.isMemoryCacheable());
        if (active != null) {
            cb.onResourceReady(active);
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                logWithTimeAndKey("Loaded resource from active resources", startTime, key);
            }
            return null;
        }
        EngineJob current = jobs.get(key);
        if (current != null) {
            current.addCallback(cb);
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                logWithTimeAndKey("Added to existing load", startTime, key);
            }
            return new LoadStatus(cb, current);
        }
        EngineJob<R> engineJob = engineJobFactory.build(key, requestContext.isMemoryCacheable());
        DecodeJob<R> decodeJob = new DecodeJob<>(requestContext, key, width, height, diskCacheProvider, engineJob);
        jobs.put(key, engineJob);
        engineJob.addCallback(cb);
        engineJob.start(decodeJob);
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            logWithTimeAndKey("Started new load", startTime, key);
        }
        return new LoadStatus(cb, engineJob);
    }

    private static void logWithTimeAndKey(String log, long startTime, Key key) {
        Log.v(TAG, log + " in " + LogTime.getElapsedMillis(startTime) + "ms, key: " + key);
    }

    private EngineResource<?> loadFromActiveResources(Key key, boolean isMemoryCacheable) {
        if (!isMemoryCacheable) {
            return null;
        }
        EngineResource<?> active = null;
        WeakReference<EngineResource<?>> activeRef = activeResources.get(key);
        if (activeRef != null) {
            active = activeRef.get();
            if (active != null) {
                active.acquire();
            } else {
                activeResources.remove(key);
            }
        }
        return active;
    }

    private EngineResource<?> loadFromCache(Key key, boolean isMemoryCacheable) {
        if (!isMemoryCacheable) {
            return null;
        }
        EngineResource<?> cached = getEngineResourceFromCache(key);
        if (cached != null) {
            cached.acquire();
            activeResources.put(key, new ResourceWeakReference(key, cached, getReferenceQueue()));
        }
        return cached;
    }

    @SuppressWarnings("unchecked")
    private EngineResource<?> getEngineResourceFromCache(Key key) {
        Resource<?> cached = cache.remove(key);
        final EngineResource result;
        if (cached == null) {
            result = null;
        } else if (cached instanceof EngineResource) {
            result = (EngineResource) cached;
        } else {
            result = new EngineResource(cached, true);
        }
        return result;
    }

    public void release(Resource resource) {
        Util.assertMainThread();
        if (resource instanceof EngineResource) {
            ((EngineResource) resource).release();
        } else {
            throw new IllegalArgumentException("Cannot release anything but an EngineResource");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onEngineJobComplete(Key key, EngineResource<?> resource) {
        Util.assertMainThread();
        if (resource != null) {
            resource.setResourceListener(key, this);
            if (resource.isCacheable()) {
                activeResources.put(key, new ResourceWeakReference(key, resource, getReferenceQueue()));
            }
        }
        jobs.remove(key);
    }

    @Override
    public void onEngineJobCancelled(EngineJob engineJob, Key key) {
        Util.assertMainThread();
        EngineJob current = jobs.get(key);
        if (engineJob.equals(current)) {
            jobs.remove(key);
        }
    }

    @Override
    public void onResourceRemoved(final Resource<?> resource) {
        Util.assertMainThread();
        resourceRecycler.recycle(resource);
    }

    @Override
    public void onResourceReleased(Key cacheKey, EngineResource resource) {
        Util.assertMainThread();
        activeResources.remove(cacheKey);
        if (resource.isCacheable()) {
            cache.put(cacheKey, resource);
        } else {
            resourceRecycler.recycle(resource);
        }
    }

    public void clearDiskCache() {
        diskCacheProvider.getDiskCache().clear();
    }

    private ReferenceQueue<EngineResource<?>> getReferenceQueue() {
        if (resourceReferenceQueue == null) {
            resourceReferenceQueue = new ReferenceQueue<>();
            MessageQueue queue = Looper.myQueue();
            queue.addIdleHandler(new RefQueueIdleHandler(activeResources, resourceReferenceQueue));
        }
        return resourceReferenceQueue;
    }

    private static class LazyDiskCacheProvider implements DecodeJob.DiskCacheProvider {

        private final DiskCache.Factory factory;

        private volatile DiskCache diskCache;

        public LazyDiskCacheProvider(DiskCache.Factory factory) {
            this.factory = factory;
        }

        @Override
        public DiskCache getDiskCache() {
            if (diskCache == null) {
                synchronized (this) {
                    if (diskCache == null) {
                        diskCache = factory.build();
                    }
                    if (diskCache == null) {
                        diskCache = new DiskCacheAdapter();
                    }
                }
            }
            return diskCache;
        }
    }

    private static class ResourceWeakReference extends WeakReference<EngineResource<?>> {

        private final Key key;

        public ResourceWeakReference(Key key, EngineResource<?> r, ReferenceQueue<? super EngineResource<?>> q) {
            super(r, q);
            this.key = key;
        }
    }

    private static class RefQueueIdleHandler implements MessageQueue.IdleHandler {

        private final Map<Key, WeakReference<EngineResource<?>>> activeResources;

        private final ReferenceQueue<EngineResource<?>> queue;

        public RefQueueIdleHandler(Map<Key, WeakReference<EngineResource<?>>> activeResources, ReferenceQueue<EngineResource<?>> queue) {
            this.activeResources = activeResources;
            this.queue = queue;
        }

        @Override
        public boolean queueIdle() {
            ResourceWeakReference ref = (ResourceWeakReference) queue.poll();
            if (ref != null) {
                activeResources.remove(ref.key);
            }
            return true;
        }
    }

    static class EngineJobFactory {

        private final ExecutorService diskCacheService;

        private final ExecutorService sourceService;

        private final EngineJobListener listener;

        public EngineJobFactory(ExecutorService diskCacheService, ExecutorService sourceService, EngineJobListener listener) {
            this.diskCacheService = diskCacheService;
            this.sourceService = sourceService;
            this.listener = listener;
        }

        public <R> EngineJob<R> build(Key key, boolean isMemoryCacheable) {
            return new EngineJob<>(key, diskCacheService, sourceService, isMemoryCacheable, listener);
        }
    }
}