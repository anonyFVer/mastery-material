package com.bumptech.glide.resize;

import android.annotation.TargetApi;
import android.app.ActivityManager;
import android.content.Context;
import android.graphics.Bitmap;
import android.os.Build;
import android.os.Handler;
import android.os.HandlerThread;
import android.util.Log;
import com.bumptech.glide.resize.bitmap_recycle.BitmapPool;
import com.bumptech.glide.resize.bitmap_recycle.BitmapPoolAdapter;
import com.bumptech.glide.resize.bitmap_recycle.BitmapReferenceCounter;
import com.bumptech.glide.resize.bitmap_recycle.BitmapReferenceCounterAdapter;
import com.bumptech.glide.resize.bitmap_recycle.LruBitmapPool;
import com.bumptech.glide.resize.bitmap_recycle.SerialBitmapReferenceCounter;
import com.bumptech.glide.resize.cache.DiskCache;
import com.bumptech.glide.resize.cache.DiskCacheAdapter;
import com.bumptech.glide.resize.cache.DiskLruCacheWrapper;
import com.bumptech.glide.resize.cache.LruMemoryCache;
import com.bumptech.glide.resize.cache.MemoryCache;
import com.bumptech.glide.resize.cache.MemoryCacheAdapter;
import com.bumptech.glide.resize.load.Downsampler;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import static android.os.Process.THREAD_PRIORITY_BACKGROUND;

public class ImageManager {

    private static final String TAG = "ImageManager";

    private static final String DEFAULT_DISK_CACHE_DIR = "image_manager_disk_cache";

    private static final int DEFAULT_DISK_CACHE_SIZE = 250 * 1024 * 1024;

    private static final int DEFAULT_BITMAP_COMPRESS_QUALITY = 90;

    private static final float MEMORY_SIZE_RATIO = 1f / 10f;

    private static final boolean CAN_RECYCLE = Build.VERSION.SDK_INT >= 11;

    private final BitmapReferenceCounter bitmapReferenceCounter;

    private final int bitmapCompressQuality;

    private final BitmapPool bitmapPool;

    private final Map<String, ImageManagerJob> jobs = new HashMap<String, ImageManagerJob>();

    private final Bitmap.CompressFormat bitmapCompressFormat;

    private boolean shutdown = false;

    private final Handler mainHandler = new Handler();

    private final Handler bgHandler;

    private final ExecutorService executor;

    private final MemoryCache memoryCache;

    private final DiskCache diskCache;

    public static int getSafeMemoryCacheSize(Context context) {
        final ActivityManager activityManager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        return Math.round(MEMORY_SIZE_RATIO * activityManager.getMemoryClass() * 1024 * 1024);
    }

    public static File getPhotoCacheDir(Context context) {
        return getPhotoCacheDir(context, DEFAULT_DISK_CACHE_DIR);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File getPhotoCacheDir(Context context, String cacheName) {
        File cacheDir = context.getCacheDir();
        if (cacheDir != null) {
            File result = new File(cacheDir, cacheName);
            result.mkdirs();
            return result;
        }
        if (Log.isLoggable(TAG, Log.ERROR)) {
            Log.e(TAG, "default disk cache dir is null");
        }
        return null;
    }

    @SuppressWarnings("unused")
    public static class Builder {

        private final Context context;

        private ExecutorService resizeService = null;

        private MemoryCache memoryCache = null;

        private DiskCache diskCache = null;

        private Bitmap.CompressFormat bitmapCompressFormat = null;

        private boolean recycleBitmaps = CAN_RECYCLE;

        private BitmapPool bitmapPool;

        private BitmapReferenceCounter bitmapReferenceCounter;

        private int bitmapCompressQuality = DEFAULT_BITMAP_COMPRESS_QUALITY;

        public Builder(Context context) {
            if (context == null) {
                throw new NullPointerException("Context must not be null");
            }
            this.context = context;
            if (!CAN_RECYCLE) {
                bitmapPool = new BitmapPoolAdapter();
            }
        }

        public ImageManager build() {
            setDefaults();
            return new ImageManager(this);
        }

        public Builder setResizeService(ExecutorService resizeService) {
            this.resizeService = resizeService;
            return this;
        }

        public Builder setBitmapCompressFormat(Bitmap.CompressFormat bitmapCompressFormat) {
            this.bitmapCompressFormat = bitmapCompressFormat;
            return this;
        }

        public Builder setBitmapCompressQuality(int quality) {
            if (quality < 0) {
                throw new IllegalArgumentException("Bitmap compression quality must be >= 0");
            }
            this.bitmapCompressQuality = quality;
            return this;
        }

        public Builder setBitmapPool(BitmapPool bitmapPool) {
            if (CAN_RECYCLE) {
                this.bitmapPool = bitmapPool;
            }
            return this;
        }

        public Builder disableBitmapRecycling() {
            recycleBitmaps = false;
            return this;
        }

        public Builder setMemoryCache(MemoryCache memoryCache) {
            this.memoryCache = memoryCache;
            return this;
        }

        public Builder disableMemoryCache() {
            return setMemoryCache(new MemoryCacheAdapter());
        }

        public Builder setDiskCache(DiskCache diskCache) {
            this.diskCache = diskCache;
            return this;
        }

        public Builder disableDiskCache() {
            return setDiskCache(new DiskCacheAdapter());
        }

        private void setDefaults() {
            if (resizeService == null) {
                final int numThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
                resizeService = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

                    int threadNum = 1;

                    @Override
                    public Thread newThread(Runnable runnable) {
                        final Thread result = new Thread(runnable, "image-manager-resize-" + threadNum);
                        threadNum++;
                        result.setPriority(THREAD_PRIORITY_BACKGROUND);
                        return result;
                    }
                });
            }
            final int safeCacheSize = getSafeMemoryCacheSize(context);
            final boolean isLowMemoryDevice = isLowMemoryDevice(context);
            if (memoryCache == null) {
                memoryCache = new LruMemoryCache(!isLowMemoryDevice && recycleBitmaps ? safeCacheSize / 2 : safeCacheSize);
            }
            if (diskCache == null) {
                File cacheDir = getPhotoCacheDir(context);
                if (cacheDir != null) {
                    try {
                        diskCache = DiskLruCacheWrapper.get(cacheDir, DEFAULT_DISK_CACHE_SIZE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (diskCache == null) {
                    diskCache = new DiskCacheAdapter();
                }
            }
            if (!recycleBitmaps) {
                bitmapPool = new BitmapPoolAdapter();
                bitmapReferenceCounter = new BitmapReferenceCounterAdapter();
            } else {
                if (bitmapPool == null) {
                    bitmapPool = new LruBitmapPool(isLowMemoryDevice ? safeCacheSize : 2 * safeCacheSize);
                }
                bitmapReferenceCounter = new SerialBitmapReferenceCounter(bitmapPool);
            }
        }
    }

    @TargetApi(19)
    private static boolean isLowMemoryDevice(Context context) {
        final ActivityManager activityManager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        return Build.VERSION.SDK_INT < 11 || (Build.VERSION.SDK_INT >= 19 && activityManager.isLowRamDevice());
    }

    private ImageManager(Builder builder) {
        HandlerThread bgThread = new HandlerThread("image_manager_thread", THREAD_PRIORITY_BACKGROUND);
        bgThread.start();
        bgHandler = new Handler(bgThread.getLooper());
        executor = builder.resizeService;
        bitmapCompressFormat = builder.bitmapCompressFormat;
        bitmapCompressQuality = builder.bitmapCompressQuality;
        memoryCache = builder.memoryCache;
        diskCache = builder.diskCache;
        bitmapReferenceCounter = builder.bitmapReferenceCounter;
        bitmapPool = builder.bitmapPool;
        memoryCache.setImageRemovedListener(new MemoryCache.ImageRemovedListener() {

            @Override
            public void onImageRemoved(Bitmap removed) {
                releaseBitmap(removed);
            }
        });
    }

    public DiskCache getDiskCache() {
        return diskCache;
    }

    public BitmapPool getBitmapPool() {
        return bitmapPool;
    }

    public LoadToken getImage(BitmapLoad task, LoadedCallback cb) {
        if (shutdown)
            return null;
        final String key = task.getId();
        LoadToken result = null;
        if (!returnFromCache(key, cb)) {
            ImageManagerJob job = jobs.get(key);
            if (job == null) {
                ImageManagerRunner runner = new ImageManagerRunner(key, task, task.getId());
                job = new ImageManagerJob(runner, key, task.getId());
                jobs.put(key, job);
                job.addCallback(cb);
                runner.execute();
            } else {
                job.addCallback(cb);
            }
            result = new LoadToken(cb, job, task.getId());
        }
        return result;
    }

    public void releaseBitmap(final Bitmap b) {
        bitmapReferenceCounter.releaseBitmap(b);
    }

    public void clearMemory() {
        memoryCache.clearMemory();
        bitmapPool.clearMemory();
    }

    public void trimMemory(int level) {
        memoryCache.trimMemory(level);
        bitmapPool.trimMemory(level);
    }

    @SuppressWarnings("unused")
    public void shutdown() {
        shutdown = true;
        executor.shutdown();
        bgHandler.getLooper().quit();
    }

    private boolean returnFromCache(String key, LoadedCallback cb) {
        Bitmap inCache = memoryCache.get(key);
        boolean found = inCache != null;
        if (found) {
            bitmapReferenceCounter.acquireBitmap(inCache);
            cb.onLoadCompleted(inCache);
        }
        return found;
    }

    private class ImageManagerJob {

        private final ImageManagerRunner runner;

        private final String key;

        private final String tag;

        private final List<LoadedCallback> cbs = new ArrayList<LoadedCallback>();

        public ImageManagerJob(ImageManagerRunner runner, String key, String tag) {
            this.runner = runner;
            this.key = key;
            this.tag = tag;
        }

        public void addCallback(LoadedCallback cb) {
            cbs.add(cb);
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "add callback for tag: " + tag + " total: " + cbs.size() + " ImageManagerJob: " + hashCode());
            }
        }

        public void cancel(LoadedCallback cb) {
            cbs.remove(cb);
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Cancel callback from ImageManagerJob for tag: " + tag + " total cbs: " + cbs.size() + " ImageManagerJob: " + hashCode());
            }
            if (cbs.size() == 0) {
                runner.cancel();
                jobs.remove(key);
            }
        }

        public void onLoadComplete(Bitmap result) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Load complete in ImageManagerJob for tag: " + tag + " total cbs: " + cbs.size() + " ImageManagerJob: " + hashCode());
            }
            for (LoadedCallback cb : cbs) {
                bitmapReferenceCounter.acquireBitmap(result);
                cb.onLoadCompleted(result);
            }
            jobs.remove(key);
        }

        public void onLoadFailed(Exception e) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Load failed in ImageManagerJob for tag: " + tag + " total cbs: " + cbs.size() + " ImageManagerJob: " + hashCode());
            }
            for (LoadedCallback cb : cbs) {
                cb.onLoadFailed(e);
            }
            jobs.remove(key);
        }
    }

    private void putInDiskCache(String key, final Bitmap bitmap) {
        diskCache.put(key, new DiskCache.Writer() {

            @Override
            public void write(OutputStream os) {
                Bitmap.CompressFormat compressFormat = getCompressFormat(bitmap);
                bitmap.compress(compressFormat, bitmapCompressQuality, os);
            }
        });
    }

    private Bitmap.CompressFormat getCompressFormat(Bitmap bitmap) {
        final Bitmap.CompressFormat format;
        if (bitmapCompressFormat != null) {
            format = bitmapCompressFormat;
        } else {
            if (bitmap.getConfig() == Bitmap.Config.RGB_565 || !bitmap.hasAlpha()) {
                format = Bitmap.CompressFormat.JPEG;
            } else {
                format = Bitmap.CompressFormat.PNG;
            }
        }
        return format;
    }

    private void putInMemoryCache(String key, final Bitmap bitmap) {
        final boolean inCache;
        inCache = memoryCache.contains(key);
        if (!inCache) {
            bitmapReferenceCounter.acquireBitmap(bitmap);
            memoryCache.put(key, bitmap);
        }
    }

    private class ImageManagerRunner implements Runnable {

        public final String key;

        private final BitmapLoad task;

        private final String tag;

        private volatile Future<?> future;

        private boolean isCancelled = false;

        public ImageManagerRunner(String key, BitmapLoad task, String tag) {
            this.key = key;
            this.task = task;
            this.tag = tag;
        }

        private void execute() {
            bgHandler.post(this);
        }

        public void cancel() {
            if (isCancelled) {
                return;
            }
            isCancelled = true;
            bgHandler.removeCallbacks(this);
            final Future current = future;
            if (current != null) {
                current.cancel(false);
            }
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Cancel job id: " + tag);
            }
            task.cancel();
        }

        @Override
        public void run() {
            Bitmap result = null;
            try {
                result = getFromDiskCache(key);
            } catch (Exception e) {
                handleException(e);
            }
            if (result == null) {
                try {
                    resizeWithPool();
                } catch (Exception e) {
                    handleException(e);
                }
            } else {
                finishResize(result, true);
            }
        }

        private Bitmap getFromDiskCache(String key) {
            Bitmap result = null;
            final InputStream is = diskCache.get(key);
            if (is != null) {
                result = Downsampler.NONE.decode(is, bitmapPool, -1, -1);
                if (result == null) {
                    diskCache.delete(key);
                }
            }
            return result;
        }

        private void resizeWithPool() {
            future = executor.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        Bitmap result = decodeIfNotFound();
                        finishResize(result, false);
                    } catch (Exception e) {
                        handleException(e);
                    }
                }
            });
        }

        private Bitmap decodeIfNotFound() throws Exception {
            return task.load(bitmapPool);
        }

        private void finishResize(final Bitmap result, boolean isInDiskCache) {
            if (result != null) {
                if (!isInDiskCache) {
                    putInDiskCache(key, result);
                }
                mainHandler.post(new Runnable() {

                    @Override
                    public void run() {
                        bitmapReferenceCounter.acquireBitmap(result);
                        putInMemoryCache(key, result);
                        final ImageManagerJob job = jobs.get(key);
                        if (job != null) {
                            job.onLoadComplete(result);
                        }
                        bitmapReferenceCounter.releaseBitmap(result);
                    }
                });
            } else {
                handleException(null);
            }
        }

        private void handleException(final Exception e) {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Exception loading image tag: " + tag, e);
            }
            mainHandler.post(new Runnable() {

                @Override
                public void run() {
                    if (isCancelled) {
                        return;
                    }
                    final ImageManagerJob job = jobs.get(key);
                    if (job != null) {
                        job.onLoadFailed(e);
                    }
                }
            });
        }
    }

    public static class LoadToken {

        private final ImageManagerJob job;

        private final LoadedCallback cb;

        private final String tag;

        public LoadToken(LoadedCallback cb, ImageManagerJob job, String tag) {
            this.cb = cb;
            this.job = job;
            this.tag = tag;
        }

        public void cancel() {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Cancel load token tag: " + tag + " cb: " + cb.hashCode());
            }
            job.cancel(cb);
        }
    }
}