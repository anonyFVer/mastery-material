package com.bumptech.glide;

import android.content.Context;
import android.graphics.Bitmap;
import android.os.Build;
import android.util.Log;
import com.bumptech.glide.load.DecodeFormat;
import com.bumptech.glide.load.engine.Engine;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPool;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPoolAdapter;
import com.bumptech.glide.load.engine.bitmap_recycle.LruBitmapPool;
import com.bumptech.glide.load.engine.cache.DiskCache;
import com.bumptech.glide.load.engine.cache.InternalCacheDiskCacheFactory;
import com.bumptech.glide.load.engine.cache.LruResourceCache;
import com.bumptech.glide.load.engine.cache.MemoryCache;
import com.bumptech.glide.load.engine.cache.MemorySizeCalculator;
import com.bumptech.glide.load.engine.executor.FifoPriorityThreadPoolExecutor;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

public class GlideBuilder {

    private static final String TAG = "Glide";

    private final Context context;

    private Engine engine;

    private BitmapPool bitmapPool;

    private MemoryCache memoryCache;

    private ExecutorService sourceService;

    private ExecutorService diskCacheService;

    private DecodeFormat decodeFormat;

    private DiskCache.Factory diskCacheFactory;

    public GlideBuilder(Context context) {
        this.context = context.getApplicationContext();
    }

    public GlideBuilder setBitmapPool(BitmapPool bitmapPool) {
        this.bitmapPool = bitmapPool;
        return this;
    }

    public GlideBuilder setMemoryCache(MemoryCache memoryCache) {
        this.memoryCache = memoryCache;
        return this;
    }

    @Deprecated
    public GlideBuilder setDiskCache(final DiskCache diskCache) {
        return setDiskCache(new DiskCache.Factory() {

            @Override
            public DiskCache build() {
                return diskCache;
            }
        });
    }

    public GlideBuilder setDiskCache(DiskCache.Factory diskCacheFactory) {
        this.diskCacheFactory = diskCacheFactory;
        return this;
    }

    public GlideBuilder setResizeService(ExecutorService service) {
        this.sourceService = service;
        return this;
    }

    public GlideBuilder setDiskCacheService(ExecutorService service) {
        this.diskCacheService = service;
        return this;
    }

    public GlideBuilder setDecodeFormat(DecodeFormat decodeFormat) {
        if (DecodeFormat.REQUIRE_ARGB_8888 && decodeFormat != DecodeFormat.PREFER_ARGB_8888) {
            this.decodeFormat = DecodeFormat.PREFER_ARGB_8888;
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unsafe to use RGB_565 on KitKat or Lollipop, ignoring setDecodeFormat");
            }
        } else {
            this.decodeFormat = decodeFormat;
        }
        return this;
    }

    GlideBuilder setEngine(Engine engine) {
        this.engine = engine;
        return this;
    }

    Glide createGlide() {
        if (sourceService == null) {
            final int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
            sourceService = new FifoPriorityThreadPoolExecutor(cores);
        }
        if (diskCacheService == null) {
            diskCacheService = new FifoPriorityThreadPoolExecutor(1);
        }
        MemorySizeCalculator calculator = new MemorySizeCalculator(context);
        if (bitmapPool == null) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
                int size = calculator.getBitmapPoolSize();
                if (DecodeFormat.REQUIRE_ARGB_8888) {
                    bitmapPool = new LruBitmapPool(size, Collections.singleton(Bitmap.Config.ARGB_8888));
                } else {
                    bitmapPool = new LruBitmapPool(size);
                }
            } else {
                bitmapPool = new BitmapPoolAdapter();
            }
        }
        if (memoryCache == null) {
            memoryCache = new LruResourceCache(calculator.getMemoryCacheSize());
        }
        if (diskCacheFactory == null) {
            diskCacheFactory = new InternalCacheDiskCacheFactory(context, Glide.DEFAULT_DISK_CACHE_SIZE);
        }
        if (engine == null) {
            engine = new Engine(memoryCache, diskCacheFactory, diskCacheService, sourceService);
        }
        if (decodeFormat == null) {
            decodeFormat = DecodeFormat.DEFAULT;
        }
        return new Glide(engine, memoryCache, bitmapPool, context, decodeFormat);
    }
}