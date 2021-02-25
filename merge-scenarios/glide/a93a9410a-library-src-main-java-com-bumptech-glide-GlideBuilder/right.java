package com.bumptech.glide;

import android.content.Context;
import android.graphics.Bitmap;
import android.os.Build;
import android.util.Log;
import com.bumptech.glide.load.DecodeFormat;
import com.bumptech.glide.load.engine.Engine;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPool;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPoolAdapter;
import com.bumptech.glide.load.engine.bitmap_recycle.ByteArrayPool;
import com.bumptech.glide.load.engine.bitmap_recycle.LruBitmapPool;
import com.bumptech.glide.load.engine.bitmap_recycle.LruByteArrayPool;
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

    private ByteArrayPool byteArrayPool;

    private MemoryCache memoryCache;

    private ExecutorService sourceService;

    private ExecutorService diskCacheService;

    private DecodeFormat decodeFormat;

    private DiskCache.Factory diskCacheFactory;

    private MemorySizeCalculator memorySizeCalculator;

    public GlideBuilder(Context context) {
        this.context = context.getApplicationContext();
    }

    public GlideBuilder setBitmapPool(BitmapPool bitmapPool) {
        this.bitmapPool = bitmapPool;
        return this;
    }

    public GlideBuilder setByteArrayPool(ByteArrayPool byteArrayPool) {
        this.byteArrayPool = byteArrayPool;
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

    public GlideBuilder setMemorySizeCalculator(MemorySizeCalculator.Builder builder) {
        return setMemorySizeCalculator(builder.build());
    }

    public GlideBuilder setMemorySizeCalculator(MemorySizeCalculator calculator) {
        this.memorySizeCalculator = calculator;
        return this;
    }

    GlideBuilder setEngine(Engine engine) {
        this.engine = engine;
        return this;
    }

    Glide createGlide() {
        if (sourceService == null) {
            final int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
            sourceService = new FifoPriorityThreadPoolExecutor("source", cores);
        }
        if (diskCacheService == null) {
            diskCacheService = new FifoPriorityThreadPoolExecutor("disk-cache", 1);
        }
        if (memorySizeCalculator == null) {
            memorySizeCalculator = new MemorySizeCalculator.Builder(context).build();
        }
        if (bitmapPool == null) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
                int size = memorySizeCalculator.getBitmapPoolSize();
                if (DecodeFormat.REQUIRE_ARGB_8888) {
                    bitmapPool = new LruBitmapPool(size, Collections.singleton(Bitmap.Config.ARGB_8888));
                } else {
                    bitmapPool = new LruBitmapPool(size);
                }
            } else {
                bitmapPool = new BitmapPoolAdapter();
            }
        }
        if (byteArrayPool == null) {
            byteArrayPool = new LruByteArrayPool();
        }
        if (memoryCache == null) {
            memoryCache = new LruResourceCache(memorySizeCalculator.getMemoryCacheSize());
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
        return new Glide(engine, memoryCache, bitmapPool, byteArrayPool, context, decodeFormat);
    }
}