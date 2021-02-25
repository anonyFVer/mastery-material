package com.bumptech.glide.resize.cache;

import com.bumptech.glide.resize.SafeKeyGenerator;
import com.jakewharton.disklrucache.DiskLruCache;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DiskLruCacheWrapper implements DiskCache {

    private static final int APP_VERSION = 1;

    private static final int VALUE_COUNT = 1;

    private static DiskLruCache CACHE = null;

    private static DiskLruCacheWrapper WRAPPER = null;

    private final SafeKeyGenerator safeKeyGenerator;

    private synchronized static DiskLruCache getDiskLruCache(File directory, int maxSize) throws IOException {
        if (CACHE == null) {
            CACHE = DiskLruCache.open(directory, APP_VERSION, VALUE_COUNT, maxSize);
        }
        return CACHE;
    }

    public synchronized static DiskCache get(File directory, int maxSize) throws IOException {
        if (WRAPPER == null) {
            WRAPPER = new DiskLruCacheWrapper(getDiskLruCache(directory, maxSize));
        }
        return WRAPPER;
    }

    private final DiskLruCache diskLruCache;

    protected DiskLruCacheWrapper(DiskLruCache diskLruCache) {
        this.diskLruCache = diskLruCache;
        this.safeKeyGenerator = new SafeKeyGenerator();
    }

    @Override
    public InputStream get(String key) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        InputStream result = null;
        try {
            final DiskLruCache.Snapshot snapshot = diskLruCache.get(safeKey);
            if (snapshot != null) {
                result = snapshot.getInputStream(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void put(String key, Writer writer) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        try {
            DiskLruCache.Editor editor = diskLruCache.edit(safeKey);
            if (editor != null) {
                OutputStream os = null;
                try {
                    os = editor.newOutputStream(0);
                    writer.write(os);
                } finally {
                    if (os != null) {
                        os.close();
                    }
                }
                editor.commit();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(String key) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        try {
            diskLruCache.remove(safeKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}