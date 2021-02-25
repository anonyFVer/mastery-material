package com.bumptech.glide.resize.cache;

import com.bumptech.glide.resize.SafeKeyGenerator;
import android.util.Log;
import com.jakewharton.disklrucache.DiskLruCache;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DiskLruCacheWrapper implements DiskCache {

    private static final String TAG = "DiskLruCacheWrapper";

    private static final int APP_VERSION = 1;

    private static final int VALUE_COUNT = 1;

    private static DiskLruCacheWrapper WRAPPER = null;

    private final SafeKeyGenerator safeKeyGenerator;

    public synchronized static DiskCache get(File directory, int maxSize) {
        if (WRAPPER == null) {
            WRAPPER = new DiskLruCacheWrapper(directory, maxSize);
        }
        return WRAPPER;
    }

    private final File directory;

    private final int maxSize;

    private DiskLruCache diskLruCache;

    protected DiskLruCacheWrapper(File directory, int maxSize) {
        this.directory = directory;
        this.maxSize = maxSize;
        this.safeKeyGenerator = new SafeKeyGenerator();
    }

    private synchronized DiskLruCache getDiskCache() throws IOException {
        if (diskLruCache == null) {
            diskLruCache = DiskLruCache.open(directory, APP_VERSION, VALUE_COUNT, maxSize);
        }
        return diskLruCache;
    }

    @Override
    public InputStream get(String key) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        InputStream result = null;
        try {
            final DiskLruCache.Snapshot snapshot = getDiskCache().get(safeKey);
            if (snapshot != null) {
                result = snapshot.getInputStream(0);
            }
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unable to get from disk cache", e);
            }
        }
        return result;
    }

    @Override
    public void put(String key, Writer writer) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        try {
            DiskLruCache.Editor editor = getDiskCache().edit(safeKey);
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
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unable to put to disk cache", e);
            }
        }
    }

    @Override
    public void delete(String key) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        try {
            getDiskCache().remove(safeKey);
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unable to delete from disk cache", e);
            }
        }
    }
}