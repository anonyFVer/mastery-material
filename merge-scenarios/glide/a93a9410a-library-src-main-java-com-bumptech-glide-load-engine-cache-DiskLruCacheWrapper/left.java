package com.bumptech.glide.load.engine.cache;

import android.util.Log;
import com.bumptech.glide.disklrucache.DiskLruCache;
import com.bumptech.glide.load.Key;
import java.io.File;
import java.io.IOException;

public class DiskLruCacheWrapper implements DiskCache {

    private static final String TAG = "DiskLruCacheWrapper";

    private static final int APP_VERSION = 1;

    private static final int VALUE_COUNT = 1;

    private static DiskLruCacheWrapper wrapper = null;

    private final DiskCacheWriteLocker writeLocker = new DiskCacheWriteLocker();

    private final SafeKeyGenerator safeKeyGenerator;

    private final File directory;

    private final int maxSize;

    private DiskLruCache diskLruCache;

    public static synchronized DiskCache get(File directory, int maxSize) {
        if (wrapper == null) {
            wrapper = new DiskLruCacheWrapper(directory, maxSize);
        }
        return wrapper;
    }

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

    private synchronized void resetDiskCache() {
        diskLruCache = null;
    }

    @Override
    public File get(Key key) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        File result = null;
        try {
            final DiskLruCache.Value value = getDiskCache().get(safeKey);
            if (value != null) {
                result = value.getFile(0);
            }
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unable to get from disk cache", e);
            }
        }
        return result;
    }

    @Override
    public void put(Key key, Writer writer) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        writeLocker.acquire(key);
        try {
            DiskLruCache.Editor editor = getDiskCache().edit(safeKey);
            if (editor != null) {
                try {
                    File file = editor.getFile(0);
                    if (writer.write(file)) {
                        editor.commit();
                    }
                } finally {
                    editor.abortUnlessCommitted();
                }
            }
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unable to put to disk cache", e);
            }
        } finally {
            writeLocker.release(key);
        }
    }

    @Override
    public void delete(Key key) {
        String safeKey = safeKeyGenerator.getSafeKey(key);
        try {
            getDiskCache().remove(safeKey);
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unable to delete from disk cache", e);
            }
        }
    }

    @Override
    public synchronized void clear() {
        try {
            getDiskCache().delete();
            resetDiskCache();
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Unable to clear disk cache", e);
            }
        }
    }
}