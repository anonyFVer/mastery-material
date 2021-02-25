package com.bumptech.glide.load.engine.cache;

import com.bumptech.glide.load.Key;
import java.io.File;

public interface DiskCache {

    interface Factory {

        int DEFAULT_DISK_CACHE_SIZE = 250 * 1024 * 1024;

        String DEFAULT_DISK_CACHE_DIR = "image_manager_disk_cache";

        DiskCache build();
    }

    interface Writer {

        boolean write(File file);
    }

    File get(Key key);

    void put(Key key, Writer writer);

    void delete(Key key);

    void clear();
}