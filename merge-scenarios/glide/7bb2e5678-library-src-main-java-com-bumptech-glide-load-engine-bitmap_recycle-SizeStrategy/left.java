package com.bumptech.glide.load.engine.bitmap_recycle;

import android.annotation.TargetApi;
import android.graphics.Bitmap;
import android.os.Build;
import com.bumptech.glide.util.Util;
import java.util.TreeMap;

@TargetApi(Build.VERSION_CODES.KITKAT)
class SizeStrategy implements LruPoolStrategy {

    private static final int MAX_SIZE_MULTIPLE = 8;

    private final KeyPool keyPool = new KeyPool();

    private final GroupedLinkedMap<Key, Bitmap> groupedMap = new GroupedLinkedMap<>();

    private final TreeMap<Integer, Integer> sortedSizes = new PrettyPrintTreeMap<>();

    @Override
    public void put(Bitmap bitmap) {
        int size = Util.getBitmapByteSize(bitmap);
        final Key key = keyPool.get(size);
        groupedMap.put(key, bitmap);
        Integer current = sortedSizes.get(key.size);
        sortedSizes.put(key.size, current == null ? 1 : current + 1);
    }

    @Override
    public Bitmap get(int width, int height, Bitmap.Config config) {
        final int size = Util.getBitmapByteSize(width, height, config);
        Key key = keyPool.get(size);
        Integer possibleSize = sortedSizes.ceilingKey(size);
        if (possibleSize != null && possibleSize != size && possibleSize <= size * MAX_SIZE_MULTIPLE) {
            keyPool.offer(key);
            key = keyPool.get(possibleSize);
        }
        final Bitmap result = groupedMap.get(key);
        if (result != null) {
            result.reconfigure(width, height, config);
            decrementBitmapOfSize(possibleSize);
        }
        return result;
    }

    @Override
    public Bitmap removeLast() {
        Bitmap removed = groupedMap.removeLast();
        if (removed != null) {
            final int removedSize = Util.getBitmapByteSize(removed);
            decrementBitmapOfSize(removedSize);
        }
        return removed;
    }

    private void decrementBitmapOfSize(Integer size) {
        Integer current = sortedSizes.get(size);
        if (current == 1) {
            sortedSizes.remove(size);
        } else {
            sortedSizes.put(size, current - 1);
        }
    }

    @Override
    public String logBitmap(Bitmap bitmap) {
        return getBitmapString(bitmap);
    }

    @Override
    public String logBitmap(int width, int height, Bitmap.Config config) {
        int size = Util.getBitmapByteSize(width, height, config);
        return getBitmapString(size);
    }

    @Override
    public int getSize(Bitmap bitmap) {
        return Util.getBitmapByteSize(bitmap);
    }

    @Override
    public String toString() {
        return "SizeStrategy:\n  " + groupedMap + "\n" + "  SortedSizes" + sortedSizes;
    }

    private static class PrettyPrintTreeMap<K, V> extends TreeMap<K, V> {

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("( ");
            for (Entry<K, V> entry : entrySet()) {
                sb.append('{').append(entry.getKey()).append(':').append(entry.getValue()).append("}, ");
            }
            final String result;
            if (!isEmpty()) {
                result = sb.substring(0, sb.length() - 2);
            } else {
                result = sb.toString();
            }
            return result + " )";
        }
    }

    private static String getBitmapString(Bitmap bitmap) {
        int size = Util.getBitmapByteSize(bitmap);
        return getBitmapString(size);
    }

    private static String getBitmapString(int size) {
        return "[" + size + "]";
    }

    static class KeyPool extends BaseKeyPool<Key> {

        public Key get(int size) {
            Key result = get();
            result.init(size);
            return result;
        }

        @Override
        protected Key create() {
            return new Key(this);
        }
    }

    static final class Key implements Poolable {

        private final KeyPool pool;

        private int size;

        Key(KeyPool pool) {
            this.pool = pool;
        }

        public void init(int size) {
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Key) {
                Key other = (Key) o;
                return size == other.size;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return size;
        }

        @Override
        public String toString() {
            return getBitmapString(size);
        }

        @Override
        public void offer() {
            pool.offer(this);
        }
    }
}