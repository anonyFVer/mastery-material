package com.bumptech.glide.load.resource.bitmap;

import static com.bumptech.glide.load.resource.bitmap.ImageHeaderParser.ImageType.GIF;
import static com.bumptech.glide.load.resource.bitmap.ImageHeaderParser.ImageType.JPEG;
import static com.bumptech.glide.load.resource.bitmap.ImageHeaderParser.ImageType.PNG;
import static com.bumptech.glide.load.resource.bitmap.ImageHeaderParser.ImageType.PNG_A;
import static com.bumptech.glide.load.resource.bitmap.ImageHeaderParser.ImageType.UNKNOWN;
import android.util.Log;
import com.bumptech.glide.load.engine.bitmap_recycle.ByteArrayPool;
import com.bumptech.glide.util.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

public class ImageHeaderParser {

    private static final String TAG = "ImageHeaderParser";

    public enum ImageType {

        GIF(true), JPEG(false), PNG_A(true), PNG(false), UNKNOWN(false);

        private final boolean hasAlpha;

        ImageType(boolean hasAlpha) {
            this.hasAlpha = hasAlpha;
        }

        public boolean hasAlpha() {
            return hasAlpha;
        }
    }

    private static final int GIF_HEADER = 0x474946;

    private static final int PNG_HEADER = 0x89504E47;

    private static final int EXIF_MAGIC_NUMBER = 0xFFD8;

    private static final int MOTOROLA_TIFF_MAGIC_NUMBER = 0x4D4D;

    private static final int INTEL_TIFF_MAGIC_NUMBER = 0x4949;

    private static final String JPEG_EXIF_SEGMENT_PREAMBLE = "Exif\0\0";

    private static final byte[] JPEG_EXIF_SEGMENT_PREAMBLE_BYTES = JPEG_EXIF_SEGMENT_PREAMBLE.getBytes(Charset.forName("UTF-8"));

    private static final int SEGMENT_SOS = 0xDA;

    private static final int MARKER_EOI = 0xD9;

    private static final int SEGMENT_START_ID = 0xFF;

    private static final int EXIF_SEGMENT_TYPE = 0xE1;

    private static final int ORIENTATION_TAG_TYPE = 0x0112;

    private static final int[] BYTES_PER_FORMAT = { 0, 1, 1, 2, 4, 8, 1, 1, 2, 4, 8, 4, 8 };

    private final ByteArrayPool byteArrayPool;

    private final Reader reader;

    public ImageHeaderParser(InputStream is, ByteArrayPool byteArrayPool) {
        Preconditions.checkNotNull(is);
        this.byteArrayPool = Preconditions.checkNotNull(byteArrayPool);
        reader = new StreamReader(is);
    }

    public ImageHeaderParser(ByteBuffer byteBuffer, ByteArrayPool byteArrayPool) {
        Preconditions.checkNotNull(byteBuffer);
        this.byteArrayPool = Preconditions.checkNotNull(byteArrayPool);
        reader = new ByteBufferReader(byteBuffer);
    }

    public boolean hasAlpha() throws IOException {
        return getType().hasAlpha();
    }

    public ImageType getType() throws IOException {
        int firstTwoBytes = reader.getUInt16();
        if (firstTwoBytes == EXIF_MAGIC_NUMBER) {
            return JPEG;
        }
        final int firstFourBytes = firstTwoBytes << 16 & 0xFFFF0000 | reader.getUInt16() & 0xFFFF;
        if (firstFourBytes == PNG_HEADER) {
            reader.skip(25 - 4);
            int alpha = reader.getByte();
            return alpha >= 3 ? PNG_A : PNG;
        }
        if (firstFourBytes >> 8 == GIF_HEADER) {
            return GIF;
        }
        return UNKNOWN;
    }

    public int getOrientation() throws IOException {
        final int magicNumber = reader.getUInt16();
        if (!handles(magicNumber)) {
            return -1;
        } else {
            byte[] exifData = getExifSegment();
            boolean hasJpegExifPreamble = exifData != null && exifData.length > JPEG_EXIF_SEGMENT_PREAMBLE_BYTES.length;
            if (hasJpegExifPreamble) {
                for (int i = 0; i < JPEG_EXIF_SEGMENT_PREAMBLE_BYTES.length; i++) {
                    if (exifData[i] != JPEG_EXIF_SEGMENT_PREAMBLE_BYTES[i]) {
                        hasJpegExifPreamble = false;
                        break;
                    }
                }
            }
            if (hasJpegExifPreamble) {
                return parseExifSegment(new RandomAccessReader(exifData));
            } else {
                return -1;
            }
        }
    }

    private byte[] getExifSegment() throws IOException {
        short segmentId, segmentType;
        int segmentLength;
        while (true) {
            segmentId = reader.getUInt8();
            if (segmentId != SEGMENT_START_ID) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Unknown segmentId=" + segmentId);
                }
                return null;
            }
            segmentType = reader.getUInt8();
            if (segmentType == SEGMENT_SOS) {
                return null;
            } else if (segmentType == MARKER_EOI) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Found MARKER_EOI in exif segment");
                }
                return null;
            }
            segmentLength = reader.getUInt16() - 2;
            if (segmentType != EXIF_SEGMENT_TYPE) {
                long skipped = reader.skip(segmentLength);
                if (skipped != segmentLength) {
                    if (Log.isLoggable(TAG, Log.DEBUG)) {
                        Log.d(TAG, "Unable to skip enough data" + ", type: " + segmentType + ", wanted to skip: " + segmentLength + ", but actually skipped: " + skipped);
                    }
                    return null;
                }
            } else {
                byte[] segmentData = byteArrayPool.get(segmentLength);
                try {
                    int read = reader.read(segmentData);
                    if (read != segmentLength) {
                        if (Log.isLoggable(TAG, Log.DEBUG)) {
                            Log.d(TAG, "Unable to read segment data" + ", type: " + segmentType + ", length: " + segmentLength + ", actually read: " + read);
                        }
                        return null;
                    } else {
                        return segmentData;
                    }
                } finally {
                    byteArrayPool.put(segmentData);
                }
            }
        }
    }

    private static int parseExifSegment(RandomAccessReader segmentData) {
        final int headerOffsetSize = JPEG_EXIF_SEGMENT_PREAMBLE.length();
        short byteOrderIdentifier = segmentData.getInt16(headerOffsetSize);
        final ByteOrder byteOrder;
        if (byteOrderIdentifier == MOTOROLA_TIFF_MAGIC_NUMBER) {
            byteOrder = ByteOrder.BIG_ENDIAN;
        } else if (byteOrderIdentifier == INTEL_TIFF_MAGIC_NUMBER) {
            byteOrder = ByteOrder.LITTLE_ENDIAN;
        } else {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Unknown endianness = " + byteOrderIdentifier);
            }
            byteOrder = ByteOrder.BIG_ENDIAN;
        }
        segmentData.order(byteOrder);
        int firstIfdOffset = segmentData.getInt32(headerOffsetSize + 4) + headerOffsetSize;
        int tagCount = segmentData.getInt16(firstIfdOffset);
        int tagOffset, tagType, formatCode, componentCount;
        for (int i = 0; i < tagCount; i++) {
            tagOffset = calcTagOffset(firstIfdOffset, i);
            tagType = segmentData.getInt16(tagOffset);
            if (tagType != ORIENTATION_TAG_TYPE) {
                continue;
            }
            formatCode = segmentData.getInt16(tagOffset + 2);
            if (formatCode < 1 || formatCode > 12) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Got invalid format code = " + formatCode);
                }
                continue;
            }
            componentCount = segmentData.getInt32(tagOffset + 4);
            if (componentCount < 0) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Negative tiff component count");
                }
                continue;
            }
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Got tagIndex=" + i + " tagType=" + tagType + " formatCode =" + formatCode + " componentCount=" + componentCount);
            }
            final int byteCount = componentCount + BYTES_PER_FORMAT[formatCode];
            if (byteCount > 4) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Got byte count > 4, not orientation, continuing, formatCode=" + formatCode);
                }
                continue;
            }
            final int tagValueOffset = tagOffset + 8;
            if (tagValueOffset < 0 || tagValueOffset > segmentData.length()) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Illegal tagValueOffset=" + tagValueOffset + " tagType=" + tagType);
                }
                continue;
            }
            if (byteCount < 0 || tagValueOffset + byteCount > segmentData.length()) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Illegal number of bytes for TI tag data tagType=" + tagType);
                }
                continue;
            }
            return segmentData.getInt16(tagValueOffset);
        }
        return -1;
    }

    private static int calcTagOffset(int ifdOffset, int tagIndex) {
        return ifdOffset + 2 + 12 * tagIndex;
    }

    private static boolean handles(int imageMagicNumber) {
        return (imageMagicNumber & EXIF_MAGIC_NUMBER) == EXIF_MAGIC_NUMBER || imageMagicNumber == MOTOROLA_TIFF_MAGIC_NUMBER || imageMagicNumber == INTEL_TIFF_MAGIC_NUMBER;
    }

    private static class RandomAccessReader {

        private final ByteBuffer data;

        public RandomAccessReader(byte[] data) {
            this.data = ByteBuffer.wrap(data);
            this.data.order(ByteOrder.BIG_ENDIAN);
        }

        public void order(ByteOrder byteOrder) {
            this.data.order(byteOrder);
        }

        public int length() {
            return data.array().length;
        }

        public int getInt32(int offset) {
            return data.getInt(offset);
        }

        public short getInt16(int offset) {
            return data.getShort(offset);
        }
    }

    private interface Reader {

        int getUInt16() throws IOException;

        short getUInt8() throws IOException;

        long skip(long total) throws IOException;

        int read(byte[] buffer) throws IOException;

        int getByte() throws IOException;
    }

    private static class ByteBufferReader implements Reader {

        private final ByteBuffer byteBuffer;

        public ByteBufferReader(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
            byteBuffer.order(ByteOrder.BIG_ENDIAN);
        }

        @Override
        public int getUInt16() throws IOException {
            return (getByte() << 8 & 0xFF00) | (getByte() & 0xFF);
        }

        @Override
        public short getUInt8() throws IOException {
            return (short) (getByte() & 0xFF);
        }

        @Override
        public long skip(long total) throws IOException {
            int toSkip = (int) Math.min(byteBuffer.remaining(), total);
            byteBuffer.position(byteBuffer.position() + toSkip);
            return toSkip;
        }

        @Override
        public int read(byte[] buffer) throws IOException {
            int toRead = Math.min(buffer.length, byteBuffer.remaining());
            byteBuffer.get(buffer);
            return toRead;
        }

        @Override
        public int getByte() throws IOException {
            if (byteBuffer.remaining() < 1) {
                return -1;
            }
            return byteBuffer.get();
        }
    }

    private static class StreamReader implements Reader {

        private final InputStream is;

        public StreamReader(InputStream is) {
            this.is = is;
        }

        @Override
        public int getUInt16() throws IOException {
            return (is.read() << 8 & 0xFF00) | (is.read() & 0xFF);
        }

        @Override
        public short getUInt8() throws IOException {
            return (short) (is.read() & 0xFF);
        }

        @Override
        public long skip(long total) throws IOException {
            if (total < 0) {
                return 0;
            }
            long toSkip = total;
            while (toSkip > 0) {
                long skipped = is.skip(toSkip);
                if (skipped > 0) {
                    toSkip -= skipped;
                } else {
                    int testEofByte = is.read();
                    if (testEofByte == -1) {
                        break;
                    } else {
                        toSkip--;
                    }
                }
            }
            return total - toSkip;
        }

        @Override
        public int read(byte[] buffer) throws IOException {
            int toRead = buffer.length;
            int read;
            while (toRead > 0 && ((read = is.read(buffer, buffer.length - toRead, toRead)) != -1)) {
                toRead -= read;
            }
            return buffer.length - toRead;
        }

        @Override
        public int getByte() throws IOException {
            return is.read();
        }
    }
}