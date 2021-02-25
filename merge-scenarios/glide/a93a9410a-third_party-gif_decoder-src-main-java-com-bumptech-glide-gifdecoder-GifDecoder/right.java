package com.bumptech.glide.gifdecoder;

import android.annotation.TargetApi;
import android.graphics.Bitmap;
import android.os.Build;
import android.util.Log;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class GifDecoder {

    private static final String TAG = GifDecoder.class.getSimpleName();

    public static final int STATUS_OK = 0;

    public static final int STATUS_FORMAT_ERROR = 1;

    public static final int STATUS_OPEN_ERROR = 2;

    public static final int STATUS_PARTIAL_DECODE = 3;

    private static final int MAX_STACK_SIZE = 4096;

    private static final int DISPOSAL_UNSPECIFIED = 0;

    private static final int DISPOSAL_NONE = 1;

    private static final int DISPOSAL_BACKGROUND = 2;

    private static final int DISPOSAL_PREVIOUS = 3;

    private static final int NULL_CODE = -1;

    private static final int INITIAL_FRAME_POINTER = -1;

    private static final Bitmap.Config BITMAP_CONFIG = Bitmap.Config.ARGB_8888;

    private int[] act;

    private ByteBuffer rawData;

    private final byte[] block = new byte[256];

    private GifHeaderParser parser;

    private short[] prefix;

    private byte[] suffix;

    private byte[] pixelStack;

    private byte[] mainPixels;

    private int[] mainScratch;

    private int framePointer;

    private GifHeader header;

    private BitmapProvider bitmapProvider;

    private Bitmap previousImage;

    private boolean savePrevious;

    private int status;

    public interface BitmapProvider {

        Bitmap obtain(int width, int height, Bitmap.Config config);

        void release(Bitmap bitmap);
    }

    public GifDecoder(BitmapProvider provider, GifHeader gifHeader, ByteBuffer rawData) {
        this(provider);
        setData(gifHeader, rawData);
    }

    public GifDecoder(BitmapProvider provider) {
        this.bitmapProvider = provider;
        header = new GifHeader();
    }

    public int getWidth() {
        return header.width;
    }

    public int getHeight() {
        return header.height;
    }

    public ByteBuffer getData() {
        return rawData;
    }

    public int getStatus() {
        return status;
    }

    public void advance() {
        framePointer = (framePointer + 1) % header.frameCount;
    }

    public int getDelay(int n) {
        int delay = -1;
        if ((n >= 0) && (n < header.frameCount)) {
            delay = header.frames.get(n).delay;
        }
        return delay;
    }

    public int getNextDelay() {
        if (header.frameCount <= 0 || framePointer < 0) {
            return -1;
        }
        return getDelay(framePointer);
    }

    public int getFrameCount() {
        return header.frameCount;
    }

    public int getCurrentFrameIndex() {
        return framePointer;
    }

    public void resetFrameIndex() {
        framePointer = -1;
    }

    public int getLoopCount() {
        return header.loopCount;
    }

    public synchronized Bitmap getNextFrame() {
        if (header.frameCount <= 0 || framePointer < 0) {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "unable to decode frame, frameCount=" + header.frameCount + " framePointer=" + framePointer);
            }
            status = STATUS_FORMAT_ERROR;
        }
        if (status == STATUS_FORMAT_ERROR || status == STATUS_OPEN_ERROR) {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Unable to decode frame, status=" + status);
            }
            return null;
        }
        status = STATUS_OK;
        GifFrame currentFrame = header.frames.get(framePointer);
        GifFrame previousFrame = null;
        int previousIndex = framePointer - 1;
        if (previousIndex >= 0) {
            previousFrame = header.frames.get(previousIndex);
        }
        if (currentFrame.lct == null) {
            act = header.gct;
        } else {
            act = currentFrame.lct;
            if (header.bgIndex == currentFrame.transIndex) {
                header.bgColor = 0;
            }
        }
        int save = 0;
        if (currentFrame.transparency) {
            save = act[currentFrame.transIndex];
            act[currentFrame.transIndex] = 0;
        }
        if (act == null) {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "No Valid Color Table");
            }
            status = STATUS_FORMAT_ERROR;
            return null;
        }
        Bitmap result = setPixels(currentFrame, previousFrame);
        if (currentFrame.transparency) {
            act[currentFrame.transIndex] = save;
        }
        return result;
    }

    public int read(InputStream is, int contentLength) {
        if (is != null) {
            try {
                int capacity = (contentLength > 0) ? (contentLength + 4096) : 16384;
                ByteArrayOutputStream buffer = new ByteArrayOutputStream(capacity);
                int nRead;
                byte[] data = new byte[16384];
                while ((nRead = is.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }
                buffer.flush();
                read(buffer.toByteArray());
            } catch (IOException e) {
                Log.w(TAG, "Error reading data from stream", e);
            }
        } else {
            status = STATUS_OPEN_ERROR;
        }
        try {
            if (is != null) {
                is.close();
            }
        } catch (IOException e) {
            Log.w(TAG, "Error closing stream", e);
        }
        return status;
    }

    public void clear() {
        header = null;
        mainPixels = null;
        mainScratch = null;
        if (previousImage != null) {
            bitmapProvider.release(previousImage);
        }
        previousImage = null;
        rawData = null;
    }

    public synchronized void setData(GifHeader header, byte[] data) {
        setData(header, ByteBuffer.wrap(data));
    }

    public synchronized void setData(GifHeader header, ByteBuffer buffer) {
        this.status = STATUS_OK;
        this.header = header;
        framePointer = INITIAL_FRAME_POINTER;
        rawData = buffer.asReadOnlyBuffer();
        rawData.rewind();
        rawData.order(ByteOrder.LITTLE_ENDIAN);
        savePrevious = false;
        for (GifFrame frame : header.frames) {
            if (frame.dispose == DISPOSAL_PREVIOUS) {
                savePrevious = true;
                break;
            }
        }
        mainPixels = new byte[header.width * header.height];
        mainScratch = new int[header.width * header.height];
    }

    private GifHeaderParser getHeaderParser() {
        if (parser == null) {
            parser = new GifHeaderParser();
        }
        return parser;
    }

    public synchronized int read(byte[] data) {
        this.header = getHeaderParser().setData(data).parseHeader();
        if (data != null) {
            rawData = ByteBuffer.wrap(data);
            rawData.rewind();
            rawData.order(ByteOrder.LITTLE_ENDIAN);
            mainPixels = new byte[header.width * header.height];
            mainScratch = new int[header.width * header.height];
            savePrevious = false;
            for (GifFrame frame : header.frames) {
                if (frame.dispose == DISPOSAL_PREVIOUS) {
                    savePrevious = true;
                    break;
                }
            }
        }
        return status;
    }

    private Bitmap setPixels(GifFrame currentFrame, GifFrame previousFrame) {
        int width = header.width;
        int height = header.height;
        final int[] dest = mainScratch;
        if (previousFrame != null && previousFrame.dispose > DISPOSAL_UNSPECIFIED) {
            if (previousFrame.dispose == DISPOSAL_BACKGROUND) {
                int c = 0;
                if (!currentFrame.transparency) {
                    c = header.bgColor;
                }
                Arrays.fill(dest, c);
            } else if (previousFrame.dispose == DISPOSAL_PREVIOUS && previousImage != null) {
                previousImage.getPixels(dest, 0, width, 0, 0, width, height);
            }
        }
        decodeBitmapData(currentFrame);
        int pass = 1;
        int inc = 8;
        int iline = 0;
        for (int i = 0; i < currentFrame.ih; i++) {
            int line = i;
            if (currentFrame.interlace) {
                if (iline >= currentFrame.ih) {
                    pass++;
                    switch(pass) {
                        case 2:
                            iline = 4;
                            break;
                        case 3:
                            iline = 2;
                            inc = 4;
                            break;
                        case 4:
                            iline = 1;
                            inc = 2;
                            break;
                        default:
                            break;
                    }
                }
                line = iline;
                iline += inc;
            }
            line += currentFrame.iy;
            if (line < header.height) {
                int k = line * header.width;
                int dx = k + currentFrame.ix;
                int dlim = dx + currentFrame.iw;
                if ((k + header.width) < dlim) {
                    dlim = k + header.width;
                }
                int sx = i * currentFrame.iw;
                while (dx < dlim) {
                    int index = ((int) mainPixels[sx++]) & 0xff;
                    int c = act[index];
                    if (c != 0) {
                        dest[dx] = c;
                    }
                    dx++;
                }
            }
        }
        if (savePrevious && (currentFrame.dispose == DISPOSAL_UNSPECIFIED || currentFrame.dispose == DISPOSAL_NONE)) {
            if (previousImage == null) {
                previousImage = getNextBitmap();
            }
            previousImage.setPixels(dest, 0, width, 0, 0, width, height);
        }
        Bitmap result = getNextBitmap();
        result.setPixels(dest, 0, width, 0, 0, width, height);
        return result;
    }

    private void decodeBitmapData(GifFrame frame) {
        if (frame != null) {
            rawData.position(frame.bufferFrameStart);
        }
        int npix = (frame == null) ? header.width * header.height : frame.iw * frame.ih;
        int available, clear, codeMask, codeSize, endOfInformation, inCode, oldCode, bits, code, count, i, datum, dataSize, first, top, bi, pi;
        if (mainPixels == null || mainPixels.length < npix) {
            mainPixels = new byte[npix];
        }
        if (prefix == null) {
            prefix = new short[MAX_STACK_SIZE];
        }
        if (suffix == null) {
            suffix = new byte[MAX_STACK_SIZE];
        }
        if (pixelStack == null) {
            pixelStack = new byte[MAX_STACK_SIZE + 1];
        }
        dataSize = read();
        clear = 1 << dataSize;
        endOfInformation = clear + 1;
        available = clear + 2;
        oldCode = NULL_CODE;
        codeSize = dataSize + 1;
        codeMask = (1 << codeSize) - 1;
        for (code = 0; code < clear; code++) {
            prefix[code] = 0;
            suffix[code] = (byte) code;
        }
        datum = bits = count = first = top = pi = bi = 0;
        for (i = 0; i < npix; ) {
            if (count == 0) {
                count = readBlock();
                if (count <= 0) {
                    status = STATUS_PARTIAL_DECODE;
                    break;
                }
                bi = 0;
            }
            datum += (((int) block[bi]) & 0xff) << bits;
            bits += 8;
            bi++;
            count--;
            while (bits >= codeSize) {
                code = datum & codeMask;
                datum >>= codeSize;
                bits -= codeSize;
                if (code == clear) {
                    codeSize = dataSize + 1;
                    codeMask = (1 << codeSize) - 1;
                    available = clear + 2;
                    oldCode = NULL_CODE;
                    continue;
                }
                if (code > available) {
                    status = STATUS_PARTIAL_DECODE;
                    break;
                }
                if (code == endOfInformation) {
                    break;
                }
                if (oldCode == NULL_CODE) {
                    pixelStack[top++] = suffix[code];
                    oldCode = code;
                    first = code;
                    continue;
                }
                inCode = code;
                if (code >= available) {
                    pixelStack[top++] = (byte) first;
                    code = oldCode;
                }
                while (code >= clear) {
                    pixelStack[top++] = suffix[code];
                    code = prefix[code];
                }
                first = ((int) suffix[code]) & 0xff;
                pixelStack[top++] = (byte) first;
                if (available < MAX_STACK_SIZE) {
                    prefix[available] = (short) oldCode;
                    suffix[available] = (byte) first;
                    available++;
                    if (((available & codeMask) == 0) && (available < MAX_STACK_SIZE)) {
                        codeSize++;
                        codeMask += available;
                    }
                }
                oldCode = inCode;
                while (top > 0) {
                    top--;
                    mainPixels[pi++] = pixelStack[top];
                    i++;
                }
            }
        }
        for (i = pi; i < npix; i++) {
            mainPixels[i] = 0;
        }
    }

    private int read() {
        int curByte = 0;
        try {
            curByte = rawData.get() & 0xFF;
        } catch (Exception e) {
            status = STATUS_FORMAT_ERROR;
        }
        return curByte;
    }

    private int readBlock() {
        int blockSize = read();
        int n = 0;
        if (blockSize > 0) {
            try {
                int count;
                while (n < blockSize) {
                    count = blockSize - n;
                    rawData.get(block, n, count);
                    n += count;
                }
            } catch (Exception e) {
                Log.w(TAG, "Error Reading Block", e);
                status = STATUS_FORMAT_ERROR;
            }
        }
        return n;
    }

    private Bitmap getNextBitmap() {
        Bitmap result = bitmapProvider.obtain(header.width, header.height, BITMAP_CONFIG);
        if (result == null) {
            result = Bitmap.createBitmap(header.width, header.height, BITMAP_CONFIG);
        }
        setAlpha(result);
        return result;
    }

    @TargetApi(12)
    private static void setAlpha(Bitmap bitmap) {
        if (Build.VERSION.SDK_INT >= 12) {
            bitmap.setHasAlpha(true);
        }
    }
}