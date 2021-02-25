package io.netty.handler.codec.http;

import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.ChannelInboundHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.handler.codec.TooLongFrameException;
import java.util.List;

public abstract class HttpMessageDecoder extends ReplayingDecoder<Object, HttpMessageDecoder.State> {

    private final int maxInitialLineLength;

    private final int maxHeaderSize;

    private final int maxChunkSize;

    private HttpMessage message;

    private ChannelBuffer content;

    private long chunkSize;

    private int headerSize;

    private int contentRead;

    protected enum State {

        SKIP_CONTROL_CHARS,
        READ_INITIAL,
        READ_HEADER,
        READ_VARIABLE_LENGTH_CONTENT,
        READ_VARIABLE_LENGTH_CONTENT_AS_CHUNKS,
        READ_FIXED_LENGTH_CONTENT,
        READ_FIXED_LENGTH_CONTENT_AS_CHUNKS,
        READ_CHUNK_SIZE,
        READ_CHUNKED_CONTENT,
        READ_CHUNKED_CONTENT_AS_CHUNKS,
        READ_CHUNK_DELIMITER,
        READ_CHUNK_FOOTER
    }

    protected HttpMessageDecoder() {
        this(4096, 8192, 8192);
    }

    protected HttpMessageDecoder(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
        super(State.SKIP_CONTROL_CHARS);
        if (maxInitialLineLength <= 0) {
            throw new IllegalArgumentException("maxInitialLineLength must be a positive integer: " + maxInitialLineLength);
        }
        if (maxHeaderSize <= 0) {
            throw new IllegalArgumentException("maxHeaderSize must be a positive integer: " + maxHeaderSize);
        }
        if (maxChunkSize < 0) {
            throw new IllegalArgumentException("maxChunkSize must be a positive integer: " + maxChunkSize);
        }
        this.maxInitialLineLength = maxInitialLineLength;
        this.maxHeaderSize = maxHeaderSize;
        this.maxChunkSize = maxChunkSize;
    }

    @Override
    public Object decode(ChannelInboundHandlerContext<Byte> ctx, ChannelBuffer buffer) throws Exception {
        switch(state()) {
            case SKIP_CONTROL_CHARS:
                {
                    try {
                        skipControlCharacters(buffer);
                        checkpoint(State.READ_INITIAL);
                    } finally {
                        checkpoint();
                    }
                }
            case READ_INITIAL:
                {
                    String[] initialLine = splitInitialLine(readLine(buffer, maxInitialLineLength));
                    if (initialLine.length < 3) {
                        checkpoint(State.SKIP_CONTROL_CHARS);
                        return null;
                    }
                    message = createMessage(initialLine);
                    checkpoint(State.READ_HEADER);
                }
            case READ_HEADER:
                {
                    State nextState = readHeaders(buffer);
                    checkpoint(nextState);
                    if (nextState == State.READ_CHUNK_SIZE) {
                        message.setChunked(true);
                        return message;
                    } else if (nextState == State.SKIP_CONTROL_CHARS) {
                        message.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
                        return message;
                    } else {
                        long contentLength = HttpHeaders.getContentLength(message, -1);
                        if (contentLength == 0 || contentLength == -1 && isDecodingRequest()) {
                            content = ChannelBuffers.EMPTY_BUFFER;
                            return reset();
                        }
                        switch(nextState) {
                            case READ_FIXED_LENGTH_CONTENT:
                                if (contentLength > maxChunkSize || HttpHeaders.is100ContinueExpected(message)) {
                                    checkpoint(State.READ_FIXED_LENGTH_CONTENT_AS_CHUNKS);
                                    message.setChunked(true);
                                    chunkSize = HttpHeaders.getContentLength(message, -1);
                                    return message;
                                }
                                break;
                            case READ_VARIABLE_LENGTH_CONTENT:
                                if (buffer.readableBytes() > maxChunkSize || HttpHeaders.is100ContinueExpected(message)) {
                                    checkpoint(State.READ_VARIABLE_LENGTH_CONTENT_AS_CHUNKS);
                                    message.setChunked(true);
                                    return message;
                                }
                                break;
                            default:
                                throw new IllegalStateException("Unexpected state: " + nextState);
                        }
                    }
                    return null;
                }
            case READ_VARIABLE_LENGTH_CONTENT:
                {
                    int toRead = actualReadableBytes();
                    if (toRead > maxChunkSize) {
                        toRead = maxChunkSize;
                    }
                    if (!message.isChunked()) {
                        message.setChunked(true);
                        return new Object[] { message, new DefaultHttpChunk(buffer.readBytes(toRead)) };
                    } else {
                        return new DefaultHttpChunk(buffer.readBytes(toRead));
                    }
                }
            case READ_VARIABLE_LENGTH_CONTENT_AS_CHUNKS:
                {
                    int toRead = actualReadableBytes();
                    if (toRead > maxChunkSize) {
                        toRead = maxChunkSize;
                    }
                    HttpChunk chunk = new DefaultHttpChunk(buffer.readBytes(toRead));
                    if (!buffer.readable()) {
                        reset();
                        if (!chunk.isLast()) {
                            return new Object[] { chunk, HttpChunk.LAST_CHUNK };
                        }
                    }
                    return chunk;
                }
            case READ_FIXED_LENGTH_CONTENT:
                {
                    return readFixedLengthContent(buffer);
                }
            case READ_FIXED_LENGTH_CONTENT_AS_CHUNKS:
                {
                    assert this.chunkSize <= Integer.MAX_VALUE;
                    int chunkSize = (int) this.chunkSize;
                    int readLimit = actualReadableBytes();
                    int toRead = chunkSize;
                    if (toRead > maxChunkSize) {
                        toRead = maxChunkSize;
                    }
                    if (toRead > readLimit) {
                        toRead = readLimit;
                    }
                    HttpChunk chunk = new DefaultHttpChunk(buffer.readBytes(toRead));
                    if (chunkSize > toRead) {
                        chunkSize -= toRead;
                    } else {
                        chunkSize = 0;
                    }
                    this.chunkSize = chunkSize;
                    if (chunkSize == 0) {
                        reset();
                        if (!chunk.isLast()) {
                            return new Object[] { chunk, HttpChunk.LAST_CHUNK };
                        }
                    }
                    return chunk;
                }
            case READ_CHUNK_SIZE:
                {
                    String line = readLine(buffer, maxInitialLineLength);
                    int chunkSize = getChunkSize(line);
                    this.chunkSize = chunkSize;
                    if (chunkSize == 0) {
                        checkpoint(State.READ_CHUNK_FOOTER);
                        return null;
                    } else if (chunkSize > maxChunkSize) {
                        checkpoint(State.READ_CHUNKED_CONTENT_AS_CHUNKS);
                    } else {
                        checkpoint(State.READ_CHUNKED_CONTENT);
                    }
                }
            case READ_CHUNKED_CONTENT:
                {
                    assert chunkSize <= Integer.MAX_VALUE;
                    HttpChunk chunk = new DefaultHttpChunk(buffer.readBytes((int) chunkSize));
                    checkpoint(State.READ_CHUNK_DELIMITER);
                    return chunk;
                }
            case READ_CHUNKED_CONTENT_AS_CHUNKS:
                {
                    assert this.chunkSize <= Integer.MAX_VALUE;
                    int chunkSize = (int) this.chunkSize;
                    int readLimit = actualReadableBytes();
                    int toRead = chunkSize;
                    if (toRead > maxChunkSize) {
                        toRead = maxChunkSize;
                    }
                    if (toRead > readLimit) {
                        toRead = readLimit;
                    }
                    HttpChunk chunk = new DefaultHttpChunk(buffer.readBytes(toRead));
                    if (chunkSize > toRead) {
                        chunkSize -= toRead;
                    } else {
                        chunkSize = 0;
                    }
                    this.chunkSize = chunkSize;
                    if (chunkSize == 0) {
                        checkpoint(State.READ_CHUNK_DELIMITER);
                    }
                    if (!chunk.isLast()) {
                        return chunk;
                    }
                }
            case READ_CHUNK_DELIMITER:
                {
                    for (; ; ) {
                        byte next = buffer.readByte();
                        if (next == HttpConstants.CR) {
                            if (buffer.readByte() == HttpConstants.LF) {
                                checkpoint(State.READ_CHUNK_SIZE);
                                return null;
                            }
                        } else if (next == HttpConstants.LF) {
                            checkpoint(State.READ_CHUNK_SIZE);
                            return null;
                        }
                    }
                }
            case READ_CHUNK_FOOTER:
                {
                    HttpChunkTrailer trailer = readTrailingHeaders(buffer);
                    if (maxChunkSize == 0) {
                        return reset();
                    } else {
                        reset();
                        return trailer;
                    }
                }
            default:
                {
                    throw new Error("Shouldn't reach here.");
                }
        }
    }

    protected boolean isContentAlwaysEmpty(HttpMessage msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse res = (HttpResponse) msg;
            int code = res.getStatus().getCode();
            if (code >= 100 && code < 200) {
                if (code == 101 && !res.containsHeader(HttpHeaders.Names.SEC_WEBSOCKET_ACCEPT)) {
                    return false;
                }
                return true;
            }
            switch(code) {
                case 204:
                case 205:
                case 304:
                    return true;
            }
        }
        return false;
    }

    private Object reset() {
        HttpMessage message = this.message;
        ChannelBuffer content = this.content;
        if (content != null) {
            message.setContent(content);
            this.content = null;
        }
        this.message = null;
        checkpoint(State.SKIP_CONTROL_CHARS);
        return message;
    }

    private static void skipControlCharacters(ChannelBuffer buffer) {
        for (; ; ) {
            char c = (char) buffer.readUnsignedByte();
            if (!Character.isISOControl(c) && !Character.isWhitespace(c)) {
                buffer.readerIndex(buffer.readerIndex() - 1);
                break;
            }
        }
    }

    private Object readFixedLengthContent(ChannelBuffer buffer) {
        long length = HttpHeaders.getContentLength(message, -1);
        assert length <= Integer.MAX_VALUE;
        int toRead = (int) length - contentRead;
        if (toRead > actualReadableBytes()) {
            toRead = actualReadableBytes();
        }
        contentRead = contentRead + toRead;
        if (length < contentRead) {
            if (!message.isChunked()) {
                message.setChunked(true);
                return new Object[] { message, new DefaultHttpChunk(buffer.readBytes(toRead)) };
            } else {
                return new DefaultHttpChunk(buffer.readBytes(toRead));
            }
        }
        if (content == null) {
            content = buffer.readBytes((int) length);
        } else {
            content.writeBytes(buffer.readBytes((int) length));
        }
        return reset();
    }

    private State readHeaders(ChannelBuffer buffer) throws TooLongFrameException {
        headerSize = 0;
        final HttpMessage message = this.message;
        String line = readHeader(buffer);
        String name = null;
        String value = null;
        if (line.length() != 0) {
            message.clearHeaders();
            do {
                char firstChar = line.charAt(0);
                if (name != null && (firstChar == ' ' || firstChar == '\t')) {
                    value = value + ' ' + line.trim();
                } else {
                    if (name != null) {
                        message.addHeader(name, value);
                    }
                    String[] header = splitHeader(line);
                    name = header[0];
                    value = header[1];
                }
                line = readHeader(buffer);
            } while (line.length() != 0);
            if (name != null) {
                message.addHeader(name, value);
            }
        }
        State nextState;
        if (isContentAlwaysEmpty(message)) {
            nextState = State.SKIP_CONTROL_CHARS;
        } else if (message.isChunked()) {
            nextState = State.READ_CHUNK_SIZE;
        } else if (HttpHeaders.getContentLength(message, -1) >= 0) {
            nextState = State.READ_FIXED_LENGTH_CONTENT;
        } else {
            nextState = State.READ_VARIABLE_LENGTH_CONTENT;
        }
        return nextState;
    }

    private HttpChunkTrailer readTrailingHeaders(ChannelBuffer buffer) throws TooLongFrameException {
        headerSize = 0;
        String line = readHeader(buffer);
        String lastHeader = null;
        if (line.length() != 0) {
            HttpChunkTrailer trailer = new DefaultHttpChunkTrailer();
            do {
                char firstChar = line.charAt(0);
                if (lastHeader != null && (firstChar == ' ' || firstChar == '\t')) {
                    List<String> current = trailer.getHeaders(lastHeader);
                    if (current.size() != 0) {
                        int lastPos = current.size() - 1;
                        String newString = current.get(lastPos) + line.trim();
                        current.set(lastPos, newString);
                    } else {
                    }
                } else {
                    String[] header = splitHeader(line);
                    String name = header[0];
                    if (!name.equalsIgnoreCase(HttpHeaders.Names.CONTENT_LENGTH) && !name.equalsIgnoreCase(HttpHeaders.Names.TRANSFER_ENCODING) && !name.equalsIgnoreCase(HttpHeaders.Names.TRAILER)) {
                        trailer.addHeader(name, header[1]);
                    }
                    lastHeader = name;
                }
                line = readHeader(buffer);
            } while (line.length() != 0);
            return trailer;
        }
        return HttpChunk.LAST_CHUNK;
    }

    private String readHeader(ChannelBuffer buffer) throws TooLongFrameException {
        StringBuilder sb = new StringBuilder(64);
        int headerSize = this.headerSize;
        loop: for (; ; ) {
            char nextByte = (char) buffer.readByte();
            headerSize++;
            switch(nextByte) {
                case HttpConstants.CR:
                    nextByte = (char) buffer.readByte();
                    headerSize++;
                    if (nextByte == HttpConstants.LF) {
                        break loop;
                    }
                    break;
                case HttpConstants.LF:
                    break loop;
            }
            if (headerSize >= maxHeaderSize) {
                throw new TooLongFrameException("HTTP header is larger than " + maxHeaderSize + " bytes.");
            }
            sb.append(nextByte);
        }
        this.headerSize = headerSize;
        return sb.toString();
    }

    protected abstract boolean isDecodingRequest();

    protected abstract HttpMessage createMessage(String[] initialLine) throws Exception;

    private static int getChunkSize(String hex) {
        hex = hex.trim();
        for (int i = 0; i < hex.length(); i++) {
            char c = hex.charAt(i);
            if (c == ';' || Character.isWhitespace(c) || Character.isISOControl(c)) {
                hex = hex.substring(0, i);
                break;
            }
        }
        return Integer.parseInt(hex, 16);
    }

    private static String readLine(ChannelBuffer buffer, int maxLineLength) throws TooLongFrameException {
        StringBuilder sb = new StringBuilder(64);
        int lineLength = 0;
        while (true) {
            byte nextByte = buffer.readByte();
            if (nextByte == HttpConstants.CR) {
                nextByte = buffer.readByte();
                if (nextByte == HttpConstants.LF) {
                    return sb.toString();
                }
            } else if (nextByte == HttpConstants.LF) {
                return sb.toString();
            } else {
                if (lineLength >= maxLineLength) {
                    throw new TooLongFrameException("An HTTP line is larger than " + maxLineLength + " bytes.");
                }
                lineLength++;
                sb.append((char) nextByte);
            }
        }
    }

    private static String[] splitInitialLine(String sb) {
        int aStart;
        int aEnd;
        int bStart;
        int bEnd;
        int cStart;
        int cEnd;
        aStart = findNonWhitespace(sb, 0);
        aEnd = findWhitespace(sb, aStart);
        bStart = findNonWhitespace(sb, aEnd);
        bEnd = findWhitespace(sb, bStart);
        cStart = findNonWhitespace(sb, bEnd);
        cEnd = findEndOfString(sb);
        return new String[] { sb.substring(aStart, aEnd), sb.substring(bStart, bEnd), cStart < cEnd ? sb.substring(cStart, cEnd) : "" };
    }

    private static String[] splitHeader(String sb) {
        final int length = sb.length();
        int nameStart;
        int nameEnd;
        int colonEnd;
        int valueStart;
        int valueEnd;
        nameStart = findNonWhitespace(sb, 0);
        for (nameEnd = nameStart; nameEnd < length; nameEnd++) {
            char ch = sb.charAt(nameEnd);
            if (ch == ':' || Character.isWhitespace(ch)) {
                break;
            }
        }
        for (colonEnd = nameEnd; colonEnd < length; colonEnd++) {
            if (sb.charAt(colonEnd) == ':') {
                colonEnd++;
                break;
            }
        }
        valueStart = findNonWhitespace(sb, colonEnd);
        if (valueStart == length) {
            return new String[] { sb.substring(nameStart, nameEnd), "" };
        }
        valueEnd = findEndOfString(sb);
        return new String[] { sb.substring(nameStart, nameEnd), sb.substring(valueStart, valueEnd) };
    }

    private static int findNonWhitespace(String sb, int offset) {
        int result;
        for (result = offset; result < sb.length(); result++) {
            if (!Character.isWhitespace(sb.charAt(result))) {
                break;
            }
        }
        return result;
    }

    private static int findWhitespace(String sb, int offset) {
        int result;
        for (result = offset; result < sb.length(); result++) {
            if (Character.isWhitespace(sb.charAt(result))) {
                break;
            }
        }
        return result;
    }

    private static int findEndOfString(String sb) {
        int result;
        for (result = sb.length(); result > 0; result--) {
            if (!Character.isWhitespace(sb.charAt(result - 1))) {
                break;
            }
        }
        return result;
    }
}