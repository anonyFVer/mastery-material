package io.netty.handler.codec.spdy;

import static io.netty.handler.codec.spdy.SpdyCodecUtil.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.Channels;
import io.netty.handler.codec.frame.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.oneone.OneToOneDecoder;

public class SpdyHttpDecoder extends OneToOneDecoder {

    private final int spdyVersion;

    private final int maxContentLength;

    private final Map<Integer, HttpMessage> messageMap = new HashMap<Integer, HttpMessage>();

    public SpdyHttpDecoder(int version, int maxContentLength) {
        super();
        if (version < SPDY_MIN_VERSION || version > SPDY_MAX_VERSION) {
            throw new IllegalArgumentException("unsupported version: " + version);
        }
        if (maxContentLength <= 0) {
            throw new IllegalArgumentException("maxContentLength must be a positive integer: " + maxContentLength);
        }
        spdyVersion = version;
        this.maxContentLength = maxContentLength;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            int streamID = spdySynStreamFrame.getStreamID();
            if (SpdyCodecUtil.isServerID(streamID)) {
                int associatedToStreamID = spdySynStreamFrame.getAssociatedToStreamID();
                if (associatedToStreamID == 0) {
                    SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, SpdyStreamStatus.INVALID_STREAM);
                    Channels.write(ctx, Channels.future(channel), spdyRstStreamFrame);
                }
                String URL = SpdyHeaders.getUrl(spdyVersion, spdySynStreamFrame);
                if (URL == null) {
                    SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                    Channels.write(ctx, Channels.future(channel), spdyRstStreamFrame);
                }
                try {
                    HttpResponse httpResponse = createHttpResponse(spdyVersion, spdySynStreamFrame);
                    SpdyHttpHeaders.setStreamID(httpResponse, streamID);
                    SpdyHttpHeaders.setAssociatedToStreamID(httpResponse, associatedToStreamID);
                    SpdyHttpHeaders.setPriority(httpResponse, spdySynStreamFrame.getPriority());
                    SpdyHttpHeaders.setUrl(httpResponse, URL);
                    if (spdySynStreamFrame.isLast()) {
                        HttpHeaders.setContentLength(httpResponse, 0);
                        return httpResponse;
                    } else {
                        messageMap.put(new Integer(streamID), httpResponse);
                    }
                } catch (Exception e) {
                    SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                    Channels.write(ctx, Channels.future(channel), spdyRstStreamFrame);
                }
            } else {
                try {
                    HttpRequest httpRequest = createHttpRequest(spdyVersion, spdySynStreamFrame);
                    SpdyHttpHeaders.setStreamID(httpRequest, streamID);
                    if (spdySynStreamFrame.isLast()) {
                        return httpRequest;
                    } else {
                        messageMap.put(new Integer(streamID), httpRequest);
                    }
                } catch (Exception e) {
                    SpdySynReplyFrame spdySynReplyFrame = new DefaultSpdySynReplyFrame(streamID);
                    spdySynReplyFrame.setLast(true);
                    SpdyHeaders.setStatus(spdyVersion, spdySynReplyFrame, HttpResponseStatus.BAD_REQUEST);
                    SpdyHeaders.setVersion(spdyVersion, spdySynReplyFrame, HttpVersion.HTTP_1_0);
                    Channels.write(ctx, Channels.future(channel), spdySynReplyFrame);
                }
            }
        } else if (msg instanceof SpdySynReplyFrame) {
            SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
            int streamID = spdySynReplyFrame.getStreamID();
            try {
                HttpResponse httpResponse = createHttpResponse(spdyVersion, spdySynReplyFrame);
                SpdyHttpHeaders.setStreamID(httpResponse, streamID);
                if (spdySynReplyFrame.isLast()) {
                    HttpHeaders.setContentLength(httpResponse, 0);
                    return httpResponse;
                } else {
                    messageMap.put(new Integer(streamID), httpResponse);
                }
            } catch (Exception e) {
                SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                Channels.write(ctx, Channels.future(channel), spdyRstStreamFrame);
            }
        } else if (msg instanceof SpdyHeadersFrame) {
            SpdyHeadersFrame spdyHeadersFrame = (SpdyHeadersFrame) msg;
            Integer streamID = new Integer(spdyHeadersFrame.getStreamID());
            HttpMessage httpMessage = messageMap.get(streamID);
            if (httpMessage == null) {
                return null;
            }
            for (Map.Entry<String, String> e : spdyHeadersFrame.getHeaders()) {
                httpMessage.addHeader(e.getKey(), e.getValue());
            }
        } else if (msg instanceof SpdyDataFrame) {
            SpdyDataFrame spdyDataFrame = (SpdyDataFrame) msg;
            Integer streamID = new Integer(spdyDataFrame.getStreamID());
            HttpMessage httpMessage = messageMap.get(streamID);
            if (httpMessage == null) {
                return null;
            }
            ChannelBuffer content = httpMessage.getContent();
            if (content.readableBytes() > maxContentLength - spdyDataFrame.getData().readableBytes()) {
                messageMap.remove(streamID);
                throw new TooLongFrameException("HTTP content length exceeded " + maxContentLength + " bytes.");
            }
            if (content == ChannelBuffers.EMPTY_BUFFER) {
                content = ChannelBuffers.dynamicBuffer(channel.getConfig().getBufferFactory());
                content.writeBytes(spdyDataFrame.getData());
                httpMessage.setContent(content);
            } else {
                content.writeBytes(spdyDataFrame.getData());
            }
            if (spdyDataFrame.isLast()) {
                HttpHeaders.setContentLength(httpMessage, content.readableBytes());
                messageMap.remove(streamID);
                return httpMessage;
            }
        } else if (msg instanceof SpdyRstStreamFrame) {
            SpdyRstStreamFrame spdyRstStreamFrame = (SpdyRstStreamFrame) msg;
            Integer streamID = new Integer(spdyRstStreamFrame.getStreamID());
            messageMap.remove(streamID);
        }
        return null;
    }

    private static HttpRequest createHttpRequest(int spdyVersion, SpdyHeaderBlock requestFrame) throws Exception {
        HttpMethod method = SpdyHeaders.getMethod(spdyVersion, requestFrame);
        String url = SpdyHeaders.getUrl(spdyVersion, requestFrame);
        HttpVersion httpVersion = SpdyHeaders.getVersion(spdyVersion, requestFrame);
        SpdyHeaders.removeMethod(spdyVersion, requestFrame);
        SpdyHeaders.removeUrl(spdyVersion, requestFrame);
        SpdyHeaders.removeVersion(spdyVersion, requestFrame);
        HttpRequest httpRequest = new DefaultHttpRequest(httpVersion, method, url);
        SpdyHeaders.removeScheme(spdyVersion, requestFrame);
        if (spdyVersion >= 3) {
            String host = SpdyHeaders.getHost(requestFrame);
            SpdyHeaders.removeHost(requestFrame);
            HttpHeaders.setHost(httpRequest, host);
        }
        for (Map.Entry<String, String> e : requestFrame.getHeaders()) {
            httpRequest.addHeader(e.getKey(), e.getValue());
        }
        HttpHeaders.setKeepAlive(httpRequest, true);
        httpRequest.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        return httpRequest;
    }

    private static HttpResponse createHttpResponse(int spdyVersion, SpdyHeaderBlock responseFrame) throws Exception {
        HttpResponseStatus status = SpdyHeaders.getStatus(spdyVersion, responseFrame);
        HttpVersion version = SpdyHeaders.getVersion(spdyVersion, responseFrame);
        SpdyHeaders.removeStatus(spdyVersion, responseFrame);
        SpdyHeaders.removeVersion(spdyVersion, responseFrame);
        HttpResponse httpResponse = new DefaultHttpResponse(version, status);
        for (Map.Entry<String, String> e : responseFrame.getHeaders()) {
            httpResponse.addHeader(e.getKey(), e.getValue());
        }
        HttpHeaders.setKeepAlive(httpResponse, true);
        httpResponse.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        httpResponse.removeHeader(HttpHeaders.Names.TRAILER);
        return httpResponse;
    }
}