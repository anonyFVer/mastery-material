package io.netty.handler.codec.spdy;

import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.ChannelInboundHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpdyHttpDecoder extends MessageToMessageDecoder<Object, Object> {

    private final int maxContentLength;

    private final Map<Integer, HttpMessage> messageMap = new HashMap<Integer, HttpMessage>();

    public SpdyHttpDecoder(int maxContentLength) {
        super();
        if (maxContentLength <= 0) {
            throw new IllegalArgumentException("maxContentLength must be a positive integer: " + maxContentLength);
        }
        this.maxContentLength = maxContentLength;
    }

    @Override
    public Object decode(ChannelInboundHandlerContext<Object> ctx, Object msg) throws Exception {
        if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            int streamID = spdySynStreamFrame.getStreamID();
            if (SpdyCodecUtil.isServerID(streamID)) {
                int associatedToStreamID = spdySynStreamFrame.getAssociatedToStreamID();
                if (associatedToStreamID == 0) {
                    SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, SpdyStreamStatus.INVALID_STREAM);
                    ctx.write(spdyRstStreamFrame);
                }
                String URL = SpdyHeaders.getUrl(spdySynStreamFrame);
                if (URL == null) {
                    SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                    ctx.write(spdyRstStreamFrame);
                }
                try {
                    HttpResponse httpResponse = createHttpResponse(spdySynStreamFrame);
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
                    ctx.write(spdyRstStreamFrame);
                }
            } else {
                try {
                    HttpRequest httpRequest = createHttpRequest(spdySynStreamFrame);
                    SpdyHttpHeaders.setStreamID(httpRequest, streamID);
                    if (spdySynStreamFrame.isLast()) {
                        return httpRequest;
                    } else {
                        messageMap.put(new Integer(streamID), httpRequest);
                    }
                } catch (Exception e) {
                    SpdySynReplyFrame spdySynReplyFrame = new DefaultSpdySynReplyFrame(streamID);
                    spdySynReplyFrame.setLast(true);
                    SpdyHeaders.setStatus(spdySynReplyFrame, HttpResponseStatus.BAD_REQUEST);
                    SpdyHeaders.setVersion(spdySynReplyFrame, HttpVersion.HTTP_1_0);
                    ctx.write(spdySynReplyFrame);
                }
            }
        } else if (msg instanceof SpdySynReplyFrame) {
            SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
            int streamID = spdySynReplyFrame.getStreamID();
            try {
                HttpResponse httpResponse = createHttpResponse(spdySynReplyFrame);
                SpdyHttpHeaders.setStreamID(httpResponse, streamID);
                if (spdySynReplyFrame.isLast()) {
                    HttpHeaders.setContentLength(httpResponse, 0);
                    return httpResponse;
                } else {
                    messageMap.put(new Integer(streamID), httpResponse);
                }
            } catch (Exception e) {
                SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                ctx.write(spdyRstStreamFrame);
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
                ChannelBuffer data = spdyDataFrame.getData();
                content = ChannelBuffers.dynamicBuffer(data.readableBytes());
                content.writeBytes(data, data.readerIndex(), data.readableBytes());
                httpMessage.setContent(content);
            } else {
                content.writeBytes(spdyDataFrame.getData());
            }
            if (spdyDataFrame.isLast()) {
                HttpHeaders.setContentLength(httpMessage, content.readableBytes());
                messageMap.remove(streamID);
                return httpMessage;
            }
        }
        return null;
    }

    private static HttpRequest createHttpRequest(SpdyHeaderBlock requestFrame) throws Exception {
        HttpMethod method = SpdyHeaders.getMethod(requestFrame);
        String url = SpdyHeaders.getUrl(requestFrame);
        HttpVersion version = SpdyHeaders.getVersion(requestFrame);
        SpdyHeaders.removeMethod(requestFrame);
        SpdyHeaders.removeUrl(requestFrame);
        SpdyHeaders.removeVersion(requestFrame);
        HttpRequest httpRequest = new DefaultHttpRequest(version, method, url);
        for (Map.Entry<String, String> e : requestFrame.getHeaders()) {
            httpRequest.addHeader(e.getKey(), e.getValue());
        }
        List<String> encodings = httpRequest.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING);
        encodings.remove(HttpHeaders.Values.CHUNKED);
        if (encodings.isEmpty()) {
            httpRequest.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        } else {
            httpRequest.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, encodings);
        }
        HttpHeaders.setKeepAlive(httpRequest, true);
        return httpRequest;
    }

    private static HttpResponse createHttpResponse(SpdyHeaderBlock responseFrame) throws Exception {
        HttpResponseStatus status = SpdyHeaders.getStatus(responseFrame);
        HttpVersion version = SpdyHeaders.getVersion(responseFrame);
        SpdyHeaders.removeStatus(responseFrame);
        SpdyHeaders.removeVersion(responseFrame);
        HttpResponse httpResponse = new DefaultHttpResponse(version, status);
        for (Map.Entry<String, String> e : responseFrame.getHeaders()) {
            httpResponse.addHeader(e.getKey(), e.getValue());
        }
        List<String> encodings = httpResponse.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING);
        encodings.remove(HttpHeaders.Values.CHUNKED);
        if (encodings.isEmpty()) {
            httpResponse.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        } else {
            httpResponse.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, encodings);
        }
        httpResponse.removeHeader(HttpHeaders.Names.TRAILER);
        HttpHeaders.setKeepAlive(httpResponse, true);
        return httpResponse;
    }
}