package io.netty.handler.codec.spdy;

import io.netty.channel.ChannelOutboundHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.http.HttpChunk;
import io.netty.handler.codec.http.HttpChunkTrailer;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.util.List;
import java.util.Map;

public class SpdyHttpEncoder extends MessageToMessageEncoder<Object, Object> {

    private volatile int currentStreamID;

    @Override
    public Object encode(ChannelOutboundHandlerContext<Object> ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest httpRequest = (HttpRequest) msg;
            SpdySynStreamFrame spdySynStreamFrame = createSynStreamFrame(httpRequest);
            int streamID = spdySynStreamFrame.getStreamID();
            return new Object[] { spdySynStreamFrame, dataFrame(streamID, httpRequest) };
        } else if (msg instanceof HttpResponse) {
            HttpResponse httpResponse = (HttpResponse) msg;
            if (httpResponse.containsHeader(SpdyHttpHeaders.Names.ASSOCIATED_TO_STREAM_ID)) {
                SpdySynStreamFrame spdySynStreamFrame = createSynStreamFrame(httpResponse);
                int streamID = spdySynStreamFrame.getStreamID();
                return new Object[] { spdySynStreamFrame, dataFrame(streamID, httpResponse) };
            } else {
                SpdySynReplyFrame spdySynReplyFrame = createSynReplyFrame(httpResponse);
                int streamID = spdySynReplyFrame.getStreamID();
                return new Object[] { spdySynReplyFrame, dataFrame(streamID, httpResponse) };
            }
        } else if (msg instanceof HttpChunk) {
            HttpChunk chunk = (HttpChunk) msg;
            SpdyDataFrame spdyDataFrame = new DefaultSpdyDataFrame(currentStreamID);
            spdyDataFrame.setData(chunk.getContent());
            spdyDataFrame.setLast(chunk.isLast());
            if (chunk instanceof HttpChunkTrailer) {
                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                List<Map.Entry<String, String>> trailers = trailer.getHeaders();
                if (trailers.isEmpty()) {
                    return spdyDataFrame;
                } else {
                    SpdyHeadersFrame spdyHeadersFrame = new DefaultSpdyHeadersFrame(currentStreamID);
                    for (Map.Entry<String, String> entry : trailers) {
                        spdyHeadersFrame.addHeader(entry.getKey(), entry.getValue());
                    }
                    return new Object[] { spdyHeadersFrame, spdyDataFrame };
                }
            } else {
                return spdyDataFrame;
            }
        } else {
            throw new UnsupportedMessageTypeException();
        }
    }

    private static SpdyDataFrame dataFrame(int streamID, HttpMessage httpMessage) {
        if (httpMessage.getContent().readableBytes() == 0) {
            return null;
        }
        SpdyDataFrame spdyDataFrame = new DefaultSpdyDataFrame(streamID);
        spdyDataFrame.setData(httpMessage.getContent());
        spdyDataFrame.setLast(true);
        return spdyDataFrame;
    }

    private SpdySynStreamFrame createSynStreamFrame(HttpMessage httpMessage) throws Exception {
        boolean chunked = httpMessage.isChunked();
        int streamID = SpdyHttpHeaders.getStreamID(httpMessage);
        int associatedToStreamID = SpdyHttpHeaders.getAssociatedToStreamID(httpMessage);
        byte priority = SpdyHttpHeaders.getPriority(httpMessage);
        String URL = SpdyHttpHeaders.getUrl(httpMessage);
        SpdyHttpHeaders.removeStreamID(httpMessage);
        SpdyHttpHeaders.removeAssociatedToStreamID(httpMessage);
        SpdyHttpHeaders.removePriority(httpMessage);
        SpdyHttpHeaders.removeUrl(httpMessage);
        httpMessage.removeHeader(HttpHeaders.Names.CONNECTION);
        httpMessage.removeHeader("Keep-Alive");
        httpMessage.removeHeader("Proxy-Connection");
        httpMessage.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        SpdySynStreamFrame spdySynStreamFrame = new DefaultSpdySynStreamFrame(streamID, associatedToStreamID, priority);
        for (Map.Entry<String, String> entry : httpMessage.getHeaders()) {
            spdySynStreamFrame.addHeader(entry.getKey(), entry.getValue());
        }
        SpdyHeaders.setVersion(spdySynStreamFrame, httpMessage.getProtocolVersion());
        if (httpMessage instanceof HttpRequest) {
            HttpRequest httpRequest = (HttpRequest) httpMessage;
            SpdyHeaders.setMethod(spdySynStreamFrame, httpRequest.getMethod());
            SpdyHeaders.setUrl(spdySynStreamFrame, httpRequest.getUri());
        }
        if (httpMessage instanceof HttpResponse) {
            HttpResponse httpResponse = (HttpResponse) httpMessage;
            SpdyHeaders.setStatus(spdySynStreamFrame, httpResponse.getStatus());
            SpdyHeaders.setUrl(spdySynStreamFrame, URL);
            spdySynStreamFrame.setUnidirectional(true);
        }
        if (chunked) {
            currentStreamID = streamID;
            spdySynStreamFrame.setLast(false);
        } else {
            spdySynStreamFrame.setLast(httpMessage.getContent().readableBytes() == 0);
        }
        return spdySynStreamFrame;
    }

    private SpdySynReplyFrame createSynReplyFrame(HttpResponse httpResponse) throws Exception {
        boolean chunked = httpResponse.isChunked();
        int streamID = SpdyHttpHeaders.getStreamID(httpResponse);
        SpdyHttpHeaders.removeStreamID(httpResponse);
        httpResponse.removeHeader(HttpHeaders.Names.CONNECTION);
        httpResponse.removeHeader("Keep-Alive");
        httpResponse.removeHeader("Proxy-Connection");
        httpResponse.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        SpdySynReplyFrame spdySynReplyFrame = new DefaultSpdySynReplyFrame(streamID);
        for (Map.Entry<String, String> entry : httpResponse.getHeaders()) {
            spdySynReplyFrame.addHeader(entry.getKey(), entry.getValue());
        }
        SpdyHeaders.setStatus(spdySynReplyFrame, httpResponse.getStatus());
        SpdyHeaders.setVersion(spdySynReplyFrame, httpResponse.getProtocolVersion());
        if (chunked) {
            currentStreamID = streamID;
            spdySynReplyFrame.setLast(false);
        } else {
            spdySynReplyFrame.setLast(httpResponse.getContent().readableBytes() == 0);
        }
        return spdySynReplyFrame;
    }
}