package io.netty.handler.codec.spdy;

import java.util.List;
import java.util.Map;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.Channels;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelUpstreamHandler;
import io.netty.handler.codec.embedder.DecoderEmbedder;
import org.junit.Assert;
import org.junit.Test;

public class SpdySessionHandlerTest {

    private static final int closeSignal = SpdyCodecUtil.SPDY_SETTINGS_MAX_ID;

    private static final SpdySettingsFrame closeMessage = new DefaultSpdySettingsFrame();

    static {
        closeMessage.setValue(closeSignal, 0);
    }

    private static void assertHeaderBlock(SpdyHeaderBlock received, SpdyHeaderBlock expected) {
        for (String name : expected.getHeaderNames()) {
            List<String> expectedValues = expected.getHeaders(name);
            List<String> receivedValues = received.getHeaders(name);
            Assert.assertTrue(receivedValues.containsAll(expectedValues));
            receivedValues.removeAll(expectedValues);
            Assert.assertTrue(receivedValues.isEmpty());
            received.removeHeader(name);
        }
        Assert.assertTrue(received.getHeaders().isEmpty());
    }

    private static void assertDataFrame(Object msg, int streamID, boolean last) {
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof SpdyDataFrame);
        SpdyDataFrame spdyDataFrame = (SpdyDataFrame) msg;
        Assert.assertTrue(spdyDataFrame.getStreamID() == streamID);
        Assert.assertTrue(spdyDataFrame.isLast() == last);
    }

    private static void assertSynReply(Object msg, int streamID, boolean last, SpdyHeaderBlock headers) {
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof SpdySynReplyFrame);
        SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
        Assert.assertTrue(spdySynReplyFrame.getStreamID() == streamID);
        Assert.assertTrue(spdySynReplyFrame.isLast() == last);
        assertHeaderBlock(spdySynReplyFrame, headers);
    }

    private static void assertRstStream(Object msg, int streamID, SpdyStreamStatus status) {
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof SpdyRstStreamFrame);
        SpdyRstStreamFrame spdyRstStreamFrame = (SpdyRstStreamFrame) msg;
        Assert.assertTrue(spdyRstStreamFrame.getStreamID() == streamID);
        Assert.assertTrue(spdyRstStreamFrame.getStatus().equals(status));
    }

    private static void assertPing(Object msg, int ID) {
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof SpdyPingFrame);
        SpdyPingFrame spdyPingFrame = (SpdyPingFrame) msg;
        Assert.assertTrue(spdyPingFrame.getID() == ID);
    }

    private static void assertGoAway(Object msg, int lastGoodStreamID) {
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof SpdyGoAwayFrame);
        SpdyGoAwayFrame spdyGoAwayFrame = (SpdyGoAwayFrame) msg;
        Assert.assertTrue(spdyGoAwayFrame.getLastGoodStreamID() == lastGoodStreamID);
    }

    private static void assertHeaders(Object msg, int streamID, SpdyHeaderBlock headers) {
        Assert.assertNotNull(msg);
        Assert.assertTrue(msg instanceof SpdyHeadersFrame);
        SpdyHeadersFrame spdyHeadersFrame = (SpdyHeadersFrame) msg;
        Assert.assertTrue(spdyHeadersFrame.getStreamID() == streamID);
        assertHeaderBlock(spdyHeadersFrame, headers);
    }

    private void testSpdySessionHandler(int version, boolean server) {
        DecoderEmbedder<Object> sessionHandler = new DecoderEmbedder<Object>(new SpdySessionHandler(version, server), new EchoHandler(closeSignal, server));
        sessionHandler.pollAll();
        int localStreamID = server ? 1 : 2;
        int remoteStreamID = server ? 2 : 1;
        SpdyPingFrame localPingFrame = new DefaultSpdyPingFrame(localStreamID);
        SpdyPingFrame remotePingFrame = new DefaultSpdyPingFrame(remoteStreamID);
        SpdySynStreamFrame spdySynStreamFrame = new DefaultSpdySynStreamFrame(localStreamID, 0, (byte) 0);
        spdySynStreamFrame.setHeader("Compression", "test");
        SpdyDataFrame spdyDataFrame = new DefaultSpdyDataFrame(localStreamID);
        spdyDataFrame.setLast(true);
        sessionHandler.offer(new DefaultSpdyDataFrame(localStreamID));
        assertRstStream(sessionHandler.poll(), localStreamID, SpdyStreamStatus.INVALID_STREAM);
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(new DefaultSpdyDataFrame(remoteStreamID));
        assertRstStream(sessionHandler.poll(), remoteStreamID, SpdyStreamStatus.PROTOCOL_ERROR);
        Assert.assertNull(sessionHandler.peek());
        remoteStreamID += 2;
        sessionHandler.offer(new DefaultSpdySynReplyFrame(remoteStreamID));
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(new DefaultSpdySynReplyFrame(remoteStreamID));
        assertRstStream(sessionHandler.poll(), remoteStreamID, SpdyStreamStatus.STREAM_IN_USE);
        Assert.assertNull(sessionHandler.peek());
        remoteStreamID += 2;
        sessionHandler.offer(spdySynStreamFrame);
        assertSynReply(sessionHandler.poll(), localStreamID, false, spdySynStreamFrame);
        Assert.assertNull(sessionHandler.peek());
        SpdyHeadersFrame spdyHeadersFrame = new DefaultSpdyHeadersFrame(localStreamID);
        spdyHeadersFrame.addHeader("HEADER", "test1");
        spdyHeadersFrame.addHeader("HEADER", "test2");
        sessionHandler.offer(spdyHeadersFrame);
        assertHeaders(sessionHandler.poll(), localStreamID, spdyHeadersFrame);
        Assert.assertNull(sessionHandler.peek());
        localStreamID += 2;
        spdySynStreamFrame.setStreamID(localStreamID);
        spdySynStreamFrame.setLast(true);
        spdySynStreamFrame.setUnidirectional(true);
        sessionHandler.offer(spdySynStreamFrame);
        assertRstStream(sessionHandler.poll(), localStreamID, SpdyStreamStatus.REFUSED_STREAM);
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(new DefaultSpdyRstStreamFrame(remoteStreamID, 3));
        Assert.assertNull(sessionHandler.peek());
        remoteStreamID += 2;
        spdySynStreamFrame.setLast(false);
        sessionHandler.offer(spdySynStreamFrame);
        Assert.assertNull(sessionHandler.peek());
        spdySynStreamFrame.setUnidirectional(false);
        sessionHandler.offer(spdySynStreamFrame);
        assertRstStream(sessionHandler.poll(), localStreamID, SpdyStreamStatus.PROTOCOL_ERROR);
        Assert.assertNull(sessionHandler.peek());
        localStreamID += 2;
        spdySynStreamFrame.setStreamID(localStreamID - 1);
        sessionHandler.offer(spdySynStreamFrame);
        assertRstStream(sessionHandler.poll(), localStreamID - 1, SpdyStreamStatus.PROTOCOL_ERROR);
        Assert.assertNull(sessionHandler.peek());
        spdySynStreamFrame.setStreamID(localStreamID);
        SpdySettingsFrame spdySettingsFrame = new DefaultSpdySettingsFrame();
        spdySettingsFrame.setValue(SpdySettingsFrame.SETTINGS_MAX_CONCURRENT_STREAMS, 2);
        sessionHandler.offer(spdySettingsFrame);
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(spdySynStreamFrame);
        assertRstStream(sessionHandler.poll(), localStreamID, SpdyStreamStatus.REFUSED_STREAM);
        Assert.assertNull(sessionHandler.peek());
        spdySettingsFrame.setValue(SpdySettingsFrame.SETTINGS_MAX_CONCURRENT_STREAMS, 4);
        sessionHandler.offer(spdySettingsFrame);
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(spdySynStreamFrame);
        assertSynReply(sessionHandler.poll(), localStreamID, false, spdySynStreamFrame);
        Assert.assertNull(sessionHandler.peek());
        int testStreamID = spdyDataFrame.getStreamID();
        sessionHandler.offer(spdyDataFrame);
        assertDataFrame(sessionHandler.poll(), testStreamID, spdyDataFrame.isLast());
        Assert.assertNull(sessionHandler.peek());
        spdyHeadersFrame.setStreamID(testStreamID);
        sessionHandler.offer(spdyHeadersFrame);
        assertRstStream(sessionHandler.poll(), testStreamID, SpdyStreamStatus.INVALID_STREAM);
        Assert.assertNull(sessionHandler.peek());
        spdyHeadersFrame.setStreamID(localStreamID);
        spdyHeadersFrame.setInvalid();
        sessionHandler.offer(spdyHeadersFrame);
        assertRstStream(sessionHandler.poll(), localStreamID, SpdyStreamStatus.PROTOCOL_ERROR);
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(localPingFrame);
        assertPing(sessionHandler.poll(), localPingFrame.getID());
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(remotePingFrame);
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.offer(closeMessage);
        assertGoAway(sessionHandler.poll(), localStreamID);
        Assert.assertNull(sessionHandler.peek());
        localStreamID += 2;
        spdySynStreamFrame.setStreamID(localStreamID);
        sessionHandler.offer(spdySynStreamFrame);
        assertRstStream(sessionHandler.poll(), localStreamID, SpdyStreamStatus.REFUSED_STREAM);
        Assert.assertNull(sessionHandler.peek());
        spdyDataFrame.setStreamID(localStreamID);
        sessionHandler.offer(spdyDataFrame);
        Assert.assertNull(sessionHandler.peek());
        sessionHandler.finish();
    }

    @Test
    public void testSpdyClientSessionHandler() {
        testSpdySessionHandler(2, false);
    }

    @Test
    public void testSpdyServerSessionHandler() {
        testSpdySessionHandler(2, true);
    }

    private class EchoHandler extends SimpleChannelUpstreamHandler {

        private final int closeSignal;

        private final boolean server;

        EchoHandler(int closeSignal, boolean server) {
            super();
            this.closeSignal = closeSignal;
            this.server = server;
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            int streamID = server ? 2 : 1;
            SpdySynStreamFrame spdySynStreamFrame = new DefaultSpdySynStreamFrame(streamID, 0, (byte) 0);
            spdySynStreamFrame.setLast(true);
            Channels.write(e.getChannel(), spdySynStreamFrame);
            spdySynStreamFrame.setStreamID(spdySynStreamFrame.getStreamID() + 2);
            Channels.write(e.getChannel(), spdySynStreamFrame);
            spdySynStreamFrame.setStreamID(spdySynStreamFrame.getStreamID() + 2);
            Channels.write(e.getChannel(), spdySynStreamFrame);
            spdySynStreamFrame.setStreamID(spdySynStreamFrame.getStreamID() + 2);
            Channels.write(e.getChannel(), spdySynStreamFrame);
            SpdySettingsFrame spdySettingsFrame = new DefaultSpdySettingsFrame();
            spdySettingsFrame.setValue(SpdySettingsFrame.SETTINGS_MAX_CONCURRENT_STREAMS, 3);
            Channels.write(e.getChannel(), spdySettingsFrame);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Object msg = e.getMessage();
            if (msg instanceof SpdyDataFrame || msg instanceof SpdyPingFrame || msg instanceof SpdyHeadersFrame) {
                Channels.write(e.getChannel(), msg, e.getRemoteAddress());
                return;
            }
            if (msg instanceof SpdySynStreamFrame) {
                SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
                int streamID = spdySynStreamFrame.getStreamID();
                SpdySynReplyFrame spdySynReplyFrame = new DefaultSpdySynReplyFrame(streamID);
                spdySynReplyFrame.setLast(spdySynStreamFrame.isLast());
                for (Map.Entry<String, String> entry : spdySynStreamFrame.getHeaders()) {
                    spdySynReplyFrame.addHeader(entry.getKey(), entry.getValue());
                }
                Channels.write(e.getChannel(), spdySynReplyFrame, e.getRemoteAddress());
                return;
            }
            if (msg instanceof SpdySettingsFrame) {
                SpdySettingsFrame spdySettingsFrame = (SpdySettingsFrame) msg;
                if (spdySettingsFrame.isSet(closeSignal)) {
                    Channels.close(e.getChannel());
                }
            }
        }
    }
}