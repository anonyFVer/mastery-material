package io.netty.example.qotm;

import io.netty.channel.ChannelInboundHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;

public class QuoteOfTheMomentClientHandler extends ChannelInboundMessageHandlerAdapter<DatagramPacket> {

    @Override
    public void messageReceived(ChannelInboundHandlerContext<DatagramPacket> ctx, DatagramPacket msg) throws Exception {
        String response = msg.data().toString(CharsetUtil.UTF_8);
        if (response.startsWith("QOTM: ")) {
            System.out.println("Quote of the Moment: " + response.substring(6));
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelInboundHandlerContext<DatagramPacket> ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}