package io.netty.channel.sctp;

import com.sun.nio.sctp.AbstractNotificationHandler;
import com.sun.nio.sctp.AssociationChangeNotification;
import com.sun.nio.sctp.HandlerResult;
import com.sun.nio.sctp.Notification;
import com.sun.nio.sctp.PeerAddressChangeNotification;
import com.sun.nio.sctp.SendFailedNotification;
import com.sun.nio.sctp.ShutdownNotification;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.Channels;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

class SctpNotificationHandler extends AbstractNotificationHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SctpNotificationHandler.class);

    private final SctpChannelImpl sctpChannel;

    private final ChannelPipeline pipeline;

    SctpNotificationHandler(SctpChannelImpl sctpChannel) {
        this.sctpChannel = sctpChannel;
        this.pipeline = sctpChannel.getPipeline();
    }

    @Override
    public HandlerResult handleNotification(AssociationChangeNotification notification, Object o) {
        fireNotificationReceived(notification, o);
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(PeerAddressChangeNotification notification, Object o) {
        fireNotificationReceived(notification, o);
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(SendFailedNotification notification, Object o) {
        fireNotificationReceived(notification, o);
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(ShutdownNotification notification, Object o) {
        Channels.fireChannelDisconnected(sctpChannel);
        return HandlerResult.RETURN;
    }

    private void fireNotificationReceived(Notification notification, Object o) {
        pipeline.sendUpstream(new SctpNotificationEvent(sctpChannel, notification, o));
    }
}