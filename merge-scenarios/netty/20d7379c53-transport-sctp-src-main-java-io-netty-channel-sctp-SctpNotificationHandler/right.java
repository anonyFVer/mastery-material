package io.netty.channel.sctp;

import com.sun.nio.sctp.AbstractNotificationHandler;
import com.sun.nio.sctp.AssociationChangeNotification;
import com.sun.nio.sctp.HandlerResult;
import com.sun.nio.sctp.Notification;
import com.sun.nio.sctp.PeerAddressChangeNotification;
import com.sun.nio.sctp.SendFailedNotification;
import com.sun.nio.sctp.ShutdownNotification;
import io.netty.channel.Channels;

class SctpNotificationHandler extends AbstractNotificationHandler {

    private final SctpChannelImpl sctpChannel;

    private final SctpWorker sctpWorker;

    public SctpNotificationHandler(SctpChannelImpl sctpChannel, SctpWorker sctpWorker) {
        this.sctpChannel = sctpChannel;
        this.sctpWorker = sctpWorker;
    }

    @Override
    public HandlerResult handleNotification(AssociationChangeNotification notification, Object o) {
        fireNotificationReceived(notification, o);
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(Notification notification, Object o) {
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
        sctpWorker.close(sctpChannel, Channels.succeededFuture(sctpChannel));
        return HandlerResult.RETURN;
    }

    private void fireNotificationReceived(Notification notification, Object o) {
        sctpChannel.getPipeline().sendUpstream(new SctpNotificationEvent(sctpChannel, notification, o));
    }
}