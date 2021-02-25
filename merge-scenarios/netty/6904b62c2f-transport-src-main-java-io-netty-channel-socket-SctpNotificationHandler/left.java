package io.netty.channel.socket;

import com.sun.nio.sctp.AbstractNotificationHandler;
import com.sun.nio.sctp.AssociationChangeNotification;
import com.sun.nio.sctp.HandlerResult;
import com.sun.nio.sctp.Notification;
import com.sun.nio.sctp.PeerAddressChangeNotification;
import com.sun.nio.sctp.SendFailedNotification;
import com.sun.nio.sctp.ShutdownNotification;

public class SctpNotificationHandler extends AbstractNotificationHandler<Object> {

    private final SctpChannel sctpChannel;

    public SctpNotificationHandler(SctpChannel sctpChannel) {
        this.sctpChannel = sctpChannel;
    }

    @Override
    public HandlerResult handleNotification(AssociationChangeNotification notification, Object o) {
        updateInboundBuffer(notification, o);
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(PeerAddressChangeNotification notification, Object o) {
        updateInboundBuffer(notification, o);
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(SendFailedNotification notification, Object o) {
        updateInboundBuffer(notification, o);
        return HandlerResult.CONTINUE;
    }

    @Override
    public HandlerResult handleNotification(ShutdownNotification notification, Object o) {
        updateInboundBuffer(notification, o);
        sctpChannel.close();
        return HandlerResult.RETURN;
    }

    private void updateInboundBuffer(Notification notification, Object o) {
        sctpChannel.pipeline().fireUserEventTriggered(new SctpNotificationEvent(notification, o));
    }
}