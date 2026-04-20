package com.spectralogic.util.notification.domain;

import com.spectralogic.util.notification.dispatch.NotificationListener;

public interface DynamicNotificationPayloadGenerator extends NotificationPayloadGenerator
{
    @Override
    default NotificationPayload generateNotificationPayload() {
        throw new UnsupportedOperationException("Must specify listener to generate payload.");
    }

    NotificationPayload generateNotificationPayload( final NotificationListener listener );
}