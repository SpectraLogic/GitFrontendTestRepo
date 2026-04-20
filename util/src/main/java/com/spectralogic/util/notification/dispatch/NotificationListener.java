package com.spectralogic.util.notification.dispatch;

import com.spectralogic.util.notification.domain.Notification;

public interface NotificationListener
{
    public void fire( final Notification event );
}