package com.spectralogic.util.notification.dispatch;

import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public interface HttpNotifcationListener extends NotificationListener
{
    public HttpNotificationRegistration<?> getRegistration();
}
