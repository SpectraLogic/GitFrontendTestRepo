/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain.bean;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.notification.domain.NotificationEvent;

public interface BeanNotificationEvent< R extends SimpleBeanSafeToProxy >
    extends NotificationEvent< R >
{
    BeansRetriever< R > getNotificationReceiverRetriever();
}
