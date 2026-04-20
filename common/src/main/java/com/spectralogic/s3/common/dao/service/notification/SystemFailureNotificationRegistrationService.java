/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.SystemFailureNotificationRegistration;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface SystemFailureNotificationRegistrationService
    extends BeansRetriever< SystemFailureNotificationRegistration >, 
            BeanCreator< SystemFailureNotificationRegistration >, 
            BeanUpdater< SystemFailureNotificationRegistration >,
            BeanDeleter
{
    // empty
}
