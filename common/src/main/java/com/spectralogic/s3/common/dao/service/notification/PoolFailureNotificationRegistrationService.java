/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.PoolFailureNotificationRegistration;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface PoolFailureNotificationRegistrationService
    extends BeansRetriever< PoolFailureNotificationRegistration >, 
            BeanCreator< PoolFailureNotificationRegistration >, 
            BeanUpdater< PoolFailureNotificationRegistration >,
            BeanDeleter
{
    // empty
}