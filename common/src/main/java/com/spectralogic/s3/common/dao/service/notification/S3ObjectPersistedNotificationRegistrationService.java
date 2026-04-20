/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface S3ObjectPersistedNotificationRegistrationService
    extends BeansRetriever< S3ObjectPersistedNotificationRegistration >, 
            BeanCreator< S3ObjectPersistedNotificationRegistration >, 
            BeanUpdater< S3ObjectPersistedNotificationRegistration >,
            BeanDeleter
{
    // empty
}
