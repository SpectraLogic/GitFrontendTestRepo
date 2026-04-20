/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectLostNotificationRegistration;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface S3ObjectLostNotificationRegistrationService
    extends BeansRetriever< S3ObjectLostNotificationRegistration >, 
            BeanCreator< S3ObjectLostNotificationRegistration >, 
            BeanUpdater< S3ObjectLostNotificationRegistration >,
            BeanDeleter
{
    // empty
}
