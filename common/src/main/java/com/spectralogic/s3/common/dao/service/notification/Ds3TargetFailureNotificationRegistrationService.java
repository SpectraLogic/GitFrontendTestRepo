/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.Ds3TargetFailureNotificationRegistration;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface Ds3TargetFailureNotificationRegistrationService
    extends BeansRetriever< Ds3TargetFailureNotificationRegistration >, 
            BeanCreator< Ds3TargetFailureNotificationRegistration >, 
            BeanUpdater< Ds3TargetFailureNotificationRegistration >,
            BeanDeleter
{
    // empty
}
