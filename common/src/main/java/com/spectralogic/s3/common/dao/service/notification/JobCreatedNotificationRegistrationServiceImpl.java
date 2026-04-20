/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.JobCreatedNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class JobCreatedNotificationRegistrationServiceImpl 
    extends BaseService< JobCreatedNotificationRegistration >
    implements JobCreatedNotificationRegistrationService
{
    JobCreatedNotificationRegistrationServiceImpl()
    {
        super( JobCreatedNotificationRegistration.class );
    }
}
