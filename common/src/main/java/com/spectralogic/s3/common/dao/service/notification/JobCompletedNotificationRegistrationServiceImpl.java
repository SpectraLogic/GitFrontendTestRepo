/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.JobCompletedNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class JobCompletedNotificationRegistrationServiceImpl
    extends BaseService< JobCompletedNotificationRegistration >
    implements JobCompletedNotificationRegistrationService
{
    JobCompletedNotificationRegistrationServiceImpl()
    {
        super( JobCompletedNotificationRegistration.class );
    }
}
