/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3TargetFailureNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class S3TargetFailureNotificationRegistrationServiceImpl
    extends BaseService< S3TargetFailureNotificationRegistration >
    implements S3TargetFailureNotificationRegistrationService
{
    S3TargetFailureNotificationRegistrationServiceImpl()
    {
        super( S3TargetFailureNotificationRegistration.class );
    }
}
