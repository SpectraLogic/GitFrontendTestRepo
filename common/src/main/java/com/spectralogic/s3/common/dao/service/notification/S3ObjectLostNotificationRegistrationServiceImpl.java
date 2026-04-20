/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectLostNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class S3ObjectLostNotificationRegistrationServiceImpl
    extends BaseService< S3ObjectLostNotificationRegistration >
    implements S3ObjectLostNotificationRegistrationService
{
    S3ObjectLostNotificationRegistrationServiceImpl()
    {
        super( S3ObjectLostNotificationRegistration.class );
    }
}
