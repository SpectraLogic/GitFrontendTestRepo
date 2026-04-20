/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class S3ObjectPersistedNotificationRegistrationServiceImpl
    extends BaseService< S3ObjectPersistedNotificationRegistration >
    implements S3ObjectPersistedNotificationRegistrationService
{
    S3ObjectPersistedNotificationRegistrationServiceImpl()
    {
        super( S3ObjectPersistedNotificationRegistration.class );
    }
}
