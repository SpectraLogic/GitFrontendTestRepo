/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class S3ObjectCachedNotificationRegistrationServiceImpl 
    extends BaseService< S3ObjectCachedNotificationRegistration >
    implements S3ObjectCachedNotificationRegistrationService
{
    S3ObjectCachedNotificationRegistrationServiceImpl()
    {
        super( S3ObjectCachedNotificationRegistration.class );
    }
}
