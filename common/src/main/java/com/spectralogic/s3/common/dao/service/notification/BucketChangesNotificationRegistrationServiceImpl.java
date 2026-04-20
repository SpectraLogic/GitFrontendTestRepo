/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class BucketChangesNotificationRegistrationServiceImpl
    extends BaseService<BucketChangesNotificationRegistration>
    implements BucketChangesNotificationRegistrationService
{
    BucketChangesNotificationRegistrationServiceImpl()
    {
        super( BucketChangesNotificationRegistration.class );
    }
}
