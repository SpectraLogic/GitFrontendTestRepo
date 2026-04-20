/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.StorageDomainFailureNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class StorageDomainFailureNotificationRegistrationServiceImpl
    extends BaseService< StorageDomainFailureNotificationRegistration >
    implements StorageDomainFailureNotificationRegistrationService
{
    StorageDomainFailureNotificationRegistrationServiceImpl()
    {
        super( StorageDomainFailureNotificationRegistration.class );
    }
}
