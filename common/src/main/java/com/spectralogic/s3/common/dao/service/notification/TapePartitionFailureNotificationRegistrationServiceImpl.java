/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.TapePartitionFailureNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class TapePartitionFailureNotificationRegistrationServiceImpl
    extends BaseService< TapePartitionFailureNotificationRegistration >
    implements TapePartitionFailureNotificationRegistrationService
{
    TapePartitionFailureNotificationRegistrationServiceImpl()
    {
        super( TapePartitionFailureNotificationRegistration.class );
    }
}
