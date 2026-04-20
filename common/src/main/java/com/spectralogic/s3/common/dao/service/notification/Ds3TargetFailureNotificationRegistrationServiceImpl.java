/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.Ds3TargetFailureNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class Ds3TargetFailureNotificationRegistrationServiceImpl
    extends BaseService< Ds3TargetFailureNotificationRegistration >
    implements Ds3TargetFailureNotificationRegistrationService
{
    Ds3TargetFailureNotificationRegistrationServiceImpl()
    {
        super( Ds3TargetFailureNotificationRegistration.class );
    }
}
