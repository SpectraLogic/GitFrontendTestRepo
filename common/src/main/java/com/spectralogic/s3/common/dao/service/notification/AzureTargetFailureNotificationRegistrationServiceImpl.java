/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.AzureTargetFailureNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class AzureTargetFailureNotificationRegistrationServiceImpl
    extends BaseService< AzureTargetFailureNotificationRegistration >
    implements AzureTargetFailureNotificationRegistrationService
{
    AzureTargetFailureNotificationRegistrationServiceImpl()
    {
        super( AzureTargetFailureNotificationRegistration.class );
    }
}
