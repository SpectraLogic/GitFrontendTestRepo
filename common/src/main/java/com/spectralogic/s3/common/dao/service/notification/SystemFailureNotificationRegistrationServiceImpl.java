/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.notification;

import com.spectralogic.s3.common.dao.domain.notification.SystemFailureNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class SystemFailureNotificationRegistrationServiceImpl
    extends BaseService< SystemFailureNotificationRegistration > 
    implements SystemFailureNotificationRegistrationService
{
    SystemFailureNotificationRegistrationServiceImpl()
    {
        super( SystemFailureNotificationRegistration.class );
    }
}
