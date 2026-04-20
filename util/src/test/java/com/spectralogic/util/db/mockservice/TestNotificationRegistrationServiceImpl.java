/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import com.spectralogic.util.db.mockdomain.TestNotificationRegistration;
import com.spectralogic.util.db.service.BaseService;

final class TestNotificationRegistrationServiceImpl
    extends BaseService< TestNotificationRegistration > implements TestNotificationRegistrationService
{
    TestNotificationRegistrationServiceImpl()
    {
        super( TestNotificationRegistration.class );
    }
}
