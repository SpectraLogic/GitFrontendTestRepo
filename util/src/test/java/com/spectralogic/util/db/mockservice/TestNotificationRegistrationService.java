/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import com.spectralogic.util.db.mockdomain.TestNotificationRegistration;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface TestNotificationRegistrationService 
    extends BeansRetriever< TestNotificationRegistration >,
            BeanUpdater< TestNotificationRegistration >,
            BeanCreator< TestNotificationRegistration >
{
    // empty
}
