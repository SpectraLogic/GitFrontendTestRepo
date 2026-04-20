/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public interface TestNotificationRegistration 
    extends DatabasePersistable, HttpNotificationRegistration< TestNotificationRegistration >
{
    // empty
}
