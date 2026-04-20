/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.notification;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;

@Indexes( @Index( S3TargetFailureNotificationRegistration.CREATION_DATE ) )
public interface S3TargetFailureNotificationRegistration
    extends NotificationRegistrationObservable< S3TargetFailureNotificationRegistration >, 
            DatabasePersistable
{
    // empty
}
