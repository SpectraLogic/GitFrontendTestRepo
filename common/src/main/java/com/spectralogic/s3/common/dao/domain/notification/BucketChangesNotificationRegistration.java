package com.spectralogic.s3.common.dao.domain.notification;

/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.notification.domain.bean.SequencedNotificationRegistrationObservable;

@Indexes( @Index( BucketChangesNotificationRegistration.CREATION_DATE ) )
public interface BucketChangesNotificationRegistration
        extends BucketNotificationRegistrationObservable<BucketChangesNotificationRegistration>,
        SequencedNotificationRegistrationObservable<BucketChangesNotificationRegistration>,
                DatabasePersistable
{
}