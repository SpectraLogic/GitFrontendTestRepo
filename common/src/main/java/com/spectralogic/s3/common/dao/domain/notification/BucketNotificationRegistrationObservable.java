/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.notification;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

import java.util.UUID;

public interface BucketNotificationRegistrationObservable< T extends HttpNotificationRegistration< T > >
    extends NotificationRegistrationObservable< T >
{
    String BUCKET_ID = "bucketId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Bucket.class )
    UUID getBucketId();

    void setBucketId(final UUID value);
}
