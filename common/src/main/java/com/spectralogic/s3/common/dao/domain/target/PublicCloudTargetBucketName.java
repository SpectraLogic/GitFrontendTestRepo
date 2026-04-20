/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.References;

public interface PublicCloudTargetBucketName< T > extends NameObservable< T >
{
    String BUCKET_ID = "bucketId";

    @CascadeDelete
    @References( Bucket.class )
    UUID getBucketId();

    T setBucketId( final UUID value );


    String TARGET_ID = "targetId";

    UUID getTargetId();

    T setTargetId( final UUID value );
}
