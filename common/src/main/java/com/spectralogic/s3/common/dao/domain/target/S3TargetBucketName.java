/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes( @Unique(
        { 
            PublicCloudTargetBucketName.BUCKET_ID,
            PublicCloudTargetBucketName.TARGET_ID
        } ) )
@Indexes( @Index( S3TargetBucketName.NAME ) )
public interface S3TargetBucketName
    extends PublicCloudTargetBucketName< S3TargetBucketName >, DatabasePersistable
{
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( S3Target.class )
    UUID getTargetId();
}
