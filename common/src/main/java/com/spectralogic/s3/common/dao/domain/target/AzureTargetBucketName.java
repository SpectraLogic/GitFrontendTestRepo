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
@Indexes( @Index( AzureTargetBucketName.NAME ) )
public interface AzureTargetBucketName
    extends PublicCloudTargetBucketName< AzureTargetBucketName >, DatabasePersistable
{
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( AzureTarget.class )
    UUID getTargetId();
}
