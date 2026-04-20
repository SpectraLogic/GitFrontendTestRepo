/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.util.db.lang.*;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
@UniqueIndexes(
{
    @Unique({ LocalBlobDestination.ENTRY_ID, RemoteBlobDestination.TARGET_ID })
})
public interface S3BlobDestination extends DatabasePersistable, RemoteBlobDestination<S3BlobDestination>
{
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( S3Target.class )
    UUID getTargetId();
}
