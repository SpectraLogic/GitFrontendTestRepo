/*******************************************************************************
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.db.lang.*;

/**
 * A blob persisted to a pool that has been obsoleted and is no longer required to be kept.
 */
@UniqueIndexes(
{
    @Unique({ ObsoleteBlobPool.OBSOLETION_ID, BlobObservable.BLOB_ID })
})
public interface ObsoleteBlobPool extends BlobPool
{
    @References( BlobPool.class )
    @CascadeDelete
    UUID getId();

    ObsoleteBlobPool setId( final UUID value );

    String OBSOLETION_ID = "obsoletionId";

    @References( Obsoletion.class )
    @CascadeDelete( CascadeDelete.WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getObsoletionId();

    ObsoleteBlobPool setObsoletionId( final UUID value );
}
