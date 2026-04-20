/*******************************************************************************
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.db.lang.*;

import java.util.UUID;

/**
 * A blob persisted to a tape that has been obsoleted and is no longer required to be kept.
 */
@UniqueIndexes(
{
    @Unique({ ObsoleteBlobTape.OBSOLETION_ID, BlobObservable.BLOB_ID })
})
public interface ObsoleteBlobTape extends BlobTape
{
    @References( BlobTape.class )
    @CascadeDelete
    UUID getId();

    ObsoleteBlobTape setId( final UUID value );

    String OBSOLETION_ID = "obsoletionId";

    @References( Obsoletion.class )
    @CascadeDelete( CascadeDelete.WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getObsoletionId();

    ObsoleteBlobTape setObsoletionId( final UUID value );
}
