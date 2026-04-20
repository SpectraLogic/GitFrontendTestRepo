/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A blob persisted to a tape that we suspect has been lost.  Since our belief that the blob has been lost
 * could be incorrect either due to a software bug or transient or correctable hardware issue, rather than
 * simply recording the blob as having been lost, we first require the user to either confirm or clear 
 * suspected data loss rather than taking any automated action.
 */
@UniqueIndexes(
{
    @Unique({ BlobObservable.BLOB_ID, BlobTape.TAPE_ID }),
    @Unique({ BlobTape.ORDER_INDEX, BlobTape.TAPE_ID })
})
public interface SuspectBlobTape extends BlobTape
{
    @CascadeDelete
    @References( BlobTape.class )
    UUID getId();
}
