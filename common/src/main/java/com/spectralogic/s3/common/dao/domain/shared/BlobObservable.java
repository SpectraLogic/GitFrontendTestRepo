/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;

public interface BlobObservable< T >
{
    String BLOB_ID = "blobId";

    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Blob.class )
    UUID getBlobId();
    
    T setBlobId( final UUID value );
}
