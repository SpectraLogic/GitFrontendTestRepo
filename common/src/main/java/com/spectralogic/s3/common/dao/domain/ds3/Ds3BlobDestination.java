/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.db.lang.*;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
@UniqueIndexes(
{
    @Unique({ LocalBlobDestination.ENTRY_ID, RemoteBlobDestination.TARGET_ID })
})
public interface Ds3BlobDestination extends DatabasePersistable, RemoteBlobDestination<Ds3BlobDestination>
{
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Ds3Target.class )
    UUID getTargetId();
    
    
    String COMMITTED = "committed";

    @DefaultBooleanValue( false )
    boolean isCommitted();
    
    Ds3BlobDestination setCommitted(final boolean value );
}
