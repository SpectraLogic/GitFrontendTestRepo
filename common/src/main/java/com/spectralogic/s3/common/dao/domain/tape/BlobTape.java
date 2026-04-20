/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ BlobTape.TAPE_ID, BlobTape.ORDER_INDEX })
})
public interface BlobTape extends BlobObservable< BlobTape >, DatabasePersistable
{
    String TAPE_ID = "tapeId";
    
    @References( Tape.class )
    UUID getTapeId();
    
    BlobTape setTapeId( final UUID value );
    
    
    String ORDER_INDEX = "orderIndex";
    
    int getOrderIndex();
    
    BlobTape setOrderIndex( final int value );
}
