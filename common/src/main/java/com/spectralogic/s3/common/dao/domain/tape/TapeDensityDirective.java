/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.UUID;

import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ TapeDensityDirective.PARTITION_ID, TapeDensityDirective.TAPE_TYPE }),
})
public interface TapeDensityDirective extends DatabasePersistable
{
    String PARTITION_ID = "partitionId";

    @References( TapePartition.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getPartitionId();
    
    TapeDensityDirective setPartitionId( final UUID value );
    
    
    String TAPE_TYPE = "tapeType";
    
    TapeType getTapeType();
    
    TapeDensityDirective setTapeType( final TapeType value );
    
    
    String DENSITY = "density";
    
    TapeDriveType getDensity();
    
    TapeDensityDirective setDensity( final TapeDriveType value );
}
