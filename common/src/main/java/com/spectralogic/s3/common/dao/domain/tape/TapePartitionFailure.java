/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes( @Index( TapePartitionFailure.DATE ) )
public interface TapePartitionFailure
    extends DatabasePersistable, Failure< TapePartitionFailure, TapePartitionFailureType >
{
    String PARTITION_ID = "partitionId";
    
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( TapePartition.class )
    UUID getPartitionId();
    
    TapePartitionFailure setPartitionId( final UUID value );
    
    
    TapePartitionFailureType getType();
    
    TapePartitionFailure setType( final TapePartitionFailureType value );
}
