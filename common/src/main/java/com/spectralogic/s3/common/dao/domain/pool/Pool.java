/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;


@UniqueIndexes(
{
    @Unique( PoolObservable.GUID ),
    @Unique( PoolObservable.MOUNTPOINT ),
    @Unique( NameObservable.NAME )
})
public interface Pool 
    extends DatabasePersistable, PoolObservable< Pool >, PersistenceTarget< Pool >
{
    String STATE = "state";
    
    PoolState getState();
    
    Pool setState( final PoolState value );
    
    
    String PARTITION_ID = "partitionId";
    
    @Optional
    @References( PoolPartition.class )
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    UUID getPartitionId();
    
    Pool setPartitionId( final UUID value );
    
    
    String QUIESCED = "quiesced";
    
    @DefaultEnumValue( "NO" )
    Quiesced getQuiesced();
    
    Pool setQuiesced( final Quiesced value );
}
