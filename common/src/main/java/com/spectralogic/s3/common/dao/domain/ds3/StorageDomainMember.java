/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A storage domain member consists of either a ({@link #TAPE_PARTITION_ID} and {@link #TAPE_TYPE}), or a
 * {@link #POOL_PARTITION_ID}.
 */
@UniqueIndexes(
{
    @Unique(
            { 
                StorageDomainMember.STORAGE_DOMAIN_ID,
                StorageDomainMember.POOL_PARTITION_ID
            }),
    @Unique( 
            { 
                StorageDomainMember.STORAGE_DOMAIN_ID,
                StorageDomainMember.TAPE_PARTITION_ID, 
                StorageDomainMember.TAPE_TYPE 
            })
})
@Indexes( { @Index( StorageDomainMember.STATE ), @Index( StorageDomainMember.WRITE_PREFERENCE ) } )
public interface StorageDomainMember extends DatabasePersistable
{
    String STATE = "state";
    
    @SortBy( 1 )
    @DefaultEnumValue( "NORMAL" )
    StorageDomainMemberState getState();
    
    StorageDomainMember setState( final StorageDomainMemberState value );
    
    
    String STORAGE_DOMAIN_ID = "storageDomainId";
    
    @SortBy( 2 )
    @References( StorageDomain.class )
    UUID getStorageDomainId();
    
    StorageDomainMember setStorageDomainId( final UUID value );
    
    
    String TAPE_PARTITION_ID = "tapePartitionId";
    
    @Optional
    @References( TapePartition.class )
    @CascadeDelete( CascadeDelete.WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getTapePartitionId();
    
    StorageDomainMember setTapePartitionId( final UUID value );
    
    
    String TAPE_TYPE = "tapeType";
    
    @Optional
    TapeType getTapeType();
    
    StorageDomainMember setTapeType( final TapeType value );
    
    
    String POOL_PARTITION_ID = "poolPartitionId";
    
    @Optional
    @References( PoolPartition.class )
    @CascadeDelete( CascadeDelete.WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getPoolPartitionId();
    
    StorageDomainMember setPoolPartitionId( final UUID value );
    
    
    String WRITE_PREFERENCE = "writePreference";
    
    @SortBy( 3 )
    @DefaultEnumValue( "NORMAL" )
    WritePreferenceLevel getWritePreference();
    
    StorageDomainMember setWritePreference( final WritePreferenceLevel value );
    
    
    String AUTO_COMPACTION_THRESHOLD = "autoCompactionThreshold";
        
    @Optional
    Integer getAutoCompactionThreshold();
    
    StorageDomainMember setAutoCompactionThreshold( final Integer value );
}
