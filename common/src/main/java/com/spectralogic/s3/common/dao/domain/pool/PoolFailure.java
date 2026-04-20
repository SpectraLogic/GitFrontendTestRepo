/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes( @Index( PoolFailure.DATE ) )
public interface PoolFailure extends DatabasePersistable, Failure< PoolFailure, PoolFailureType >
{
    String POOL_ID = "poolId";
    
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Pool.class )
    UUID getPoolId();
    
    PoolFailure setPoolId( final UUID value );
    
    
    PoolFailureType getType();
    
    PoolFailure setType( final PoolFailureType value );
}
