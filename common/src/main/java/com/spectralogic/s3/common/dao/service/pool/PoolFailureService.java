/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.FailureService;

public interface PoolFailureService extends FailureService< PoolFailure >
{
    void create( final UUID poolId, final PoolFailureType type, final Throwable t );
    
    
    void create( final UUID poolId, final PoolFailureType type, final String error );
    
    
    void deleteAll( final UUID poolId );
    
    
    void deleteAll( final UUID poolId, final PoolFailureType failureType );
    
    
    ActiveFailures startActiveFailures( final UUID poolId, final PoolFailureType type );
}
