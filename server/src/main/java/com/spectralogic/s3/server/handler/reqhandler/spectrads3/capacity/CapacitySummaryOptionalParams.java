/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.capacity;

import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface CapacitySummaryOptionalParams extends SimpleBeanSafeToProxy
{
    String TAPE_TYPE = "tapeType";
    
    @Optional
    TapeType getTapeType();
    
    void setTapeType( final TapeType value );
    
    
    String POOL_TYPE = "poolType";
    
    @Optional
    PoolType getPoolType();
    
    void setPoolType( final PoolType value );
    
    
    String TAPE_STATE = "tapeState";

    @Optional
    TapeState getTapeState();
    
    void setTapeState( final TapeState value );
    
    
    String POOL_STATE = "poolState";

    @Optional
    PoolState getPoolState();
    
    void setPoolState( final PoolState value );
    
    
    String POOL_HEALTH = "poolHealth";

    @Optional
    PoolHealth getPoolHealth();
    
    void setPoolHealth( final PoolHealth value );
}