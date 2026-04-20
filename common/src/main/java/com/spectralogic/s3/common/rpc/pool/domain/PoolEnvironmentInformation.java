/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.pool.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface PoolEnvironmentInformation extends SimpleBeanSafeToProxy
{
    String POOLS = "pools";
    
    @Optional
    PoolInformation [] getPools();
    
    void setPools( final PoolInformation [] value );
}
