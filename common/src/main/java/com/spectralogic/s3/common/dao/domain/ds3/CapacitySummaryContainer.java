/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface CapacitySummaryContainer extends SimpleBeanSafeToProxy
{
    String TAPE = "tape";
    
    StorageDomainCapacitySummary getTape();
    
    CapacitySummaryContainer setTape( final StorageDomainCapacitySummary value );
    
    
    String POOL = "pool";
    
    StorageDomainCapacitySummary getPool();
    
    CapacitySummaryContainer setPool( final StorageDomainCapacitySummary pool );
}
