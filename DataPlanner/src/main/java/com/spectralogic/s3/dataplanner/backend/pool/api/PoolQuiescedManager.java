/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.api;

import java.util.UUID;

public interface PoolQuiescedManager
{
    void verifyNotQuiesced( final UUID poolId );
    
    
    boolean isQuiesced( final UUID poolId );
    
    
    String getQuiescedCause( final UUID poolId );
}
