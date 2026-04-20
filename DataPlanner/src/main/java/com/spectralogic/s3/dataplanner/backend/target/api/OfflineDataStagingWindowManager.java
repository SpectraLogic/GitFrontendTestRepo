/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.api;

import java.util.UUID;

import com.spectralogic.util.db.lang.DatabasePersistable;

public interface OfflineDataStagingWindowManager
{
    < T extends DatabasePersistable > 
    boolean tryLock( final Class< T > targetType, final UUID targetId, final UUID chunkId );
    
    
    void releaseLock( final UUID chunkId );
}
