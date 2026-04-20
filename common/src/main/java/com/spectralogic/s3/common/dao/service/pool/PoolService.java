/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface PoolService
    extends BeansRetriever< Pool >, BeanCreator< Pool >, BeanUpdater< Pool >, BeanDeleter
{
    long [] getAvailableSpacesForBucket( final UUID bucketId, final UUID storageDomainId );
    
    
    void updateDates( final UUID poolId, final PoolAccessType accessType );
    
    
    void updateAssignment( final UUID poolId );
    
    
    public enum PoolAccessType
    {
        ACCESSED,
        MODIFIED,
        VERIFIED
    }
}
