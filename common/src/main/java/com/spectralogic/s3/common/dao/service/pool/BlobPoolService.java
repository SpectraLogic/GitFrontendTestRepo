/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.service.shared.BlobLossRecorder;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface BlobPoolService extends BeansRetriever< BlobPool >, BeanDeleter, BlobLossRecorder< BlobPool >, BeanUpdater< BlobPool >
{
    void create( final Set< BlobPool > blobPools );
    
    
    void obsoleteBlobPools(  final Set< BlobPool > blobTargets, final UUID obsoletion );


    void registerFailureToRead( final DiskFileInfo diskFileInfo);
    
    
    void delete( final Set< UUID > ids );
    
    
    void reclaimForTemporaryPersistenceRule( 
            final UUID poolId, 
            final UUID bucketId,
            final int minDaysOldToReclaim,
            final String beanPropertyToCheckForMinDaysOld );
    
    
    void updateLastAccessed( final Set< UUID > blobIds );
    
    
    void reclaimForDeletedPersistenceRule( final UUID dataPolicyId, final UUID storageDomainId );
}
