/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.service.shared.BlobLossRecorder;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface BlobTargetService
    < T extends DatabasePersistable & BlobTarget< T >, R extends DataReplicationRule< R > >
    extends BeansRetriever< T >, BeanCreator< T >, BlobLossRecorder< T >
{
    /**
     * @param error - if null, this means blobs were lost under normal operation
     * @param targetId - the replication target that has lost the blobs
     * @param blobIds - the blobs that have been lost
     */
    void blobsLost(
            final String error, 
            final UUID targetId, 
            final Set< UUID > blobIds );
    
    
    void reclaimForDeletedReplicationRule( 
            final UUID dataPolicyId, 
            final R rule );
}
