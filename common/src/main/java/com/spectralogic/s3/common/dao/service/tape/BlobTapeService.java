/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.service.shared.BlobLossRecorder;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface BlobTapeService extends BeansRetriever< BlobTape >, BeanDeleter, BlobLossRecorder< BlobTape >
{
    void create( final Set< BlobTape > blobTapes );
    
    
    void obsoleteBlobTapes(  final Set< BlobTape > blobTargets, final UUID obsoletion );
    
    
    int getNextOrderIndex( final UUID tapeId );
    
    
    void delete( final Set< UUID > ids );
    
    
    /**
     * Deletes all the blobs on the tape and releases it from any bucket assignment.
     */
    void reclaimTape( final String cause, final UUID tapeId );
    
    
    void reclaimForDeletedPersistenceRule( final UUID dataPolicyId, final UUID storageDomainId );
}
