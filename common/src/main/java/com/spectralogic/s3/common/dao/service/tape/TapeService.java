/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface TapeService
    extends BeansRetriever< Tape >, BeanCreator< Tape >, BeanUpdater< Tape >, BeanDeleter
{
    void deassociateFromPartition( final Set< UUID > tapeIdsToDeassociateFromTheirPartition );
    
    
    void transistState( final Tape tape, final TapeState newState );
    
    
    void updatePreviousState( final Tape tape, final TapeState newPreviousState );
    
    
    void rollbackLastStateTransition( final Tape tape );
    
    
    long [] getAvailableSpacesForBucket( final UUID bucketId, final UUID storageDomainId );
    
    
    void updateDates( final UUID tapeId, final TapeAccessType accessType );
    
    
    void updateAssignment( final UUID tapeId );
    
    
    public enum TapeAccessType
    {
        ACCESSED,
        MODIFIED,
        VERIFIED
    }
}
