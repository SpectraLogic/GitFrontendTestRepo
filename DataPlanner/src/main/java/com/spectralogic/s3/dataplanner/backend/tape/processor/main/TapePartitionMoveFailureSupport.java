/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

public final class TapePartitionMoveFailureSupport
{
    TapePartitionMoveFailureSupport( final long suspensionInMillisUponMoveFailure )
    {
        setSuspensionInMillisUponMoveFailure( suspensionInMillisUponMoveFailure );
    }
    
    
    synchronized public void setSuspensionInMillisUponMoveFailure( 
            final long suspensionInMillisUponMoveFailure )
    {
        Validations.verifyInRange( "Suspension", 0, Long.MAX_VALUE, suspensionInMillisUponMoveFailure );
        m_suspensionInMillisUponMoveFailure = suspensionInMillisUponMoveFailure;
    }
    
    
    synchronized void moveFailureOccurred( final UUID partitionId )
    {
        Validations.verifyNotNull( "Partition id", partitionId );
        m_durationsSinceLastMoveFailure.put( partitionId, new Duration() );
    }
    
    
    synchronized void clearMoveFailure( final UUID partitionId )
    {
        Validations.verifyNotNull( "Partition id", partitionId );
        m_durationsSinceLastMoveFailure.remove( partitionId );
    }
    
    
    synchronized boolean isTaskExecutionSuspended( final UUID partitionId )
    {
        Validations.verifyNotNull( "Partition id", partitionId );
        final Duration duration = m_durationsSinceLastMoveFailure.get( partitionId );
        return ( null != duration && duration.getElapsedMillis() < m_suspensionInMillisUponMoveFailure );
    }
    
    
    private long m_suspensionInMillisUponMoveFailure;
    private final Map< UUID, Duration > m_durationsSinceLastMoveFailure = new HashMap<>();
}
