/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

public final class OfflineDurationTracker
{
    synchronized public void update( final UUID target, final boolean offline )
    {
        Validations.verifyNotNull( "Target", target );
        
        final boolean wasOffline = ( m_offlineTargets.containsKey( target ) );
        if ( wasOffline == offline )
        {
            return;
        }
        
        if ( offline )
        {
            m_offlineTargets.put( target, new Duration() );
        }
        else
        {
            m_offlineTargets.remove( target );
        }
    }
    
    
    synchronized public Duration getOfflineDuration( final UUID target )
    {
        Validations.verifyNotNull( "Target", target );
        return m_offlineTargets.get( target );
    }
    
    
    private final Map< UUID, Duration > m_offlineTargets = new HashMap<>();
}
