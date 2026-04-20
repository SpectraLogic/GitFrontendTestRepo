/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;

public final class MockDs3TargetBlobPhysicalPlacement implements Ds3TargetBlobPhysicalPlacement
{
    synchronized public Set< UUID > getCandidateTargets()
    {
        return new HashSet<>( m_state.keySet() );
    }

    
    synchronized public TargetReadPreferenceType getReadPreference( final UUID targetId )
    {
        final Ds3TargetState state = m_state.get( targetId );
        return ( null == state ) ? null : state.m_readPreference;
    }

    
    synchronized public Set< UUID > getBlobsOnTape( final UUID targetId )
    {
        final Ds3TargetState state = m_state.get( targetId );
        return ( null == state ) ? new HashSet< UUID >() : new HashSet<>( state.m_blobsOnTape );
    }

    
    synchronized public Set< UUID > getBlobsOnPool( final UUID targetId )
    {
        final Ds3TargetState state = m_state.get( targetId );
        return ( null == state ) ? new HashSet< UUID >() : new HashSet<>( state.m_blobsOnPool );
    }
    
    
    synchronized public void add( final UUID targetId, final Ds3TargetState state )
    {
        m_state.put( targetId, state );
    }
    
    
    synchronized public void remove( final UUID targetId )
    {
        m_state.remove( targetId );
    }
    
    
    public final static class Ds3TargetStateBuilder
    {
        public Ds3TargetStateBuilder withReadPreference( final TargetReadPreferenceType readPreference )
        {
            m_readPreference = readPreference;
            return this;
        }
        
        
        public Ds3TargetStateBuilder withBlobsOnTape( final UUID ... blobIds )
        {
            for ( final UUID blobId : blobIds )
            {
                m_blobsOnTape.add( blobId );
            }
            return this;
        }
        
        
        public Ds3TargetStateBuilder withBlobsOnPool( final UUID ... blobIds )
        {
            for ( final UUID blobId : blobIds )
            {
                m_blobsOnPool.add( blobId );
            }
            return this;
        }
        
        
        public Ds3TargetState build()
        {
            return new Ds3TargetState( m_readPreference, m_blobsOnTape, m_blobsOnPool );
        }
        

        private volatile TargetReadPreferenceType m_readPreference = TargetReadPreferenceType.LAST_RESORT;
        private final Set< UUID > m_blobsOnTape = new HashSet<>();
        private final Set< UUID > m_blobsOnPool = new HashSet<>();
    } // end inner class def
    
    
    public final static class Ds3TargetState
    {
        public Ds3TargetState( 
                final TargetReadPreferenceType readPreference, 
                final Set< UUID > blobsOnTape, 
                final Set< UUID > blobsOnPool )
        {
            m_readPreference = readPreference;
            m_blobsOnTape = blobsOnTape;
            m_blobsOnPool = blobsOnPool;
        }
        
        
        private final TargetReadPreferenceType m_readPreference;
        private final Set< UUID > m_blobsOnTape; 
        private final Set< UUID > m_blobsOnPool;
    } // end inner class def

    
    private final Map< UUID, Ds3TargetState > m_state = new HashMap<>();
}
