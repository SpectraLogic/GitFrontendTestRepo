/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.util.lang.CollectionFactory;

public final class MockTapeAvailability implements TapeAvailability
{
    public MockTapeAvailability()
    {
        setTapePartitionId( UUID.randomUUID() );
        setDriveId( UUID.randomUUID() );
    }
    
    
    public MockTapeAvailability( final UUID tapeParitionId, final UUID driveId )
    {
        setTapePartitionId( tapeParitionId );
        setDriveId( driveId );
    }
    
    
    public MockTapeAvailability setDriveId( final UUID driveId )
    {
        m_driveId = driveId;
        return this;
    }
    
    
    public MockTapeAvailability setTapePartitionId( final UUID partitionId )
    {
        m_partitionId = partitionId;
        return this;
    }
    
    
    public MockTapeAvailability setPreferredTape( final Tape preferredTape )
    {
        m_preferredTape = preferredTape.getId();
        m_partitionId = preferredTape.getPartitionId();
        return this;
    }
    
    
    public MockTapeAvailability setPreferredTape( final UUID preferredTape )
    {
        m_preferredTape = preferredTape;
        return this;
    }
    
    
    public MockTapeAvailability addUnavailableTape( final UUID unavailableTape )
    {
        m_unavailableTapes.add( unavailableTape );
        return this;
    }
    
    
    public MockTapeAvailability addUnavailableTapes( final UUID ... unavailableTapes )
    {
        m_unavailableTapes.addAll( CollectionFactory.toSet( unavailableTapes ) );
        return this;
    }
    
    
    public MockTapeAvailability setVerifyAvailableReturnsError( final boolean value )
    {
        m_verifyAvailableReturnsError = value;
        return this;
    }
    
    
    public MockTapeAvailability setVerifyAvailableException( final RuntimeException ex )
    {
        m_verifyAvailableException = ex;
        return this;
    }


    public UUID getTapeInDrive()
    {
        return m_preferredTape;
    }


    public Set< UUID > getAvailableTapes()
    {
        return new HashSet<>();
    }


    public Set< UUID > getTemporarilyUnavailableTapes()
    {
        return new HashSet<>();
    }


    public Set< UUID > getPermanentlyUnavailableTapes()
    {
        return new HashSet<>( m_unavailableTapes );
    }


    public Set< UUID > getAllUnavailableTapes()
    {
        return new HashSet<>( m_unavailableTapes );
    }


    public String verifyAvailable( final UUID tapeId )
    {
        if ( null != m_verifyAvailableException )
        {
            throw m_verifyAvailableException;
        }
        if ( m_verifyAvailableReturnsError )
        {
            return "I said so";
        }
        return null;
    }

    @Override
    public String getSummary() {
        return "<<mock availability summary>>";
    }


    public UUID getDriveId()
    {
        return m_driveId;
    }
    
    
    public UUID getTapePartitionId()
    {
        return m_partitionId;
    }
    

    private volatile UUID m_partitionId;
    private volatile UUID m_driveId;
    private volatile UUID m_preferredTape;
    private volatile boolean m_verifyAvailableReturnsError;
    private volatile RuntimeException m_verifyAvailableException;
    private final Set< UUID > m_unavailableTapes = new CopyOnWriteArraySet<>();
}
