/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.dataplanner.backend.target.api.OfflineDataStagingWindowManager;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.lang.CollectionFactory;

final class MockOfflineDataStagingWindowManager implements OfflineDataStagingWindowManager
{
    synchronized public < T extends DatabasePersistable > 
    boolean tryLock(
            final Class< T > targetType,
            final UUID targetId,
            final UUID chunkId )
    {
        m_triedLocks.add( chunkId );
        return m_tryLockReturnValue;
    }
    
    
    synchronized public void setTryLockReturnValue( final boolean value )
    {
        m_tryLockReturnValue = value;
    }

    
    synchronized public void releaseLock( final UUID chunkId )
    {
        m_releasedLocks.add( chunkId );
    }
    
    
    synchronized public void assertTryLockCalls( final UUID ... chunkIds )
    {
        final List< UUID > expected = CollectionFactory.toList( chunkIds );
        if ( !expected.equals( m_triedLocks ) )
        {
            throw new RuntimeException( "Expected " + expected + ", but was " + m_triedLocks + "." );
        }
        m_triedLocks.clear();
    }
    
    
    synchronized public void assertReleaseLockCalls( final UUID ... chunkIds )
    {
        final List< UUID > expected = CollectionFactory.toList( chunkIds );
        if ( !expected.equals( m_releasedLocks ) )
        {
            throw new RuntimeException( "Expected " + expected + ", but was " + m_releasedLocks + "." );
        }
        m_releasedLocks.clear();
    }
    
    
    private volatile boolean m_tryLockReturnValue = true;
    private final List< UUID > m_triedLocks = new ArrayList<>();
    private final List< UUID > m_releasedLocks = new ArrayList<>();
}
