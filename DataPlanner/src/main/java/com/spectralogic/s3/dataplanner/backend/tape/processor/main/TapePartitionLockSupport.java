/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.util.lang.Validations;

public final class TapePartitionLockSupport
{
    void lock( final TapePartitionResource resource )
    {
        final ReentrantLock lock = getLock( resource );
        lock.lock();
    }
    
    
    boolean isLockedByCurrentThread( final TapePartitionResource resource )
    {
        final ReentrantLock lock = getLock( resource );
        return lock.isHeldByCurrentThread();
    }
    
    
    int getNumberOfThreadsWaitingForLock( final TapePartitionResource resource )
    {
        final ReentrantLock lock = getLock( resource );
        return lock.getQueueLength();
    }
    
    
    /**
     * @return TRUE if the lock was acquired, FALSE otherwise
     */
    boolean tryLock( final TapePartitionResource resource )
    {
        final ReentrantLock lock = getLock( resource );
        return lock.tryLock();
    }
    
    
    private ReentrantLock getLock( final TapePartitionResource resource )
    {
        Validations.verifyNotNull( "Resource", resource );
        synchronized ( m_locks )
        {
            if ( !m_locks.containsKey( resource ) )
            {
                m_locks.put( resource, new ReentrantLock( true ) );
            }
            return m_locks.get( resource );
        }
    }
    
    
    void unlock( final TapePartitionResource resource )
    {
        Validations.verifyNotNull( "Resource", resource );
        final ReentrantLock lock;
        synchronized ( m_locks )
        {
            lock = m_locks.get( resource );
        }
        lock.unlock();
    }
    
    
    private final Map< Object, ReentrantLock > m_locks = new WeakHashMap<>();
}
