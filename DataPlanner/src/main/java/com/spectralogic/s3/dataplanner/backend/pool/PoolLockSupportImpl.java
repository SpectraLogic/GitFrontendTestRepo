/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.service.pool.PoolFailureService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

public final class PoolLockSupportImpl< L > implements PoolLockSupport< L >
{
    public PoolLockSupportImpl(
            final BlobPoolLastAccessedUpdater blobPoolLastAccessedUpdater,
            final PoolPowerManager powerManager,
            final PoolQuiescedManager quiescedManager )
    {
        m_blobPoolLastAccessedUpdater = blobPoolLastAccessedUpdater;
        m_powerManager = powerManager;
        m_quiescedManager = quiescedManager;
        Validations.verifyNotNull( "Blob last accessed updater", m_blobPoolLastAccessedUpdater );
        Validations.verifyNotNull( "Power manager", m_powerManager );
        Validations.verifyNotNull( "Quiesced manager", m_quiescedManager );
    }


    public void acquireReadLock( final UUID poolId, final UUID blobId )
    {
        Validations.verifyNotNull( "Pool", poolId );
        Validations.verifyNotNull( "Blob", blobId );

        try
        {
            acquireReadLockInternal( poolId, blobId );
        }
        catch ( final RuntimeException ex )
        {
            throw new PoolLockingException(
                    "Failed to acquire read lock on pool " + poolId + " for blob " + blobId + ".", ex );
        }
    }


    synchronized private void acquireReadLockInternal( final UUID poolId, final UUID blobId )
    {
        m_blobPoolLastAccessedUpdater.accessed( blobId );

        verifyNotQuiesced( poolId );
        verifyNoExclusiveLockAcquired( poolId );

        m_powerManager.powerOn( poolId );

        m_readBlobLocks.computeIfAbsent( poolId, k -> new HashSet<>() )
                       .add( blobId );
        m_readPoolsForBlobs.computeIfAbsent( blobId, k -> new HashSet<>() )
                           .add( poolId );
        logLockOperation( "read", "acquired", poolId, "blob " + blobId );
    }


    public void acquireReadLock( final UUID poolId, final L lockHolder )
    {
        Validations.verifyNotNull( "Pool", poolId );
        Validations.verifyNotNull( "Lock holder", lockHolder );

        try
        {
            acquireReadLockInternal( poolId, lockHolder );
        }
        catch ( final RuntimeException ex )
        {
            throw new PoolLockingException(
                    "Failed to acquire read lock on pool " + poolId + " for " + lockHolder + ".", ex );
        }
    }


    synchronized private void acquireReadLockInternal( final UUID poolId, final L lockHolder )
    {
        verifyNotQuiesced( poolId );

        final Set< L > readLockHolders = m_readLocks.get( poolId );
        if ( null != readLockHolders && readLockHolders.contains( lockHolder ) )
        {
            throw new IllegalStateException( "Pool already locked by lock-holder." );
        }
        verifyNoExclusiveLockAcquired( poolId );

        m_powerManager.powerOn( poolId );
        m_readLocks.computeIfAbsent( poolId, k -> new HashSet<>() )
                   .add( lockHolder );
        logLockOperation( "read", "acquired", poolId, lockHolder );
    }


    public void acquireDeleteLock( final UUID poolId, final L lockHolder )
    {
        Validations.verifyNotNull( "Pool", poolId );
        Validations.verifyNotNull( "Lock holder", lockHolder );

        try
        {
            acquireDeleteLockInternal( poolId, lockHolder );
        }
        catch ( final RuntimeException ex )
        {
            throw new PoolLockingException(
                    "Failed to acquire delete lock on pool " + poolId + " for " + lockHolder + ".", ex );
        }
    }


    synchronized private void acquireDeleteLockInternal( final UUID poolId, final L lockHolder )
    {
        verifyNotQuiesced( poolId );
        verifyNoWriteLockAcquired( poolId );

        final Set< L > deleteLockHolders = m_deleteLocks.get( poolId );
        if ( null != deleteLockHolders && deleteLockHolders.contains( lockHolder ) )
        {
            throw new IllegalStateException( "Pool already locked by lock-holder." );
        }
        verifyNoExclusiveLockAcquired( poolId );

        m_powerManager.powerOn( poolId );
        m_deleteLocks.computeIfAbsent( poolId, k -> new HashSet<>() )
                     .add( lockHolder );
        logLockOperation( "delete", "acquired", poolId, lockHolder );
    }


    public void acquireDeleteLockWait( final UUID poolId, final L lockHolder )
    {
        Validations.verifyNotNull( "Pool", poolId );
        Validations.verifyNotNull( "Lock holder", lockHolder );

        boolean haveLock = false;
        synchronized ( this )
        {
            while ( !haveLock )
            {
                try
                {
                    acquireDeleteLockInternal( poolId, lockHolder );
                    haveLock = true;
                }
                catch ( final IllegalStateException ex )
                {
                    LOG.info( ex.getMessage() );
                    try
                    {
                        this.wait( TimeUnit.MINUTES.toMillis( 15 ) );
                    }
                    catch ( final InterruptedException interruptedEx )
                    {
                        throw new RuntimeException(
                                "Failed to acquire delete lock on pool " + poolId + " for " + lockHolder + ".",
                                interruptedEx );
                    }
                }
            }
        }
    }


    public void acquireWriteLock(final UUID poolId, final L lockHolder, final long bytesToWrite, long availableCapacityOnPool)
    {
        Validations.verifyNotNull( "Pool", poolId );
        Validations.verifyNotNull( "Lock holder", lockHolder );

        try
        {
            acquireWriteLockInternal( poolId, lockHolder, bytesToWrite, availableCapacityOnPool );
        }
        catch ( final RuntimeException ex )
        {
            throw new PoolLockingException(
                    "Failed to acquire write lock on pool " + poolId + " for " + lockHolder + ".", ex );
        }
    }


    synchronized private void acquireWriteLockInternal(final UUID poolId, final L lockHolder, final long bytesToWrite, long availableCapacityOnPool)
    {
        verifyNotQuiesced( poolId );
        verifyNoDeleteLocksAcquired( poolId );
        verifyNoExclusiveLockAcquired( poolId );

        final long available = m_bytesAvailablePerPool.getOrDefault(poolId, availableCapacityOnPool);
        if (bytesToWrite > available) {
            throw new IllegalStateException(
                    "Cannot lock pool " + poolId + " for writing " + bytesToWrite + " bytes since it has "
                            + availableCapacityOnPool + " bytes available. (" + available
                            + " including pending work).");
        }

        m_powerManager.powerOn( poolId );
        m_writeLocks.computeIfAbsent( poolId, k -> new HashSet<>() ).add( lockHolder );
        m_bytesAvailablePerPool.compute( poolId, (k, v) -> v == null ? availableCapacityOnPool - bytesToWrite : v - bytesToWrite );
        logLockOperation( "write", "acquired", poolId, lockHolder );
    }


    synchronized public Set< UUID > getBlobLockHolders()
    {
        return new HashSet<>( m_readPoolsForBlobs.keySet() );
    }


    synchronized public Set< UUID > getPoolsUnavailableForDeleteLock()
    {
        final Set< UUID > retval = new HashSet<>();
        retval.addAll( m_writeLocks.keySet() );
        retval.addAll( m_exclusiveLocks.keySet() );

        return retval;
    }

    synchronized public Set< UUID > getPoolsUnavailableForWriteLock() {
        return getPoolsUnavailableForWriteLock(0);
    }

    synchronized public Set< UUID > getPoolsUnavailableForWriteLock( final long bytesToWrite )
    {
        final Set< UUID > retval = new HashSet<>();
        retval.addAll( m_deleteLocks.keySet() );
        retval.addAll( m_exclusiveLocks.keySet() );
        for (Map.Entry<UUID, Long> e : m_bytesAvailablePerPool.entrySet()) {
            if (bytesToWrite > e.getValue()) {
                retval.add(e.getKey());
            }
        }
        return retval;
    }

    synchronized public Set< UUID > getPoolsUnavailableForExclusiveLock()
    {
        final Set< UUID > retval = new HashSet<>();
        retval.addAll( m_deleteLocks.keySet() );
        retval.addAll( m_exclusiveLocks.keySet() );
        retval.addAll( m_writeLocks.keySet() );
        retval.addAll( m_readBlobLocks.keySet() );
        retval.addAll( m_readLocks.keySet() );

        return retval;
    }


    public void acquireExclusiveLock( final UUID poolId, final L lockHolder )
    {
        Validations.verifyNotNull( "Pool", poolId );
        Validations.verifyNotNull( "Lock holder", lockHolder );

        try
        {
            acquireExclusiveLockInternal( poolId, lockHolder );
        }
        catch ( final RuntimeException ex )
        {
            throw new PoolLockingException(
                    "Failed to acquire exclusive lock on pool " + poolId + " for " + lockHolder + ".", ex );
        }
    }


    synchronized private void acquireExclusiveLockInternal( final UUID poolId, final L lockHolder )
    {
        verifyNotQuiesced( poolId );
        verifyNoReadLocksAcquired( poolId );
        verifyNoDeleteLocksAcquired( poolId );
        verifyNoWriteLockAcquired( poolId );
        verifyNoExclusiveLockAcquired( poolId );

        m_powerManager.powerOn( poolId );
        m_exclusiveLocks.put( poolId, lockHolder );
        logLockOperation( "exclusive", "acquired", poolId, lockHolder );
    }


    private void verifyNoDeleteLocksAcquired( final UUID poolId )
    {
        final Set< L > lockHolders = m_deleteLocks.get( poolId );
        if ( null != lockHolders )
        {
            throw new IllegalStateException(
                    "Cannot lock pool " + poolId + " since read locks acquired by " + lockHolders + "." );
        }
    }


    private void verifyNoReadLocksAcquired( final UUID poolId )
    {
        final Set< L > lockHolders = m_readLocks.get( poolId );
        if ( null != lockHolders )
        {
            throw new IllegalStateException(
                    "Cannot lock pool " + poolId + " since read locks acquired by " + lockHolders + "." );
        }

        final Set< UUID > blobLockHolders = m_readBlobLocks.get( poolId );
        if ( null != blobLockHolders )
        {
            throw new IllegalStateException(
                    "Cannot lock pool " + poolId + " since " + blobLockHolders.size()
                    + " read lock(s) acquired for blob reads." );
        }
    }


    private void verifyNoWriteLockAcquired( final UUID poolId )
    {
        final Set< L > lockHolders = m_writeLocks.get( poolId );
        if ( null != lockHolders )
        {
            throw new IllegalStateException(
                    "Cannot lock pool " + poolId + " since write lock acquired by " + lockHolders + "." );
        }
    }


    private void verifyNoExclusiveLockAcquired( final UUID poolId )
    {
        final L lockHolder = m_exclusiveLocks.get( poolId );
        if ( null != lockHolder )
        {
            throw new IllegalStateException(
                    "Cannot lock pool " + poolId + " since exclusive lock acquired by " + lockHolder + "." );
        }
    }


    synchronized public void releaseBlobLocks( final Set< UUID > blobIds )
    {
        Validations.verifyNotNull( "Blob ids", blobIds );
        for ( final Map.Entry< UUID, Set< UUID > > e : new HashSet<>( m_readBlobLocks.entrySet() ) )
        {
            for ( UUID blobId : new HashSet<>( e.getValue() ) )
            {
                if ( !blobIds.contains( blobId ) )
                {
                    continue;
                }

                e.getValue().remove( blobId );
                m_readPoolsForBlobs.get( blobId ).remove( e.getKey() );
                if ( m_readPoolsForBlobs.get( blobId ).isEmpty() )
                {
                    m_readPoolsForBlobs.remove( blobId );
                }
                lockReleased( "read", e.getKey(), "blob " + blobId );
            }
            if ( e.getValue().isEmpty() )
            {
                m_readBlobLocks.remove( e.getKey() );
            }
        }
    }


    public boolean releaseLock( final L lockHolder )
    {
        Validations.verifyNotNull( "Lock holder", lockHolder );
        boolean lockHeld = false;
        synchronized ( this )
        {
            for ( final Map.Entry< UUID, Set< L > > e : new HashSet<>( m_deleteLocks.entrySet() ) )
            {
                if ( e.getValue()
                      .contains( lockHolder ) )
                {
                    lockHeld = true;
                    e.getValue()
                     .remove( lockHolder );
                    if ( e.getValue()
                          .isEmpty() )
                    {
                        m_deleteLocks.remove( e.getKey() );
                    }
                    lockReleased( "delete", e.getKey(), lockHolder );
                }
            }
            for ( final Map.Entry< UUID, Set< L > > e : new HashSet<>( m_readLocks.entrySet() ) )
            {
                if ( e.getValue()
                      .contains( lockHolder ) )
                {
                    lockHeld = true;
                    e.getValue()
                     .remove( lockHolder );
                    if ( e.getValue()
                          .isEmpty() )
                    {
                        m_readLocks.remove( e.getKey() );
                    }
                    lockReleased( "read", e.getKey(), lockHolder );
                }
            }
            for ( final Map.Entry< UUID, Set< L > > e : new HashSet<>( m_writeLocks.entrySet() ) )
            {
                if ( e.getValue().contains( lockHolder ) )
                {
                    lockHeld = true;
                    final Pair<UUID, L> keyForPendingBytes = Pair.of(e.getKey(), lockHolder);
                    e.getValue().remove( lockHolder );
                    if ( e.getValue().isEmpty() )
                    {
                        m_writeLocks.remove( e.getKey() );
                        //Since there are no locks left we will remove our record of bytes available and populate it
                        //with a fresh value from the database next time a write lock is acquired.
                        m_bytesAvailablePerPool.remove( e.getKey() );
                    }
                    lockReleased( "write", e.getKey(), lockHolder );
                }
            }
            if ( m_exclusiveLocks.containsValue( lockHolder ) )
            {
                lockHeld = true;
                releaseLockInternal( "exclusive", m_exclusiveLocks, lockHolder );
            }

            if ( lockHeld )
            {
                this.notifyAll();
            }
        }
        return lockHeld;
    }


    private void releaseLockInternal( final String lockType, final Map< UUID, L > map, final L lockHolder )
    {
        for ( final Map.Entry< UUID, L > e : new HashSet<>( map.entrySet() ) )
        {
            if ( e.getValue() == lockHolder )
            {
                map.remove( e.getKey() );
                lockReleased( lockType, e.getKey(), lockHolder );
            }
        }
    }


    private void lockReleased( final String lockType, final UUID poolId, final Object lockHolder )
    {
        try
        {
            logLockOperation( lockType, "released", poolId, lockHolder );
            if ( isLocked( poolId ) )
            {
                final List< String > lockHolders = new ArrayList<>();
                if ( m_exclusiveLocks.containsKey( poolId ) )
                {
                    lockHolders.add( "Exclusive lock held by: " + m_exclusiveLocks.get( poolId ) );
                }
                if ( m_writeLocks.containsKey( poolId ) )
                {
                    lockHolders.add( "Write lock held by: " + m_writeLocks.get( poolId ) );
                }
                if ( m_readLocks.containsKey( poolId ) )
                {
                    lockHolders.add( "Read lock held by: " + m_readLocks.get( poolId ) );
                }
                if ( m_deleteLocks.containsKey( poolId ) )
                {
                    lockHolders.add( "Delete lock held by: " + m_deleteLocks.get( poolId ) );
                }
                if ( m_readBlobLocks.containsKey( poolId ) && !m_readBlobLocks.get( poolId ).isEmpty() )
                {
                    lockHolders.add( "Read blob locks held: " + m_readBlobLocks.get( poolId ).size() );
                }
                LOG.info( "Pool " + poolId + " still has lock holders: " + lockHolders );
            }
            else
            {
                m_powerManager.powerOff( poolId );
            }
        }
        catch ( final RuntimeException ex )
        {
            throw new PoolLockingException(
                    "Failed to power off pool " + poolId + ".", ex );
        }
    }


    private void logLockOperation(
            final String lockType,
            final String lockAction,
            final UUID poolId,
            final Object lockHolder )
    {
        LOG.info( lockType.substring( 0, 1 ).toUpperCase() + lockType.substring( 1 )
                + " lock " + lockAction + " on pool " + poolId + " by " + lockHolder + "." );
    }


    private boolean isLocked( final UUID poolId )
    {
        return ( ( m_readBlobLocks.containsKey( poolId ) && !m_readBlobLocks.get( poolId ).isEmpty() )
                || ( m_readLocks.containsKey( poolId ) && !m_readLocks.get( poolId ).isEmpty() )
                || ( m_deleteLocks.containsKey( poolId ) && !m_deleteLocks.get( poolId ).isEmpty() )
                || ( m_writeLocks.containsKey( poolId ) && !m_writeLocks.get( poolId ).isEmpty() )
                || m_exclusiveLocks.containsKey( poolId ) );
    }


    private void verifyNotQuiesced( final UUID poolId )
    {
        m_quiescedManager.verifyNotQuiesced( poolId );
    }


    synchronized public void fullyQuiesceUnlockedPoolsThatAreQuiescePending(
            final BeansServiceManager serviceManager )
    {
        final Set< UUID > lockedPools = getPoolsUnavailableForExclusiveLock();
        final Map< UUID, Pool > pools =
                BeanUtils.toMap( serviceManager.getRetriever( Pool.class ).retrieveAll().toSet() );

        final BeansServiceManager transaction = serviceManager.startTransaction();
        try
        {
            for ( final Map.Entry<UUID, Pool> poolEntry : pools.entrySet() )
            {
                if ( Quiesced.PENDING == poolEntry.getValue().getQuiesced()
                        && !lockedPools.contains( poolEntry.getKey() ) )
                {
                    transaction.getService( PoolService.class ).update(
                            poolEntry.getValue().setQuiesced( Quiesced.YES ),
                            Pool.QUIESCED );
                }
            }

            for ( final UUID poolId : pools.keySet() )
            {
                final ActiveFailures failures =
                        transaction.getService( PoolFailureService.class ).startActiveFailures(
                                poolId, PoolFailureType.QUIESCED );
                final String failure = m_quiescedManager.getQuiescedCause( poolId );
                if ( null != failure )
                {
                    transaction.getService( PoolFailureService.class ).create(
                            poolId, PoolFailureType.QUIESCED, failure );
                }
                failures.commit();
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    /** Map< blob id, Set< pool id > > */
    private final Map< UUID, Set< UUID > > m_readPoolsForBlobs = new HashMap<>();
    /** Map< pool id, Set< blob id > > */
    private final Map< UUID, Set< UUID > > m_readBlobLocks = new HashMap<>();
    private final Map< UUID, Set< L > > m_readLocks = new HashMap<>();
    private final Map< UUID, Set< L > > m_writeLocks = new HashMap<>();
    private final Map<UUID, Long > m_bytesAvailablePerPool = new HashMap<>();
    private final Map< UUID, Set< L > > m_deleteLocks = new HashMap<>();
    private final Map< UUID, L > m_exclusiveLocks = new HashMap<>();
    private final PoolPowerManager m_powerManager;
    private final PoolQuiescedManager m_quiescedManager;
    private final BlobPoolLastAccessedUpdater m_blobPoolLastAccessedUpdater;

    private final static Logger LOG = Logger.getLogger( PoolLockSupportImpl.class );
}
