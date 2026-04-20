/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.api;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public interface PoolLockSupport< L >
{
    /**
     * Acquires a non-exclusive, reentrant lock to read the specified blob from the specified pool.  <br><br>
     * 
     * The acquired lock must be released via a call to {@link #releaseBlobLocks}.
     * 
     * @throws PoolLockingException if the lock cannot be acquired
     */
    void acquireReadLock( final UUID poolId, final UUID blobId );
    
    
    /**
     * Acquires a non-exclusive, non-reentrant lock to read from the specified pool.  <br><br>
     * 
     * The acquired lock must be released via a call to {@link #releaseLock}.
     * 
     * @throws PoolLockingException if the lock cannot be acquired
     */
    void acquireReadLock( final UUID poolId, final L lockHolder );
    
    
    /**
     * Acquires an exclusive, non-reentrant lock to write to the specified pool.  Acquiring a write lock on a 
     * pool does not prevent read locks from being acquired on that pool.  It is exclusive of
     * delete locks on the pool. <br><br>
     * 
     * The acquired lock must be released via a call to {@link #releaseLock}.
     * 
     * @throws PoolLockingException if the lock cannot be acquired
     */
    void acquireWriteLock(final UUID poolId, final L lockHolder, final long bytesToWrite, long availableCapacityOnPool);
    
    
    /**
     * Acquires an non-exclusive, non-reentrant lock to delete from the specified pool.  Acquiring a delete lock on a
     * pool does not prevent read locks from being acquired on that pool.  It is exclusive of
     * write locks on the pool. <br><br>
     *
     * The acquired lock must be released via a call to {@link #releaseLock}.
     *
     * @throws PoolLockingException if the lock cannot be acquired
     */
    void acquireDeleteLock( final UUID poolId, final L lockHolder );
    
    
    void acquireDeleteLockWait( final UUID poolId, final L lockHolder );
    
    
    /**
     * @return the blobs that are holding read locks on pool(s)
     */
    Set< UUID > getBlobLockHolders();
    
    
    /**
     * @return Set <pool id> of all pools that cannot have {@link #acquireDeleteLock} called on them at this
     * time
     */
    Set< UUID > getPoolsUnavailableForDeleteLock();
    
    
    /**
     * @return Set <pool id> of all pools that cannot have {@link #acquireWriteLock} called on them at this
     * time
     */
    Set< UUID > getPoolsUnavailableForWriteLock();

    /**
     * @return Set <pool id> of all pools that cannot have {@link #acquireWriteLock} called on them  and have
     * bytesToWrite bytes available at this time
     */
    Set< UUID > getPoolsUnavailableForWriteLock( final long bytesToWrite );
    
    
    /**
     * @return Set <pool id> of all pools that cannot have {@link #acquireExclusiveLock} called on them at 
     * this time
     */
    Set< UUID > getPoolsUnavailableForExclusiveLock();
    
    
    /**
     * Acquires an exclusive, non-reentrant lock to the specified pool.  Acquiring an exclusive lock on a pool
     * prevents read and write locks from being acquired on that pool.  <br><br>
     * 
     * The acquired lock must be released via a call to {@link #releaseLock}.
     * 
     * @throws PoolLockingException if the lock cannot be acquired
     */
    void acquireExclusiveLock( final UUID poolId, final L lockHolder );
    
    
    /**
     * Releases blob read locks for the blobs specified.
     */
    void releaseBlobLocks( final Set< UUID > blobIds );
    
    
    /**
     * Releases all acquired locks held by the lock holder.
     * 
     * @return TRUE if one or more locks were released; false otherwise
     */
    boolean releaseLock( final L lockHolder );
    
    
    /**
     * Pools can enter {@link Quiesced#PENDING} at any time by the user's request; however, a pool cannot
     * be transisted to state {@link Quiesced#YES} until that pool is no longer locked (and thus, is no longer
     * in use).  Calling this method transists pools from {@link Quiesced#PENDING} to {@link Quiesced#YES} as
     * possible.
     */
    void fullyQuiesceUnlockedPoolsThatAreQuiescePending( final BeansServiceManager serviceManager );
}
