/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.api;

import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;

/**
 * Manages the tape drive resources, including bringing up the RPC {@link TapeDriveResource} instances as 
 * necessary and managing the locks on said RPC resources.
 * 
 * @param <L> - The lock type
 */
public interface TapeLockSupport< L >
{
    /**
     * Updates the internal lock state to add locks for new tape drives that have come online and to remove
     * locks for tape drives that have gone offline.  <br><br>
     * 
     * It is recommended that this method is called just before locking any tape drives.
     * @param callback
     */
    void ensureAvailableTapeDrivesAreUpToDate(final Function<UUID, Boolean> callback);


    default void ensureAvailableTapeDrivesAreUpToDate() { ensureAvailableTapeDrivesAreUpToDate((id) -> true); }

    /**
     * @return Set <tape drive id>
     */
    Set< UUID > getAvailableTapeDrives();
    
    
    /**
     * Acquires a lock without locking a tape drive.  Driveless locks are unlimited in number.
     */
    void lockWithoutDrive( final L lockHolder );
    
    
    /**
     * Acquires a lock that includes a tape drive.  A tape drive can only be locked by one lock holder and a
     * single lock holder can only lock one drive.
     */
    TapeDriveResource lock( final UUID tapeDriveId, final L lockHolder );
    
    
    /**
     * Acquires a lock that includes a tape drive.  A tape drive can only be locked by one lock holder and a
     * single lock holder can only lock one drive.  <br><br>
     * 
     * This method will proceed to acquire a lock on the drive even if the drive is in a bad state.  The
     * {@link #lock} method should almost always be used instead of this one.
     */
    TapeDriveResource forceLock( final UUID tapeDriveId, final L lockHolder );
    
    
    /**
     * A tape lock can only be added to an existing lock by the lock holder.  The lock holder can only lock a
     * single tape.  The tape lock, if any, will be released automatically when the existing lock by the lock
     * holder is released.
     */
    void addTapeLock( final L lockHolder, final UUID tapeId );
    
    
    /**
     * Returns a set of zero or one tapes if the lock holder is not null, or a set of zero or more tapes if
     * the lock holder is null
     */
    Set< UUID > getLockedTapes( final L lockHolder );
    
    
    /**
     * Returns the lock holder for the given tape, or null if there is no lock holder for the tape
     */
    L getTapeLockHolder( final UUID tapeId );
    
    
    /**
     * Releases the lock held by the lock holder, along with any tape locks, if any
     */
    UUID unlock( final L lockHolder );
    
    
    /**
     * @return Set <lock holder that has not yet been unlocked> 
     */
    Set< L > getAllLockHolders();

    /**
     * Returns a set of tape ID's that were recently unlocked. We use this list to avoid selecting tapes for tasks
     * that require them to move if we haven't yet checked whether we can use them for something that doesn't require
     * any moves.
     */
    Set< UUID > getRecentlyUnlocked();

    /**
     *  Clears the recently unlocked list.
     */
    void clearRecentlyUnlocked();
}
