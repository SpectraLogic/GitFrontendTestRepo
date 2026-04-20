/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveState;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.service.ds3.DataPathBackendService;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;

public final class TapeLockSupportImpl< L > implements TapeLockSupport< L >
{
    public TapeLockSupportImpl( 
            final RpcClient rpcClient, 
            final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        
        m_rpcClient = rpcClient;
        m_tapeDriveService = serviceManager.getService( TapeDriveService.class );
        m_tapePartitionService = serviceManager.getService( TapePartitionService.class );
        m_failureService = serviceManager.getService( TapePartitionFailureService.class );
        m_dataPathBackendService = serviceManager.getService( DataPathBackendService.class );
        
        Validations.verifyNotNull( "RPC client", m_rpcClient );
        Validations.verifyNotNull( "Tape drive retriever", m_tapeDriveService );
    }
    
    
    synchronized public void ensureAvailableTapeDrivesAreUpToDate(final Function<UUID, Boolean> moveTapeOutOfIdleDrive)
    {
        final Map< UUID, TapePartition > partitions = 
                BeanUtils.toMap( m_tapePartitionService.retrieveAll().toSet() );
        final Set< UUID > partitionsWithoutUsableDrives = new HashSet<>( partitions.keySet() );  
        final Map< UUID, TapeDrive > assignableDrives = new HashMap<>();
        final boolean backendActivated = m_dataPathBackendService.isActivated();
        for ( final TapeDrive drive : m_tapeDriveService.retrieveAll().toSet() )
        {
            final TapePartition partition = partitions.get( drive.getPartitionId() );
            final boolean assignable = isAssignable( partition, drive, backendActivated );
            if ( !m_locks.containsKey( drive.getId() ) )
            { 
                registerNewTapeDrive( drive, () -> moveTapeOutOfIdleDrive.apply(drive.getId()));
            }
            if ( assignable )
            {
                if ( !m_assignableDrives.containsKey( drive.getId() ) )
                {
                    tapeDriveBecameAssignable( drive );
                }
                partitionsWithoutUsableDrives.remove( drive.getPartitionId() );
                assignableDrives.put( drive.getId(), drive );
            }
        }
        
        for ( final UUID knownTapeDriveId : new HashSet<>( m_assignableDrives.keySet() ) )
        {
            if ( !assignableDrives.containsKey( knownTapeDriveId ) )
            {
                tapeDriveNoLongerAssignable( knownTapeDriveId );
            }
        }
        
        for (final Map.Entry<UUID, TapePartition> partitionEntry : partitions.entrySet() )
        {
            final TapePartition partition = partitionEntry.getValue();
            final ActiveFailures failures = m_failureService.startActiveFailures(
                    partition.getId(), TapePartitionFailureType.NO_USABLE_DRIVES );
            if ( !backendActivated )
            {
                failures.add( "Backend isn't activated." );
            }
            else if ( TapePartitionState.ONLINE != partition.getState() )
            {
                failures.add( "Partition is in state " + partition.getState() + "." );
            }
            else if ( Quiesced.NO != partition.getQuiesced() )
            {
                failures.add( "Partition is in quiesced state " + partition.getQuiesced() + "." );
            }
            else if ( partitionsWithoutUsableDrives.contains( partitionEntry.getKey() ) )
            {
                failures.add( "No " + TapeDriveState.NORMAL + " drives in partition." );
            }
            failures.commit();
        }
        
        updateDrivesBeingQuiesced();
        updatePartitionsBeingQuiesced();
    }
    
    
    private void updateDrivesBeingQuiesced()
    {
        for ( final TapeDrive drive : m_tapeDriveService.retrieveAll(
                TapeDrive.QUIESCED, Quiesced.PENDING ).toSet() )
        {
            final AvailableDrive< L > ad = m_locks.get( drive.getId() );
            if ( ( null == ad || null == ad.m_lockHolder) && null == drive.getTapeId() ) 
            {
                m_tapeDriveService.update( 
                        drive.setQuiesced( Quiesced.YES ),
                        TapeDrive.QUIESCED );
                m_failureService.create(
                        drive.getPartitionId(),
                        TapePartitionFailureType.TAPE_DRIVE_QUIESCED,
                        "Tape drive " + drive.getSerialNumber() + " is now quiesced.",
                        null );
            }
        }
    }
    
    
    private void updatePartitionsBeingQuiesced()
    {
        for ( final TapePartition partition : m_tapePartitionService.retrieveAll(
                TapePartition.QUIESCED, Quiesced.PENDING ).toSet() )
        {
            // Partition isn't quiesced if we've locked drives
            boolean quiesced = true;
            final Set< UUID > drivesInPartition = BeanUtils.extractPropertyValues( 
                    m_tapeDriveService.retrieveAll( TapeDrive.PARTITION_ID, partition.getId() ).toSet(),
                    Identifiable.ID );
            for ( final UUID drive : drivesInPartition )
            {
                final AvailableDrive< L > ad = m_locks.get( drive );
                if ( null != ad.m_lockHolder )
                {
                    quiesced = false;
                }
            }
            if ( !quiesced )
            {
                continue;
            }

            // Partition isn't quiesced if we've locked tapes for slot-to-slot moves
            final Set< UUID > tapeIdsLockedWithoutDrive = new HashSet<>();
            for ( final AvailableDrive< L > ad : m_locks.values() )
            {
                if ( null == ad.m_resource && null != ad.m_lockHolder )
                {
                    tapeIdsLockedWithoutDrive.add( ad.m_tapeId );
                }
            }
            if ( 0 < m_tapePartitionService.getCount( Require.all( 
                    Require.beanPropertyEquals( Identifiable.ID, partition.getId() ),
                    Require.exists(
                            Tape.class,
                            Tape.PARTITION_ID,
                            Require.beanPropertyEqualsOneOf(
                                    Identifiable.ID, 
                                    tapeIdsLockedWithoutDrive ) ) ) ) )
            {
                continue;
            }
            
            // Partition isn't quiesced if there are tapes in drives
            if ( 0 < m_tapePartitionService.getCount( Require.all( 
                    Require.beanPropertyEquals( Identifiable.ID, partition.getId() ),
                    Require.exists(
                            TapeDrive.class, 
                            TapeDrive.PARTITION_ID, 
                            Require.not( Require.beanPropertyEquals( TapeDrive.TAPE_ID, null ) ) ) ) ) )
            {
                continue;
            }
            
            m_tapePartitionService.update( 
                    partition.setQuiesced( Quiesced.YES ),
                    TapePartition.QUIESCED );
        }
    }
    
    
    private boolean isAssignable( 
            final TapePartition partition, 
            final TapeDrive drive, 
            final boolean backendActivated )
    {
        if ( !backendActivated )
        {
            LOG.info( getTapeDriveDescription( null, drive )
                      + " is not assignable since the backend is deactivated." );
            return false;
        }
        if ( TapeDriveState.NORMAL != drive.getState() )
        {
            LOG.info( getTapeDriveDescription( null, drive )
                      + " is not assignable since it is in state " + drive.getState() + "." );
            return false;
        }
        if ( TapePartitionState.ONLINE != partition.getState() )
        {
            LOG.info( getTapeDriveDescription( null, drive )
                      + " is not assignable since its partition (" 
                      + partition.getId() + ") is in state " + partition.getState() + "." );
            return false;
        }
        if ( Quiesced.NO != partition.getQuiesced() )
        {
            LOG.info( getTapeDriveDescription( null, drive )
                      + " is not assignable since its partition (" 
                      + partition.getId() + ") is " + TapePartition.QUIESCED + "=" + partition.getQuiesced() 
                      + "." );
            return false;
        }
        if ( Quiesced.NO != drive.getQuiesced() )
        {
            LOG.info( getTapeDriveDescription( null, drive )
                      + " is not assignable since it is " + TapeDrive.QUIESCED
                      + "=" + drive.getQuiesced() + "." );
            return false;
        }
        return true;
    }
    
    
    private void tapeDriveNoLongerAssignable( final UUID tapeDriveId )
    {
        final TapeDrive drive = m_assignableDrives.get( tapeDriveId );
        LOG.warn( getTapeDriveDescription( tapeDriveId, drive )
                  + " is no longer available to have work assigned to it." );
        m_assignableDrives.remove( tapeDriveId );
        final AvailableDrive< L > atd = m_locks.get( tapeDriveId );
        if ( null != atd.m_lockHolder )
        {
            LOG.warn( "When " + getTapeDriveDescription( tapeDriveId, drive )
                    + " became unavailable, it was locked as a resource by: " + atd.m_lockHolder );
        }
    }
    
    
    private void registerNewTapeDrive( final TapeDrive tapeDrive, final Callable<Boolean> idleCallback )
    {
        final UUID tapeDriveId = tapeDrive.getId();
        LOG.info( getTapeDriveDescription( tapeDriveId, tapeDrive )
                  + " came online." );

        final Integer driveIdleTimeoutInMinutes = m_tapePartitionService.retrieve(tapeDrive.getPartitionId())
                .getDriveIdleTimeoutInMinutes();
        
        final AvailableDrive< L > atd = new AvailableDrive<>();
        final String resourceInstanceName = m_tapeDriveService.attain( tapeDriveId ).getSerialNumber();
        atd.m_resource = new TapeDriveResourceWrapper(
                m_rpcClient.getRpcResource(
                TapeDriveResource.class,
                resourceInstanceName, 
                ConcurrentRequestExecutionPolicy.SERIALIZED ),
                idleCallback,
                driveIdleTimeoutInMinutes != null? TimeUnit.MINUTES.toMillis( driveIdleTimeoutInMinutes ): null);
        if ( m_locks.containsKey( tapeDriveId ) )
        {
            LOG.warn( "Tape drive registered as new, but was already managed: " + tapeDriveId );
        }
        else
        {
            m_locks.put( tapeDriveId, atd );
        }
        
    }
    
    private void tapeDriveBecameAssignable( final TapeDrive tapeDrive )
    {
        final UUID tapeDriveId = tapeDrive.getId();
        LOG.info( getTapeDriveDescription( tapeDriveId, tapeDrive )
                      + " became available to have work assigned to it." );
        m_assignableDrives.put( tapeDriveId, tapeDrive );
        if ( !m_locks.containsKey( tapeDriveId ) )
        {
            throw new IllegalStateException( "Tape drive is not managed: " + tapeDriveId );
        }
    }
    
    
    synchronized private L getLockHolder( final UUID tapeDriveId )
    {
        final AvailableDrive< L > atd = m_locks.get( tapeDriveId );
        if ( null == atd )
        {
            throw new IllegalStateException( "Tape drive is not managed: " + tapeDriveId );
        }
        
        return atd.m_lockHolder;
    }
    
    
    synchronized public Set< UUID > getAvailableTapeDrives()
    {
        final Set< UUID > retval = new HashSet<>();
        for ( final UUID id : m_assignableDrives.keySet() )
        {
            if ( null == getLockHolder( id ) )
            {
                retval.add( id );
            }
        }
        
        return retval;
    }
    
    
    synchronized public void lockWithoutDrive( final L lockHolder )
    {
        lock( acquireDrivelessLock(), lockHolder );
    }
    
    
    private UUID acquireDrivelessLock()
    {
        for ( final Map.Entry< UUID, AvailableDrive< L > > e : m_locks.entrySet() )
        {
            if ( m_assignableDrives.containsKey( e.getKey() ) || null != e.getValue().m_resource )
            {
                continue;
            }
            
            if ( null == e.getValue().m_lockHolder )
            {
                return e.getKey();
            }
        }
        
        final UUID newLock = UUID.randomUUID();
        m_locks.put( newLock, new AvailableDrive<>() );
        return newLock;
    }
    
    
    public TapeDriveResource forceLock( final UUID tapeDriveId, final L lockHolder )
    {
        return lockInternal( tapeDriveId, lockHolder, true );
    }
    
    
    public TapeDriveResource lock( final UUID tapeDriveId, final L lockHolder )
    {
        return lockInternal( tapeDriveId, lockHolder, false );
    }
    
    
    synchronized private TapeDriveResource lockInternal( 
            final UUID tapeDriveId, final L lockHolder, final boolean force )
    {
        verifyNothingLocked( tapeDriveId, lockHolder );
        final L existingLockHolder = getLockHolder( tapeDriveId );
        if ( null != existingLockHolder && existingLockHolder != lockHolder )
        {
            throw new IllegalStateException(
                    getTapeDriveDescription( tapeDriveId )
                    + " is already locked by " + existingLockHolder + "." );
        }
        if ( null != m_locks.get( tapeDriveId ).m_resource && !m_assignableDrives.containsKey( tapeDriveId ) )
        {
            if ( !force )
            {
                throw new IllegalStateException(
                        getTapeDriveDescription( tapeDriveId )
                        + " is not available to be locked at this time." );
            }
        }
        LOG.info( "Locked " + getTapeDriveDescription( tapeDriveId ) + " for " + lockHolder + "." );
        m_locks.get( tapeDriveId ).m_lockHolder = lockHolder;
        return m_locks.get( tapeDriveId ).m_resource;
    }
    
    
    synchronized public void addTapeLock( final L lockHolder, final UUID tapeId )
    {
        Validations.verifyNotNull( "Tape id", tapeId );
        if ( getLockedTapes( null ).contains( tapeId ) )
        {
            final L lh = getTapeLockHolder( tapeId );
            if ( lh == lockHolder )
            {
                return;
            }
            throw new IllegalStateException( "Tape " + tapeId + " is already locked by " + lh );
        }
        if ( !m_dataPathBackendService.isActivated() )
        {
            throw new IllegalStateException( "The backend is deactivated." );
        }
        
        for ( final AvailableDrive< L > d : m_locks.values() )
        {
            if ( d.m_lockHolder == lockHolder )
            {
                if ( null != d.m_tapeId )
                {
                    throw new UnsupportedOperationException( 
                            "The locked tape cannot be changed from " 
                            + d.m_tapeId + " to " + tapeId + " for " + lockHolder );
                }
                if ( null == d.m_resource )
                {
                    final TapePartition partition = m_tapePartitionService.retrieve( Require.exists( 
                            Tape.class,
                            Tape.PARTITION_ID, 
                            Require.beanPropertyEquals( Identifiable.ID, tapeId ) ) );
                    if ( null != partition )
                    {
                        if ( TapePartitionState.ONLINE != partition.getState() )
                        {
                            throw new IllegalStateException( 
                                    "The tape is in a partition that is " + partition.getState() + "." );
                        }
                        if ( Quiesced.NO != partition.getQuiesced() )
                        {
                            throw new IllegalStateException( 
                                    "The tape is in a partition that is quiesced=" 
                                    + partition.getQuiesced() + "." );
                        }
                    }
                }
                LOG.info( "Locked tape " + tapeId + " for " + lockHolder + "." );
                d.m_tapeId = tapeId;
                return;
            }
        }
        
        throw new IllegalStateException( "You must acquire a lock before you can lock a tape." );
    }
    
    
    synchronized public Set< UUID > getLockedTapes( final L lockHolder )
    {
        final Set< UUID > retval = new HashSet<>();
        for ( final AvailableDrive< L > d : m_locks.values() )
        {
            if ( null == lockHolder || lockHolder == d.m_lockHolder )
            {
                retval.add( d.m_tapeId );
            }
        }
        
        retval.remove( null );
        return retval;
    }
    
    
    synchronized public L getTapeLockHolder( final UUID tapeId )
    {
        Validations.verifyNotNull( "Tape id", tapeId );
        for ( final AvailableDrive< L > d : m_locks.values() )
        {
            if ( tapeId.equals( d.m_tapeId ) )
            {
                return d.m_lockHolder;
            }
        }
        
        return null;
    }
    
    
    synchronized public UUID unlock( final L lockHolder )
    {
        for ( final Map.Entry< UUID, AvailableDrive< L > > e : m_locks.entrySet() )
        {
            if ( e.getValue().m_lockHolder == lockHolder )
            {
                final String suffix =
                        ( null == e.getValue().m_tapeId ) ? "" : " and tape " + e.getValue().m_tapeId;
                if ( e.getValue().m_tapeId != null ) {
                    m_recentlyUnlockedTapes.add(e.getValue().m_tapeId);
                }
                LOG.info( "Unlocked " + getTapeDriveDescription( e.getKey() ) 
                          + suffix + " (was locked by " + lockHolder + ")." );
                e.getValue().m_lockHolder = null;
                e.getValue().m_tapeId = null;
                return e.getKey();
            }
        }
        
        throw new IllegalStateException( "No lock held by " + lockHolder + "." );
    }
    
    
    private void verifyNothingLocked( final UUID lockToAcquire, final L lockHolder )
    {
        for ( final Map.Entry< UUID, AvailableDrive< L > > e : m_locks.entrySet() )
        {
            if ( e.getKey().equals( lockToAcquire ) )
            {
                continue;
            }
            if ( e.getValue().m_lockHolder == lockHolder )
            {
                throw new UnsupportedOperationException(
                        lockHolder 
                        + " cannot lock " + getTapeDriveDescription( lockToAcquire )
                        + " since already locked " 
                        + getTapeDriveDescription( e.getKey() )
                        + " and no lock holder is allowed to hold multiple locks." );
            }
        }
    }
    
    
    synchronized public Set< L > getAllLockHolders()
    {
        final Set< L > retval = new HashSet<>();
        for ( final AvailableDrive< L > d : m_locks.values() )
        {
            retval.add( d.m_lockHolder );
        }
        
        retval.remove( null );
        return retval;
    }
    
    
    private final static class AvailableDrive< L >
    {
        private TapeDriveResource m_resource;
        private L m_lockHolder;
        private UUID m_tapeId;
    } // end inner class def
    
    
    private String getTapeDriveDescription( final UUID lockId, final TapeDrive drive )
    {
        if ( null != drive )
        {
            return "Tape Drive " + drive.getId() + " (" + drive.getSerialNumber() + ")";
        }
        return getTapeDriveDescription( lockId );
    }
    
    
    private String getTapeDriveDescription( final UUID lockId )
    {
        if ( m_assignableDrives.containsKey( lockId ) )
        {
            final TapeDrive drive = m_assignableDrives.get( lockId );
            return "Tape Drive " + drive.getId() + " (" + drive.getSerialNumber() + ")";
        }
        
        final TapeDrive drive = m_tapeDriveService.retrieve( lockId );
        if ( null != drive )
        {
            return "Unavailable Tape Drive " + lockId;
        }
        
        return "<no tape drive>";
    }

    public synchronized  Set< UUID > getRecentlyUnlocked() {
        return new HashSet<>( m_recentlyUnlockedTapes );
    }

    /**
     *  Clears the recently unlocked list.
     */
    public synchronized void clearRecentlyUnlocked() {
        m_recentlyUnlockedTapes.clear();
    }
    
    
    private final Map< UUID, TapeDrive > m_assignableDrives = new HashMap<>(); //Keys are tape drive ID's
    private final Map< UUID, AvailableDrive< L > > m_locks = new HashMap<>(); //Keys are tape drive ID's, or random UUID's if driveless lock
    private final Set< UUID > m_recentlyUnlockedTapes = new HashSet<>();
    
    private final TapeDriveService m_tapeDriveService;
    private final TapePartitionService m_tapePartitionService;
    private final TapePartitionFailureService m_failureService;
    private final DataPathBackendService m_dataPathBackendService;
    private final RpcClient m_rpcClient;
    
    private final static Logger LOG = Logger.getLogger( TapeLockSupport.class );
}
