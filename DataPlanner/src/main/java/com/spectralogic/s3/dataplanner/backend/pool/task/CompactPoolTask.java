/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.orm.PoolRM;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

import com.spectralogic.util.tunables.Tunables;

public final class CompactPoolTask extends BasePoolTask
{
    public CompactPoolTask(final BlobStoreTaskPriority priority, final UUID poolId, final BeansServiceManager serviceManager,
                           final PoolEnvironmentResource poolEnvironmentResource,
                           final PoolLockSupport<PoolTask> lockSupport,
                           final DiskManager diskManager,
                           final JobProgressManager jobProgressManager)
    {
        super( priority, serviceManager, poolEnvironmentResource, lockSupport, diskManager, jobProgressManager );
        m_poolId = poolId;
        Validations.verifyNotNull( "Pool id", m_poolId );
        m_poolName = poolId.toString();
    }
    
    
    @Override public String getDescription()
    {
        return "Compact Pool " + m_poolName + " (" + m_compactionLevel + ")";
    }
    
    
    private boolean reclaimEntirePool( final Pool pool )
    {
        if ( pool.isAssignedToStorageDomain() && null != pool.getStorageDomainMemberId() )
        {
            final StorageDomain storageDomain = new PoolRM( pool, m_serviceManager ).getStorageDomainMember()
                                                                                    .getStorageDomain()
                                                                                    .unwrap();
            if ( storageDomain.isSecureMediaAllocation() )
            {
                LOG.info( "Even though pool has no data on it, cannot reclaim it since storage domain " +
                        storageDomain.getId() + " has secure media allocation." );
                return false;
            }
        }
        
        LOG.info( "Full reclaim will be performed since there is no data on the pool." );
        new UnknownDataDeleter( this ).run();
        if ( !pool.isAssignedToStorageDomain() )
        {
            return true;
        }
        
        LOG.info( "Releasing pool from its storage domain assignment." );
        getServiceManager().getService( PoolService.class )
                           .update( pool.setStorageDomainMemberId( null )
                                        .setBucketId( null )
                                        .setAssignedToStorageDomain( false ),
                                   PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID,
                                   PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        return true;
    }
    
    
    @Override protected BlobStoreTaskState runInternal()
    {
        final Pool pool = getPool();
        if ( 0 == getServiceManager().getRetriever( Pool.class )
                                     .getCount( Require.all( Require.beanPropertyEquals( Identifiable.ID, getPoolId() ),
                                             Require.exists( BlobPool.class, BlobPool.POOL_ID, Require.nothing() ) ) ) )
        {
            if ( reclaimEntirePool( pool ) )
            {
                return BlobStoreTaskState.COMPLETED;
            }
        }
    
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final long total = pool.getTotalCapacity() - pool.getReservedCapacity();
        long used = pool.getUsedCapacity();
        double usage = used * 1.0f / total;
        int usagePercent = ( int ) ( usage * 100 );
    
        LOG.info( "Pool " + m_poolId + " is at " + usagePercent + "% utilization (" + bytesRenderer.render( used ) +
                " used, " + bytesRenderer.render( pool.getAvailableCapacity() ) + " available, and " +
                bytesRenderer.render( total ) + " total)." );
    
        if ( CompactionLevel.ONE_TIME_UNKNOWN_DATA_ONLY == m_compactionLevel )
        {
            final Path compactedMarkPath = PoolUtils.getCompactedUnknownMarkPath( getPool() );
            m_compactionLevel = CompactionLevel.values()[ m_compactionLevel.ordinal() + 1 ];
            if ( Files.exists( compactedMarkPath ) )
            {
                LOG.info( "Marker file " + compactedMarkPath + " exists, initial catchup compaction not required." );
            }
            else
            {
                new UnknownDataDeleter( this ).run();
                try
                {
                    Files.createFile( compactedMarkPath );
                }
                catch ( final IOException e )
                {
                    LOG.warn( "Marker file " + compactedMarkPath + " could not be created.", e );
                }
                return BlobStoreTaskState.READY;
            }
        }
    
        used = pool.getUsedCapacity();
        usage = used * 1.0f / total;
        usagePercent = ( int ) ( usage * 100 );
        if ( usagePercent < THRESHOLD_TO_COMPACT )
        {
            LOG.info( "Pool " + m_poolId + " does not require compaction at this time." );
            return BlobStoreTaskState.COMPLETED;
        }
        if ( CompactionLevel.GIVE_UP == m_compactionLevel )
        {
            LOG.info( "Pool " + m_poolId + " could not be compacted to the desired usage level." );
            return BlobStoreTaskState.COMPLETED;
        }
        
        LOG.info( "Pool " + m_poolId + " will be compacted at level " + m_compactionLevel + "." );
        new OldDataDeleter().run();
    
        LOG.info( "Downgrading lock on pool " + m_poolId + " from exclusive to delete lock." );
        synchronized ( getLockSupport() )
        {
            getLockSupport().releaseLock( this );
            getLockSupport().acquireDeleteLock( getPoolId(), this );
        }
        new UnknownDataDeleter( this ).run();
    
        m_compactionLevel = CompactionLevel.values()[ m_compactionLevel.ordinal() + 1 ];
        
        doNotTreatReadyReturnValueAsFailure();
        return BlobStoreTaskState.READY;
    }
    
    
    @Override protected UUID selectPool()
    {
        synchronized ( getLockSupport() )
        {
            if ( ( CompactionLevel.ONE_TIME_UNKNOWN_DATA_ONLY != m_compactionLevel ) &&
                    getLockSupport().getPoolsUnavailableForDeleteLock()
                                    .contains( m_poolId ) )
            {
                return null;
            }
    
            if ( CompactionLevel.ONE_TIME_UNKNOWN_DATA_ONLY == m_compactionLevel )
            {
                getLockSupport().acquireDeleteLock( m_poolId, this );
            }
            else
            {
                getLockSupport().acquireExclusiveLock( m_poolId, this );
            }
        }
    
        m_poolName = getServiceManager().getService( PoolService.class )
                                        .attain( m_poolId )
                                        .getName();
        return m_poolId;
    }
    
    
    private enum CompactionLevel
    {
        ONE_TIME_UNKNOWN_DATA_ONLY( null ), NOT_RECENTLY_ACCESSED( BlobPool.LAST_ACCESSED ),
        MAXIMUM( BlobPool.DATE_WRITTEN ), GIVE_UP( null );
        
        
        CompactionLevel( final String blobPoolDateProperty )
        {
            m_blobPoolDateProperty = blobPoolDateProperty;
        }
        
        
        private final String m_blobPoolDateProperty;
    } // end inner class def
    
    private final class OldDataDeleter implements Runnable
    {
        @Override public void run()
        {
            if ( null == m_compactionLevel.m_blobPoolDateProperty )
            {
                return;
            }
            
            final Duration duration = new Duration();
            m_work = new MonitoredWork( StackTraceLogging.NONE, "Deleting old data on pool " + m_poolId + "." );
            m_work.setShouldStopMonitoring( false );
            try
            {
                runInternal();
                LOG.info( "Marked old files for deletion in " + duration + "." );
            }
            finally
            {
                m_work.completed();
            }
        }
        
        
        private void runInternal()
        {
            final Set< Bucket > buckets = getServiceManager().getRetriever( Bucket.class )
                                                             .retrieveAll(
                                                                     Require.exists( BlobPool.class, BlobPool.BUCKET_ID,
                                                                             Require.beanPropertyEquals(
                                                                                     BlobPool.POOL_ID, m_poolId ) ) )
                                                             .toSet();
            
            if ( null != getPool().getStorageDomainMemberId() )
            {
                final UUID storageDomainId = new PoolRM( getPool(), m_serviceManager ).getStorageDomainMember()
                                                                                      .unwrap()
                                                                                      .getStorageDomainId();
                for ( final Bucket bucket : buckets )
                {
                    final DataPersistenceRule temporaryRule =
                            getServiceManager().getRetriever( DataPersistenceRule.class )
                                               .retrieve( Require.all( Require.beanPropertyEquals(
                                                       DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainId ),
                                                       Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID,
                                                               bucket.getDataPolicyId() ),
                                                       Require.beanPropertyEquals( DataPersistenceRule.TYPE,
                                                               DataPersistenceRuleType.TEMPORARY ) ) );
                    if ( null == temporaryRule )
                    {
                        continue;
                    }
    
                    final Integer minimumDaysToRetain = temporaryRule.getMinimumDaysToRetain();
                    m_work.setCustomMessage(
                            x -> "Deleting temporary data from bucket " + bucket.getName() + " on pool " + m_poolId +
                                    " by " + m_compactionLevel.m_blobPoolDateProperty + " with " + minimumDaysToRetain +
                                    " minimum days to retain." );
                    getServiceManager().getService( BlobPoolService.class )
                                       .reclaimForTemporaryPersistenceRule( m_poolId, bucket.getId(),
                                               minimumDaysToRetain,
                                               m_compactionLevel.m_blobPoolDateProperty );
                }
            }
        }
    
    
        private MonitoredWork m_work;
        
    } // end inner class def
    
    private final class UnknownDataDeleter implements Runnable
    {
        private UnknownDataDeleter( final BasePoolTask lockHolder )
        {
            final Pool pool = getPool();
            m_lockHolder = lockHolder;
            m_root = Paths.get( pool.getMountpoint() );
            m_trash = PoolUtils.getTrashPath( pool );
            m_compactMarker = PoolUtils.getCompactedUnknownMarkPath( pool );
        }
    
    
        @SuppressWarnings( "unused" ) private UnknownDataDeleter()
        {
            throw new RuntimeException(
                    UnknownDataDeleter.class.getCanonicalName() + " cannot be used with the default constructor." );
        }
        
        private void cleanOutEmptyDirectories( final Path path )
        {
            if ( m_trash.equals( path ) || !Files.isDirectory( path ) )
            {
                return;
            }
    
            currentPath = path.toString();
    
            m_directoriesExamined.incrementAndGet();
            try ( final DirectoryStream< Path > stream = Files.newDirectoryStream( path ) )
            {
                for ( final Path p : stream )
                {
                    cleanOutEmptyDirectories( p );
                }
            }
            catch ( final IOException ex )
            {
                LOG.warn( "Failed to get directory entries from '" + path + "'.", ex );
            }
    
            checkForDelay( getDeletingDirectoriesStringFunction() );
    
            try ( final Stream< Path > pathStream = Files.list( path ) )
            {
                if ( !path.equals( m_root ) && 0 == pathStream.count() )

                {
                    for ( final String suffix : PoolUtils.getInfoFileSuffixes() )
                    {
                        final Path infoFile = Paths.get( path + suffix );
                        Files.deleteIfExists( infoFile );
                    }
                    Files.deleteIfExists( path );
                    m_directoriesRemoved.incrementAndGet();
                }
            }
            catch ( final IOException ex )
            {
                LOG.warn( "Failed trying to delete '" + path + "'.", ex );
            }
        }
    
    
        /**
         * We have a delete lock, we're trolling through all of the pool data which can take a wonderfully long
         * time.  So, every so often, release the lock for a while to allow some pool write tasks to come through.
         */
        private void checkForDelay( final Function< Duration, String > returnCustomMessage )
        {
            if ( Tunables.compactPoolTaskMaxContinuousPruneDuration() <= m_duration.getElapsedMinutes() )
            {
                m_work.setCustomMessage(
                        x -> "Delaying pool compaction on " + m_poolId + " to allow other pool tasks to run at " +
                                x.toString() );
                releaseLock();
                try
                {
                    TimeUnit.MINUTES.sleep( PRUNE_WAIT_TIME );
                }
                catch ( InterruptedException ignored )
                {
                }
                acquireLock( returnCustomMessage );
                m_duration.reset();
            }
        }
    
    
        private void releaseLock()
        {
            synchronized ( getLockSupport() )
            {
                getLockSupport().releaseLock( m_lockHolder );
            }
        }
    
    
        private void flushVerify()
        {
            m_blobsChecked.addAndGet( m_blobsToVerify.size() );
            try
            {
                acquireLock( getProcessingStringFunction() );
                final Set< UUID > blobsThatShouldExist = BeanUtils.extractPropertyValues(
                        getServiceManager().getRetriever( BlobPool.class )
                                           .retrieveAll( Require.all(
                                                   Require.beanPropertyEquals( BlobPool.POOL_ID, m_poolId ),
                                                   Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID,
                                                           m_blobsToVerify.keySet() ) ) )
                                           .toSet(), BlobObservable.BLOB_ID );
        
                final Set< UUID > blobsThatShouldNotExist = new HashSet<>( m_blobsToVerify.keySet() );
                blobsThatShouldNotExist.removeAll( blobsThatShouldExist );
                m_blobsRemoved.addAndGet( blobsThatShouldNotExist.size() );
                for ( final UUID blobId : blobsThatShouldNotExist )
                {
                    final Path blobThatShouldNotExist = m_blobsToVerify.get( blobId );
    
                    for ( final String suffix : PoolUtils.getInfoFileSuffixes() )
                    {
                        final Path infoFile = Paths.get( blobThatShouldNotExist.toString() + suffix );
                        try
                        {
                            Files.deleteIfExists( infoFile );
                        }
                        catch ( final IOException e )
                        {
                            LOG.warn( "Failed trying to delete items related to blob '" + blobId + "'.", e );
                        }
                    }
                    try
                    {
                        Files.deleteIfExists( blobThatShouldNotExist );
                    }
                    catch ( final IOException e )
                    {
                        LOG.warn( "Failed trying to delete blob '" + blobId + "'.", e );
                    }
                }
                m_blobsToVerify.clear();
            }
            finally
            {
                releaseLock();
            }
        }
    
    
        private void acquireLock( Function< Duration, String > durationStringFunction )
        {
            m_waitDuration.reset();
            m_work.setCustomMessage(
                    x -> "Have waited " + m_waitDuration + " to reacquire delete lock for " + m_poolId + " at " +
                            x.toString() );
            synchronized ( getLockSupport() )
            {
                getLockSupport().acquireDeleteLockWait( getPoolId(), m_lockHolder );
            }
            m_work.setCustomMessage( durationStringFunction );
        }
    
    
        @Override public void run()
        {
            final Duration totalDuration = new Duration();
            m_work = new MonitoredWork( StackTraceLogging.NONE, UnknownDataDeleter.class.getSimpleName(),
                    getProcessingStringFunction() );
            m_work.setShouldStopMonitoring( false );
            releaseLock();
            try
            {
                run( m_root );
                flushVerify();
                LOG.info( "Deleted " + m_blobsRemoved.get() + " files that had no " + BlobPool.class.getSimpleName() +
                        " records in " + totalDuration + "." );
                m_work.setCustomMessage( getDeletingDirectoriesStringFunction() );
                cleanOutEmptyDirectories( m_root );
                LOG.info(
                        "Deleted " + m_directoriesRemoved.get() + " empty directories.  Total pool compact duration " +
                                totalDuration );
            }
            finally
            {
                releaseLock();
                m_work.completed();
            }
        }
    
    
        private Function< Duration, String > getProcessingStringFunction()
        {
            return x -> String.format( "So far processed %d (%d pending) blobs and removed %d dead ones from %s in %s",
                    m_blobsChecked.get(), m_blobsToVerify.size(), m_blobsRemoved.get(), m_root, x.toString() );
        }
    
    
        private Function< Duration, String > getDeletingDirectoriesStringFunction()
        {
            return x -> {
                final String format =
                        String.format( "So far removed %d directories out of %d (%.1f/s) examined at %s in %s",
                                m_directoriesRemoved.get(), m_directoriesExamined.get(),
                                ( m_directoriesExamined.get() - lastDirectoriesExamined.get() ) /
                                        ( double ) lastUpdate.getElapsedSeconds(), currentPath, x.toString() );
                lastUpdate.reset();
                lastDirectoriesExamined.set( m_directoriesExamined.get() );
                return format;
            };
        }
    
    
        private void run( final Path path )
        {
            if ( m_trash.equals( path ) || PoolUtils.isInfoFile( path ) || m_compactMarker.equals( path ) )
            {
                return;
            }
            if ( !Files.isDirectory( path ) )
            {
                verify( path );
                return;
            }
            
            try ( final DirectoryStream< Path > stream = Files.newDirectoryStream( path ) )
            {
                for ( final Path p : stream )
                {
                    run( p );
                }
            }
            catch ( final IOException ex )
            {
                LOG.warn( "Failed to get directory entries from '" + path + "'.", ex );
            }
        }
        
        
        private void verify( final Path file )
        {
            final UUID blobId = UUID.fromString( file.getFileName()
                                                     .toString() );
            m_blobsToVerify.put( blobId, file );
            if ( Tunables.compactPoolTaskMaxBlobsToVerify() <= m_blobsToVerify.size() )
            {
                flushVerify();
            }
        }
    
    
        static final int PRUNE_WAIT_TIME = 2;
        private final AtomicInteger lastDirectoriesExamined = new AtomicInteger( 0 );
        private final Duration lastUpdate = new Duration();
        private final AtomicInteger m_blobsChecked = new AtomicInteger( 0 );
        private final AtomicInteger m_blobsRemoved = new AtomicInteger( 0 );
        private final Map< UUID, Path > m_blobsToVerify = new HashMap<>();
        private final AtomicInteger m_directoriesExamined = new AtomicInteger( 0 );
        private final AtomicInteger m_directoriesRemoved = new AtomicInteger( 0 );
        private final Duration m_duration = new Duration();
        private final BasePoolTask m_lockHolder;
        private final Path m_root;
        private final Path m_trash;
        private final Path m_compactMarker;
        private final Duration m_waitDuration = new Duration();
        private String currentPath = "";
        private MonitoredWork m_work;
    } // end inner class def
    
    private final static int THRESHOLD_TO_COMPACT = 95;
    private final UUID m_poolId;
    private String m_poolName;
    private volatile CompactionLevel m_compactionLevel = CompactionLevel.ONE_TIME_UNKNOWN_DATA_ONLY;
}
