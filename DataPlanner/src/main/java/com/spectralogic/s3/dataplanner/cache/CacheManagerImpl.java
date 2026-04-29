/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.platform.cache.CacheListener;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.S3ObjectsCachedNotificationPayloadGenerator;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheEntryInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheFilesystemInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheInformation;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.CloseableIterable;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import lombok.NonNull;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.spectralogic.util.tunables.Tunables;

public class CacheManagerImpl implements CacheManager
{
    public CacheManagerImpl( final BeansServiceManager serviceManager, final TierExistingCache tierExistingCache ) {
        this( serviceManager,
                tierExistingCache,
                HardwareInformationProvider.getZfsCacheFilesystemRecordSize() * 2 ); //* 2 because we have two files per blob );
    }

    CacheManagerImpl( final BeansServiceManager serviceManager, final TierExistingCache tierExistingCache, final long fileSystemOverheadPerBlob ) {
        this( serviceManager,
                new AsyncBlobCacheDeleterImpl(serviceManager.getService( BlobCacheService.class ),
                        fileSystemOverheadPerBlob), fileSystemOverheadPerBlob,
                new CacheInitializer(serviceManager.getRetriever( CacheFilesystem.class ).retrieveAll().getFirst(),
                        tierExistingCache,
                        serviceManager,
                        fileSystemOverheadPerBlob).init()
        );
    }

    CacheManagerImpl(final BeansServiceManager serviceManager,
                     final AsyncBlobCacheDeleter asyncBlobCacheDeleter,
                     final long fileSystemOverheadPerBlob,
                     final long bytesAlreadyAllocated)
    {
        this( serviceManager,
                asyncBlobCacheDeleter,
                new CacheSpaceReclaimerImpl(serviceManager.getService(BlobCacheService.class), asyncBlobCacheDeleter, fileSystemOverheadPerBlob),
                fileSystemOverheadPerBlob,
                bytesAlreadyAllocated );
    }

    //package private constructor for test only
    CacheManagerImpl(final BeansServiceManager serviceManager,
                     final AsyncBlobCacheDeleter asyncBlobCacheDeleter,
                     final CacheSpaceReclaimer cacheSpaceReclaimer,
                     final long fileSystemOverheadPerBlob,
                     final long bytesAlreadyAllocated)
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_serviceManager = serviceManager;
        m_blobCacheService = m_serviceManager.getService( BlobCacheService.class );
        m_filesystem = m_serviceManager.getRetriever( CacheFilesystem.class )
                .retrieveAll()
                .getFirst();
        m_filesystemOverheadPerBlob = fileSystemOverheadPerBlob;

        m_asyncBlobCacheDeleter = asyncBlobCacheDeleter;
        m_bytesAllocated = new AtomicLong(bytesAlreadyAllocated);


        m_totalCapacityInBytes = calculateTotalCapacityInBytes();

        m_cacheThrottleManager =  new CacheThrottleManager(m_serviceManager);

        deleteIncompleteCacheFiles();
        m_serviceManager.getUpdater( CacheFilesystem.class )
                .update(m_filesystem.setNeedsReconcile(false), CacheFilesystem.NEEDS_RECONCILE);
        logThresholds();

        m_cacheSpaceReclaimer = cacheSpaceReclaimer;
        if ( !performAutoReclaim() )
        {
            logStatistics();
        }
    }


    public CacheManagerImpl( final BeansServiceManager serviceManager )
    {
        this( serviceManager, new TierExistingCacheImpl( serviceManager.getRetriever( CacheFilesystem.class )
                                                                       .retrieveAll()
                                                                       .getFirst()
                                                                       .getPath() ) );
    }


    public void createFilesForZeroLengthChunk( final JobEntry chunk )
    {
        allocateChunk(chunk.getId()); //NOTE: see comments on allocateChunksForBlob() regarding why this is necessary
        try
        {
            final Path path = Paths.get( allocateChunksForBlob( chunk.getBlobId() ) );
            if ( !Files.exists( path ) )
            {
                Files.createDirectories( path.getParent() ); //NOTE: this is currently only necessary for tests, but may be desirable in future work
                Files.createFile( path );
            }
            blobLoadedToCache( chunk.getBlobId() );
        } catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        m_serviceManager.getNotificationEventDispatcher().fire( new JobNotificationEvent(
                m_serviceManager.getRetriever( Job.class ).attain( chunk.getJobId() ),
                m_serviceManager.getRetriever( S3ObjectCachedNotificationRegistration.class ),
                new S3ObjectsCachedNotificationPayloadGenerator(
                        chunk.getJobId(),
                        Set.of(chunk),
                        m_serviceManager.getRetriever( S3Object.class ),
                        m_serviceManager.getRetriever( Blob.class ) ) ) );
    }


    synchronized public void allocateChunks(final Set< UUID > chunkIds )
    {
        if (chunkIds.isEmpty()) {
            LOG.warn("No chunks to allocate.");
            return;
        }
        final MonitoredWork work = new MonitoredWork(
                StackTraceLogging.SHORT,
                "Allocate cache for " + chunkIds.size() + " chunks" );

        final JobEntryService jobEntryService = m_serviceManager.getService( JobEntryService.class );
        final Set<JobEntry> allEntries = jobEntryService.retrieveAll(
            Require.all(
                    //Is one of these chunks
                    Require.beanPropertyEqualsOneOf( JobEntry.ID, chunkIds ),
                    //We are not planning to read the entry from pool
                    Require.beanPropertyNull( JobEntry.READ_FROM_POOL_ID),
                    //For every blob, every cache entry for that blob (if one exists) must be pending delete. If it isn't
                    //that means it's already allocated, we don't want to allocate it again.
                    Require.every(
                            JobEntry.BLOB_ID,
                            Require.every(
                                    BlobCache.class,
                                    BlobCache.BLOB_ID,
                                    Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE) ) ) ) ).toSet();
        final Set< UUID > blobIds = BeanUtils.extractPropertyValues( allEntries, JobEntry.BLOB_ID );
        final Map< UUID, Blob > blobs = BeanUtils.toMap( m_serviceManager.getRetriever(Blob.class).retrieveAll(blobIds).toSet() );
        final long bytesOfData = m_serviceManager.getRetriever(Blob.class)
                .getSum( Blob.LENGTH, Require.beanPropertyEqualsOneOf(Identifiable.ID, blobIds) );
        final long requiredBytesForOverhead = m_filesystemOverheadPerBlob * blobIds.size();
        final long bytesNeeded = bytesOfData + requiredBytesForOverhead;
        if (blobs.isEmpty()) {
            LOG.info( "All blobs for " + chunkIds.size() + " chunks are already in cache." );
        } else {
            LOG.info( "Cache space will be allocated for " + blobs.size() + " blobs from " + chunkIds.size() + " chunks." );

            final boolean mustWait = bytesNeeded > getImmediatelyAvailableCapacityInBytes() || m_blobCacheService.anyDeletePending(blobIds);
            LOG.info( "Cache space needs to be allocated for " + blobIds.size() + " blobs, requiring "
                    + bytesOfData + " bytes for data and "
                    + m_filesystemOverheadPerBlob * blobIds.size()
                    + " bytes for filesystem overhead for a total of " + bytesNeeded + " bytes ("
                    + new BytesRenderer().render( bytesNeeded ) + ")." );
            if (mustWait) {
                final long bytesToFree = bytesNeeded - getSoonAvailableCapacityInBytes();
                final long bytesReclaimed;
                if (bytesToFree > 0) {
                    LOG.info( "Will try to free up cache space and attempt cache allocation again." );
                    bytesReclaimed = runCacheReclaimer( bytesNeeded, bytesNeeded );
                } else {
                    LOG.info( "There is sufficient cache space for the allocation, "
                            + "but only once pending file deletions are committed." );
                    bytesReclaimed = 0;
                }
                if (bytesReclaimed < bytesToFree) {
                    LOG.info("Need " + bytesToFree + " more bytes for allocation, but only " + bytesReclaimed + " bytes were available for reclaim.");
                    //NOTE: there's no need to wait for pending file deletions if we already know we weren't able to
                    //free enough space for allocation.
                } else {
                    LOG.info( "Will quiesce pending file deletions and attempt cache allocation again." );
                    m_asyncBlobCacheDeleter.waitUntilCurrentDeletionsDone();
                }
            }
            ensureCacheFilesystemUpToDate();

            final DataPlannerException insufficientCacheCapacityException =
                    createInsufficientCacheCapacityException( bytesNeeded );
            if ( null != insufficientCacheCapacityException )
            {
                throw insufficientCacheCapacityException;
            }

            // Note: all entries are guaranteed to have the same request type, priority and bucket ID.
            final DetailedJobEntry firstDetailedJobEntry = m_serviceManager.getRetriever(DetailedJobEntry.class).retrieve(allEntries.stream().iterator().next().getId());
            throttleCacheAllocation(firstDetailedJobEntry.getRequestType(), firstDetailedJobEntry.getPriority(), firstDetailedJobEntry.getBucketId(), bytesNeeded);
        }

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            if (blobs.isEmpty()) {
                //NOTE: in this case all we needed to do was update the node ID's because everything was already allocated.
                transaction.commitTransaction();
                return;
            }
            final BlobCacheService bcs = transaction.getService(BlobCacheService.class);
            m_bytesAllocated.addAndGet( bytesNeeded );
            for ( final Blob blob : blobs.values())
            {
                bcs.allocate(blob.getId(), blob.getLength(), m_filesystem );
            }
            m_periodicCacheReclaimExecutor.add( m_periodicCacheReclaimWorker );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
            work.completed();
        }
    }


    public void allocateChunk(final UUID chunkId )
    {
        allocateChunks( Collections.singleton( chunkId ) );
    }


    //NOTE: The vast majority of times this gets called it should result in an early return. Ideally we would refactor
    //this so that allocation is never done in this way, as we should always know the chunk ID to allocate and use
    //that path instead. IF this method were relied on to allocate a GET chunk which had already been indirectly
    //allocated by another overlapping GET job, we would early return here and fail to set the node ID, resulting in a
    //stuck job. Currently, all calls to this in service of GET jobs allocate the chunk separately beforehand.
    public String allocateChunksForBlob( @NonNull final UUID blobId )
    {
        final File existingCacheEntry = m_blobCacheService.getFile( blobId );
        if ( null != existingCacheEntry )
        {
            return existingCacheEntry.getAbsolutePath();
        }
        final JobEntryService jobEntryService = m_serviceManager.getService( JobEntryService.class );

        final Set< UUID > chunkIds = BeanUtils.extractPropertyValues(
                jobEntryService.retrieveAll( Require.beanPropertyEquals(BlobObservable.BLOB_ID, blobId) ).toSet(),
                JobEntry.ID);
        allocateChunks( chunkIds );
        return Objects.requireNonNull( m_blobCacheService.getFile(blobId) )
                .getAbsolutePath();
    }


    protected void throttleCacheAllocation(final JobRequestType requestType, final BlobStoreTaskPriority priority, final UUID bucketId, final long numBytesToAllocate) {
        ensureCacheFilesystemUpToDate();
        m_cacheThrottleManager.throttleCacheAllocation(requestType, priority, bucketId, numBytesToAllocate, getUsedCapacityInBytes(), getTotalCapacityInBytes());
    }


    public String getCacheFileFor( final UUID blobId )
    {
        final File retval = m_blobCacheService.getFileIffInCache( blobId );
        if ( null == retval )
        {
            return null;
        }
        return retval.getAbsolutePath();
    }


    public void blobLoadedToCache( final UUID blobId )
    {
        if (blobId == null) throw new IllegalArgumentException("Blob ID cannot be null");
        final BlobCache bc = m_blobCacheService.retrieveByBlobId(blobId);
        if (bc == null) throw new IllegalStateException("Cache must be allocated for blob before it is loaded: " + blobId );
        final long bytesFreed = m_blobCacheService.cacheEntryLoaded( bc, m_filesystem.getCacheSafetyEnabled() );
        m_bytesAllocated.addAndGet(-bytesFreed);
        for ( final CacheListener l : m_listeners )
        {
        	l.blobLoadedToCache( blobId );
        }
    }


    public boolean isInCache( final UUID blobId )
    {
        return m_blobCacheService.contains( blobId );
    }


    public boolean isChunkEntirelyInCache( final UUID chunkId )
    {
        final JobEntry chunk = m_serviceManager.getRetriever(JobEntry.class).retrieve(chunkId);
        if (!isInCache( chunk.getBlobId() ))
        {
            return false;
        }
        return m_serviceManager.getRetriever( Blob.class ).attain(chunk.getBlobId()).getChecksum() != null;
    }


    public boolean isCacheSpaceAllocated( final UUID blobId )
    {
        return ( m_blobCacheService.isInCache( blobId ) );
    }


    public CacheInformation getCacheState( final boolean includeCacheEntries )
    {
        final CacheInformation retval = BeanFactory.newBean( CacheInformation.class );
        retval.setFilesystems(
                new CacheFilesystemInformation [] { getCacheStateInternal( includeCacheEntries ) } );
        return retval;
    }


    public long getMaximumChunkSizeInBytes()
    {
        return Math.min( Tunables.cacheManagerMaxChunkSize(), getTotalCapacityInBytes() );
    }


    public void forceFullCacheReclaimNow()
    {
        runCacheReclaimer( Long.MAX_VALUE, Long.MAX_VALUE );
    }


    public void registerCacheListener( final CacheListener listener )
    {
    	m_listeners.add( listener );
    }


    public void unregisterCacheListener( final CacheListener listener )
    {
    	m_listeners.remove( listener );
    }


    //NOTE: this function INCLUDES bytes pending delete, so it the number reported may not be strictly immediately available.
    public long getSoonAvailableCapacityInBytes()
    {
        return getTotalCapacityInBytes() - m_bytesAllocated.get();
    }

    public long getImmediatelyAvailableCapacityInBytes()
    {
        return getSoonAvailableCapacityInBytes() - m_asyncBlobCacheDeleter.getBytesPendingDelete();
    }


    private DataPlannerException createInsufficientCacheCapacityException(
            final long requiredBytesTotal )
    {
        final GenericFailure failure;
        final String atThisTime;
        if ( requiredBytesTotal > getTotalCapacityInBytes() )
        {
            failure = GenericFailure.BAD_REQUEST;
            atThisTime = "";
        }
        else if ( requiredBytesTotal > getImmediatelyAvailableCapacityInBytes())
        {
            failure = GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT;
            atThisTime = " at this time";
        }
        else
        {
            return null;
        }

        final BytesRenderer bytesRenderer = new BytesRenderer();
        return new DataPlannerException(
                failure,
                "Cannot allocate capacity" + atThisTime + ".  " + bytesRenderer.render( requiredBytesTotal )
                        + " of cache space is required, but the cache has only "
                        + bytesRenderer.render( getSoonAvailableCapacityInBytes() )
                        + " available out of a total capacity of "
                        + bytesRenderer.render( getTotalCapacityInBytes() ) + "." );
    }


    private void delete(final BlobCache bc) {
        m_blobCacheService.update( bc.setState( CacheEntryState.PENDING_DELETE ), BlobCache.STATE );
        m_asyncBlobCacheDeleter.delete( bc );
        m_bytesAllocated.addAndGet(-(bc.getSizeInBytes() + m_filesystemOverheadPerBlob));
    }


    public long getUsedCapacityInBytes()
    {
        return m_bytesAllocated.get();
    }


    private String logStatistics()
    {
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final String msg =
                m_blobCacheService.getBlobCachesCount() + " blobs allocated by cache:"
                        + bytesRenderer.render( getUsedCapacityInBytes() )
                        + " allocated, "
                        + bytesRenderer.render( getSoonAvailableCapacityInBytes() )
                        + " available (" + bytesRenderer.render( m_asyncBlobCacheDeleter.getBytesPendingDelete() )
                        + " pending reclaim), "
                        + bytesRenderer.render( getTotalCapacityInBytes() )
                        + " total.";
        LOG.info( msg );
        return msg;
    }

    private final class PeriodicCacheReclaimer implements ThrottledRunnable
    {
        @Override
        public void run( final RunnableCompletionNotifier completionNotifier )
        {
            try
            {
                Thread.currentThread().setName( getClass().getSimpleName() );
                performAutoReclaim();
            }
            finally
            {
                completionNotifier.completed();
            }
        }
    } // end inner class def


    private boolean performAutoReclaim()
    {
        ensureCacheFilesystemUpToDate();

        final long total = getTotalCapacityInBytes();
        return runCacheReclaimer(
                (long)( ( 1 - m_filesystem.getAutoReclaimInitiateThreshold() ) * total ),
                (long) ( ( 1 - m_filesystem.getAutoReclaimTerminateThreshold() ) * total ) ) > 0;
    }


    //De-allocates some or all space from the "from" blob and allocates it for the "to" blob.
    public void reallocate(
            final UUID blobIdToAllocateFrom,
            final UUID blobIdToAllocateTo,
            final boolean deleteBlobToAllocateFrom )
    {
        Validations.verifyNotNull( "Blob Id To Allocate From", blobIdToAllocateFrom );
        Validations.verifyNotNull( "Blob Id To Allocate To", blobIdToAllocateTo );

        final Blob blobTo = m_serviceManager.getRetriever(Blob.class).attain(blobIdToAllocateTo);
        final BlobCache bcFrom = m_blobCacheService.retrieveByBlobId(blobIdToAllocateFrom);
        final BlobCache bcTo = m_blobCacheService.retrieveByBlobId(blobIdToAllocateTo);
        if ( null == bcFrom )
        {
            throw new IllegalStateException(
                    "Cannot reallocate from a blob that has not been allocated." );
        }
        if ( null != bcTo )
        {
            throw new IllegalStateException(
                    "Cannot reallocate to a blob that has already been allocated." );
        }
        if ( Files.exists( Paths.get( bcFrom.getPath() ) ) )
        {
            if (deleteBlobToAllocateFrom)
            {
                //NOTE: we do not decrement the space used because we are still going to use it for the blob we
                //are reallocating to
                m_blobCacheService.update( bcFrom.setState( CacheEntryState.PENDING_DELETE ), BlobCache.STATE );
                m_asyncBlobCacheDeleter.delete( bcFrom );
            }
            else
            {
                throw new IllegalStateException(
                        "Cannot reallocate from a blob that has been written partially or completely to cache." );
            }
        }
        if ( blobTo.getLength() > bcFrom.getSizeInBytes() )
        {
            throw new IllegalStateException(
                    "Cannot reallocate from a blob that has " + bcFrom.getSizeInBytes()
                            + "B to a blob of size " + blobTo.getLength() + "B." );
        }
        m_blobCacheService.update( bcFrom.setSizeInBytes( bcFrom.getSizeInBytes() - blobTo.getLength() ), BlobCache.SIZE_IN_BYTES );
        final long size = blobTo.getLength();
        //NOTE: we only increase by the overhead because we already had this space allocated
        m_bytesAllocated.addAndGet(m_filesystemOverheadPerBlob);
        m_blobCacheService.allocate(blobIdToAllocateTo, size, m_filesystem );
    }

    protected long getJobLockedCacheInBytes() {
        return m_blobCacheService.getSum(BlobCache.SIZE_IN_BYTES,
                Require.all(
                        Require.exists(
                                BlobCache.BLOB_ID,
                                Require.exists(
                                        JobEntry.class,
                                        BlobObservable.BLOB_ID,
                                        Require.nothing()
                                )
                        ),
                        Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE))
                ));
    }


    public long runCacheReclaimer(
            final long minFreeBytesDesired,
            final long minFreeBytesBeforeReclaimStopsEarly ) {
        if (getSoonAvailableCapacityInBytes() >= minFreeBytesDesired) {
            return 0;
        }
        final long bytesReclaimed =  m_cacheSpaceReclaimer.run(minFreeBytesBeforeReclaimStopsEarly - getSoonAvailableCapacityInBytes());
        m_bytesAllocated.addAndGet(-bytesReclaimed);
        logStatistics();
        return bytesReclaimed;
    }


    private CacheFilesystemInformation getCacheStateInternal( final boolean includeCacheEntries )
    {
        ensureCacheFilesystemUpToDate();
        final CacheFilesystemInformation retval = BeanFactory.newBean( CacheFilesystemInformation.class );
        retval.setUsedCapacityInBytes( getUsedCapacityInBytes() );
        retval.setAvailableCapacityInBytes( getSoonAvailableCapacityInBytes() );
        retval.setTotalCapacityInBytes( getTotalCapacityInBytes() );
        retval.setUnavailableCapacityInBytes( m_asyncBlobCacheDeleter.getBytesPendingDelete() );
        retval.setJobLockedCacheInBytes( getJobLockedCacheInBytes() );
        retval.setSummary( logStatistics() + "  " + logThresholds() );
        retval.setCacheFilesystem( m_filesystem );

        if ( includeCacheEntries )
        {
            final Set<CacheEntryInformation> entries = new HashSet<>();
            //NOTE: since we need to pull both blobs and blob caches into memory at once, we go in batches. All at once would burden
            //memory and one at a time would be too slow.
            try (final CloseableIterable<Set<BlobCache>> blobCacheSets = m_blobCacheService.retrieveAll(
                    Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE))
            ).toSetsOf(10000) ) {
                for (Set<BlobCache> blobCaches : blobCacheSets) {
                    final Set<UUID> blobIds = BeanUtils.extractPropertyValues(blobCaches, BlobCache.BLOB_ID);
                    blobIds.remove(null); //Remove null, which could have been added by a BlobCache whose blob has been deleted.
                    final Map<UUID, Blob> blobs = m_serviceManager.getRetriever(Blob.class).retrieveAll( Require.beanPropertyEqualsOneOf(Identifiable.ID, blobIds) ).toMap();
                    for ( final BlobCache bc : blobCaches )
                    {
                        final CacheEntryInformation entry = BeanFactory.newBean( CacheEntryInformation.class );
                        if (bc.getBlobId() != null) {
                            entry.setBlob( blobs.get( bc.getBlobId() ) );
                        }
                        entry.setState( bc.getState() );
                        entries.add( entry );
                    }
                }
            }
            retval.setEntries( CollectionFactory.toArray( CacheEntryInformation.class, entries ) );
        }
        return retval;
    }

    private void deleteIncompleteCacheFiles() {
        final RetrieveBeansResult<BlobCache> blobCachesToDelete = m_blobCacheService.retrieveAll(
                Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.IN_CACHE)));
        int fileCount = 0;
        try (final EnhancedIterable<BlobCache> iter = blobCachesToDelete.toIterable()) {
            iter.forEach(bc -> delete(bc));
            fileCount++;
        }
        LOG.info("Found " + fileCount + " cache files that were incomplete or pending delete.");
    }

    public void lockBlobs(final UUID lockHolder, final Set<UUID> blobIds) {
        m_cacheSpaceReclaimer.lockBlobs(lockHolder, blobIds);
    }

    public void unlockBlobs(final UUID jobId) {
        m_cacheSpaceReclaimer.unlockBlobs(jobId);
    }


    public long getCacheSizeInBytes()
    {
        return calculateTotalCapacityInBytes();
    }


    private boolean ensureCacheFilesystemUpToDate()
    {
        final CacheFilesystem latest = m_serviceManager.getRetriever(CacheFilesystem.class).attain( m_filesystem.getId() );
        final List< String > changedProps = BeanUtils.getChangedProps(CacheFilesystem.class, m_filesystem, latest);
        if ( !changedProps.isEmpty() )
        {
            m_filesystem = latest;
            m_totalCapacityInBytes = calculateTotalCapacityInBytes();
            LOG.info( "Cache configuration changes detected for: " + changedProps );
            logThresholds();
            logStatistics();
            return true;
        }
        return false;
    }


    public String logThresholds()
    {
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final long bytesAutoReclaimStart =
                (long)( getTotalCapacityInBytes() * m_filesystem.getAutoReclaimInitiateThreshold() );
        final long bytesAutoReclaimStop =
                (long)( getTotalCapacityInBytes() * m_filesystem.getAutoReclaimTerminateThreshold() );
        final String msg =
                "Auto-reclaims start at " + bytesRenderer.render( bytesAutoReclaimStart )
                        + " and stop early once usage drops below "
                        + bytesRenderer.render( bytesAutoReclaimStop ) + ".";
        LOG.info( msg );
        return msg;
    }


    private long calculateTotalCapacityInBytes()
    {
        final long staticMax = ( null == m_filesystem.getMaxCapacityInBytes() ) ?
                Long.MAX_VALUE
                : m_filesystem.getMaxCapacityInBytes();
        final long dynamicMax = ( null == m_filesystem.getMaxPercentUtilizationOfFilesystem() ) ?
                Long.MAX_VALUE
                : (long) ( m_filesystem.getMaxPercentUtilizationOfFilesystem() *
                ( new File( m_filesystem.getPath() ).getTotalSpace() ) );
        return Math.min( staticMax, dynamicMax );
    }


    public long getTotalCapacityInBytes()
    {
        return m_totalCapacityInBytes;
    }




    private final BeansServiceManager m_serviceManager;
    private final BlobCacheService m_blobCacheService;

    private CacheFilesystem m_filesystem;
    private final AsyncBlobCacheDeleter m_asyncBlobCacheDeleter;
    private final long m_filesystemOverheadPerBlob;

    private volatile long m_totalCapacityInBytes;
    private AtomicLong m_bytesAllocated;

    private final Set< CacheListener > m_listeners = new HashSet<>();

    private final CacheSpaceReclaimer m_cacheSpaceReclaimer;
    private final PeriodicCacheReclaimer m_periodicCacheReclaimWorker = new PeriodicCacheReclaimer();
    private final ThrottledRunnableExecutor<PeriodicCacheReclaimer> m_periodicCacheReclaimExecutor =
            new ThrottledRunnableExecutor<>( 1, null, ThrottledRunnableExecutor.WhenAggregating.DELAY_EXECUTION );

    private final CacheThrottleManager m_cacheThrottleManager;

    private final static Logger LOG = Logger.getLogger( CacheManagerImpl.class );
}
