/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.*;

import com.google.common.collect.Lists;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.S3ObjectsCachedNotificationPayloadGenerator;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ChunkReadingTask;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.render.BytesRenderer;

public final class ReadChunkFromTapeTask extends BaseIoTask implements ChunkReadingTask, StaticTapeTask
{
    public ReadChunkFromTapeTask(final BlobStoreTaskPriority priority,
                                 final List<JobEntry> chunks,
                                 final DiskManager diskManager,
                                 final JobProgressManager jobProgressManager,
                                 final TapeFailureManagement tapeFailureManagement,
                                 final BeansServiceManager serviceManager )
    {
        this(
                new ReadDirective( priority, chunks.iterator().next().getReadFromTapeId(), PersistenceType.TAPE, chunks ),
                diskManager,
                jobProgressManager,
                tapeFailureManagement,
                serviceManager );
    }


    public ReadChunkFromTapeTask(final ReadDirective readDirective,
                                 final DiskManager diskManager,
                                 final JobProgressManager jobProgressManager,
                                 final TapeFailureManagement tapeFailureManagement,
                                 final BeansServiceManager serviceManager )
    {
        super( readDirective.getPriority(), readDirective.getReadSourceId(), diskManager, jobProgressManager, tapeFailureManagement, serviceManager);
        //NOTE: we need to be sure we have a mutable set so we can remove entries in the event of partial success
        m_remainingEntries = Lists.newArrayList( readDirective.getEntries() );
    }


    public JobEntry getEntry()
    {
        throw new IllegalStateException( "Not implemented for this task type" );
    }


    public List<JobEntry> getEntries()
    {
        return m_remainingEntries;
    }


    @Override
    protected void performPreRunValidations()
    {
        final boolean verifyQuiescedToCheckpointOnRead = m_serviceManager.getRetriever( DataPathBackend.class )
                .attain( Require.nothing() ).isVerifyCheckpointBeforeRead();
        verifyTapeInDrive( new DefaultTapeInDriveVerifier(this, false, verifyQuiescedToCheckpointOnRead ) );
    }


    @Override
    protected BlobStoreTaskState runInternal()
    {
        final Set<UUID> remainingEntryIds = BeanUtils.extractPropertyValues(m_remainingEntries, Identifiable.ID );
        //remove entries that are no longer in DB:
        final Map<UUID, JobEntry> jobEntries = getServiceManager().getRetriever(JobEntry.class)
                .retrieveAll(Require.beanPropertyEqualsOneOf(Identifiable.ID, remainingEntryIds)).toMap();
        m_remainingEntries.removeIf(e -> !jobEntries.containsKey(e.getId()));

    	if ( m_remainingEntries.isEmpty() )
		{
    		LOG.info( "No job chunks to read, no work to perform." );
    		return BlobStoreTaskState.COMPLETED;
		}

        final Set< UUID > blobsToRead = BeanUtils.extractPropertyValues(jobEntries.values(), JobEntry.BLOB_ID );
        synchronized ( BLOB_READS_IN_PROGRESS )
        {
            int numberOfBlobsAlreadyInCache = 0;
            for ( final JobEntry e : new HashSet<>( jobEntries.values() ) )
            {
                if ( m_diskManager.isInCache( e.getBlobId() ) )
                {
                    ++numberOfBlobsAlreadyInCache;
                    jobEntries.remove( e.getId() );
                }
            }
            if ( 0 < numberOfBlobsAlreadyInCache )
            {
                LOG.info( numberOfBlobsAlreadyInCache
                          + " blobs that needed to be read from tape were already in cache, "
                          + "so there's no longer a need to read them from tape." );
            }
            for ( final UUID id : blobsToRead )
            {
                if ( BLOB_READS_IN_PROGRESS.contains( id ) )
                {
                    LOG.info( "This read task must be retried later since blob " + id
                              + " is actively being read into cache from another task." );
                    return BlobStoreTaskState.READY;
                }
            }
            if ( blobsToRead.isEmpty() )
            {
                markChunkAsCompleted();
                return BlobStoreTaskState.COMPLETED;
            }
            BLOB_READS_IN_PROGRESS.addAll( blobsToRead );
        }

        try
        {
            final BlobStoreTaskState retval = jobEntries.isEmpty()
                    ? BlobStoreTaskState.COMPLETED
                    : performRead(jobEntries);
            if ( retval == BlobStoreTaskState.COMPLETED ) {
                markChunkAsCompleted();
            }
            return retval;
        }
        finally
        {
            synchronized ( BLOB_READS_IN_PROGRESS )
            {
                BLOB_READS_IN_PROGRESS.removeAll( blobsToRead );
            }
        }
    }


    private BlobStoreTaskState performRead(
            final Map< UUID, JobEntry> jobEntries )
    {
        final S3ObjectsIoRequest objects = constructObjectsIoRequestFromJobEntries( JobRequestType.GET, new HashSet<>( jobEntries.values() ) );
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final long bytesToRead = getTotalWorkInBytes( objects );
        final Duration duration = new Duration();
        final String dataDescription =
                jobEntries.size() + " entries (" + bytesRenderer.render( bytesToRead ) + ")";
        LOG.info( "Will read " + dataDescription + "..." );

        final BlobIoFailures failures;
        try
        {
            failures = getDriveResource().readData( objects ).get( Timeout.VERY_LONG );
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.READ_FAILED);
        }
        catch ( final RpcException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    TapeFailureType.READ_FAILED,
                    ex );
            throw ex;
        }
        LOG.info( dataDescription + " read from tape at "
                  + bytesRenderer.render( bytesToRead, duration ) + "." );

        final Set< UUID > failedBlobIds = new HashSet<>();
        if ( 0 != failures.getFailures().length )
        {
            final Map< BlobIoFailureType, List< UUID > > failuresByCause = new HashMap<>();
            final Map< UUID, JobEntry> blobIdToJobEntryMap = new HashMap<>();
            for ( final JobEntry e : jobEntries.values() )
            {
                blobIdToJobEntryMap.put( e.getBlobId(), e );
            }
            for ( final BlobIoFailure failure : failures.getFailures() )
            {
                final UUID blobId = failure.getBlobId();
                final UUID jobEntryId = blobIdToJobEntryMap.get( blobId ).getId();
                failedBlobIds.add( blobId );
                jobEntries.remove( jobEntryId );

                if ( !failuresByCause.containsKey( failure.getFailure() ) )
                {
                    failuresByCause.put( failure.getFailure(), new ArrayList<>() );
                }
                failuresByCause.get( failure.getFailure() ).add( blobId );
            }

            final RuntimeException readFailuresEx = new RuntimeException(
                    "Failed to read " + failedBlobIds.size() + " blobs from tape due to "
                    + failuresByCause.keySet() + ": "
                    + LogUtil.getShortVersion( failuresByCause.toString() ) );
            LOG.warn( "Failed to read " + failedBlobIds.size() + " blobs from tape.", readFailuresEx );
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    TapeFailureType.BLOB_READ_FAILED,
                    readFailuresEx );
        } else {
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.BLOB_READ_FAILED);
        }

        for ( final JobEntry entry : jobEntries.values() )
        {
            m_diskManager.blobLoadedToCache( entry.getBlobId() );
        }

        if ( failures.getFailures().length != 0 )
        {
            m_drivesFailedOn.add(getDriveId());
            if ( ++m_failureCount < MAX_FAILURES && m_drivesFailedOn.size() < MAX_DRIVES_TO_FAIL_ON )
            {
                return BlobStoreTaskState.READY;
            }
            final BeansServiceManager transaction = getServiceManager().startTransaction();
            try
            {
                transaction.getService( BlobTapeService.class ).blobsSuspect(
                        "failures reading blobs",
                        getServiceManager().getRetriever( BlobTape.class ).retrieveAll( Require.all(
                                Require.beanPropertyEquals( BlobTape.TAPE_ID, getTapeId() ),
                                Require.beanPropertyEqualsOneOf(
                                        BlobObservable.BLOB_ID, failedBlobIds ) ) ).toSet() );
                transaction.commitTransaction();
                m_tapeFailureManagement.resetBlobReadFailuresWhenBlobMarkedSuspect( getTapeId() );
            }
            finally
            {
                transaction.closeTransaction();
            }
        }

        final JobEntryService entryService = getServiceManager().getService( JobEntryService.class );
        final Map<UUID, JobEntry> failedEntriesById = entryService.retrieveAll(
                Require.all(
                        Require.beanPropertyEqualsOneOf(Identifiable.ID, getChunkIds()),
                        Require.beanPropertyEqualsOneOf(JobEntry.BLOB_ID, failedBlobIds)
                )).toMap();
        for (JobEntry entry : failedEntriesById.values()) {
            LOG.info( "Job entry " + entry + " failed to read from tape. Marking it as requiring re-read from other source." );
            entryService.update( entry.setReadFromTapeId(null), JobEntry.READ_FROM_TAPE_ID );
        }
        m_remainingEntries.removeIf(e -> failedEntriesById.keySet().contains(e.getId()));
        return BlobStoreTaskState.COMPLETED;
    }





    public void prepareForExecutionIfPossible(
            final TapeDriveResource tapeDriveResource,
            final TapeAvailability tapeAvailability )
    {
        try {
            allocateCacheSpace();
        } catch ( final FailureTypeObservableException ex ) {
            if ( GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT.getHttpResponseCode()
                    == ex.getFailureType().getHttpResponseCode() )
            {
                //If cache is not available we fail to prepare for execution, but we do not invalidate the task.
                throw new RuntimeException("Cache space cannot be allocated to " + m_remainingEntries.size() + " read chunks at this time.", ex);
            }
            else
            {
                invalidateTaskAndThrow( ex );
            }
        } catch ( final RuntimeException e ) {
            invalidateTaskAndThrow( e );
        }
        super.prepareForExecutionIfPossible(tapeDriveResource, tapeAvailability);
    }

    private void allocateCacheSpace()
    {
        for ( final JobEntry chunk : m_remainingEntries)
        {
            getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId());
        }
        m_diskManager.allocateChunks( BeanUtils.toMap(m_remainingEntries).keySet() );

        for ( final JobEntry e : m_remainingEntries)
        {
            if ( !m_diskManager.isInCache( e.getBlobId() ) )
            {
                return;
            }
        }
        markChunkAsCompleted();
        throw new IllegalStateException(
                "All blobs that needs to be read for " + m_remainingEntries.size() + " chunk(s) are already in cache." );
    }


    public String getDescription()
    {
        return "Read " + m_remainingEntries.size() + " Chunks";
    }


    private void markChunkAsCompleted()
    {
        final Set<UUID> entryIds = BeanUtils.extractPropertyValues(m_remainingEntries, Identifiable.ID);
        try (final NestableTransaction transaction = getServiceManager().startNestableTransaction()) {

            transaction.getService(JobEntryService.class).update(Require.beanPropertyEqualsOneOf(Identifiable.ID, entryIds),
                    (entry) -> entry.setBlobStoreState(JobChunkBlobStoreState.COMPLETED),
                    JobEntry.BLOB_STORE_STATE);
            m_jobProgressManager.entriesLoadedToCache(transaction, m_remainingEntries);
            transaction.commitTransaction();
        }

    }


    @Override
    public Collection<UUID> getChunkIds() {
        return BeanUtils.extractPropertyValues(getEntries(), Identifiable.ID);
    }


    @Override
    public boolean allowMultiplePerTape() {
        return true;
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_remainingEntries, JobEntry.JOB_ID).toArray(new UUID[0]);
    }

    // EMPROD-1774
    private int m_failureCount = 0;
    private Set<UUID> m_drivesFailedOn = new HashSet<>();
    private final List<JobEntry> m_remainingEntries;
    private final static Set< UUID > BLOB_READS_IN_PROGRESS = new HashSet<>();
    private static int MAX_FAILURES = 4;
    private static int MAX_DRIVES_TO_FAIL_ON = 3;
}
