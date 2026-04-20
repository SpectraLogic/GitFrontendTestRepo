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
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.LocalJobEntryWork;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.LocalJobEntryWorkService;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ChunkReadingTask;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.render.BytesRenderer;

public final class VerifyChunkOnTapeTask extends BaseIoTask implements ChunkReadingTask, StaticTapeTask
{
    public VerifyChunkOnTapeTask(final BlobStoreTaskPriority priority,
                                 final List<JobEntry> chunks,
                                 final DiskManager diskManager,
                                 final JobProgressManager jobProgressManager,
                                 final TapeFailureManagement tapeFailureManagement,
                                 final BeansServiceManager serviceManager)
    {
        this(
                new ReadDirective( priority, chunks.iterator().next().getReadFromTapeId(), PersistenceType.TAPE, chunks ),
                diskManager,
                jobProgressManager,
                tapeFailureManagement,
                serviceManager );
    }


    public VerifyChunkOnTapeTask(final ReadDirective readDirective,
                                 final DiskManager diskManager,
                                 final JobProgressManager jobProgressManager,
                                 final TapeFailureManagement tapeFailureManagement,
                                 final BeansServiceManager serviceManager)
    {
        super( readDirective.getPriority(), readDirective.getReadSourceId(), diskManager, jobProgressManager, tapeFailureManagement, serviceManager );
        //NOTE: we need to be sure we have a mutable set so we can remove entries in the event of partial success
        m_readDirective = readDirective;
        m_remainingEntries = Lists.newArrayList( readDirective.getEntries() );
    }
    
    
    public List<JobEntry> getEntries()
    {
    	return m_remainingEntries;
    }


    @Override
    public Collection<UUID> getChunkIds() {
        return BeanUtils.extractPropertyValues(getEntries(), Identifiable.ID);
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
            LOG.info( "No job chunks to verify, no work to perform." );
            return BlobStoreTaskState.COMPLETED;
        }

        final Set< UUID > blobsToRead = 
                BeanUtils.extractPropertyValues( m_remainingEntries, BlobObservable.BLOB_ID );
        if ( blobsToRead.isEmpty() )
        {
            return BlobStoreTaskState.COMPLETED;
        }
        
        final S3ObjectsIoRequest objects =
                constructObjectsIoRequestFromJobEntries( JobRequestType.VERIFY, new HashSet<>( m_remainingEntries ) );
        return performVerify( BeanUtils.toMap(m_remainingEntries), objects );
    }
    
    
    private BlobStoreTaskState performVerify(
            final Map< UUID, JobEntry> jobEntries,
            final S3ObjectsIoRequest objects )
    {
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final long bytesToRead = getTotalWorkInBytes( objects );
        final Duration duration = new Duration();
        final String dataDescription = 
                jobEntries.size() + " blobs (" + bytesRenderer.render( bytesToRead ) + ")";
        LOG.info( "Will verify " + dataDescription + "..." );
        
        final BlobIoFailures failures;
        try
        {
            failures = getDriveResource().verifyData(
                    TapeTaskUtils.buildVerifyObjectsPayload( objects ) ).get( Timeout.VERY_LONG );

            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.VERIFY_FAILED);
        }
        catch ( final RpcException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    TapeFailureType.VERIFY_FAILED, ex );
            throw ex;
        }
        LOG.info( dataDescription + " verified on tape at " 
                  + bytesRenderer.render( bytesToRead, duration ) + "." );

        final Set< UUID > failedBlobIds = new HashSet<>();
        final Map< UUID, JobEntry> blobIdToJobChunkMap = new HashMap<>();
        for ( final JobEntry e : jobEntries.values() )
        {
            blobIdToJobChunkMap.put( e.getBlobId(), e );
        }
        if ( 0 != failures.getFailures().length )
        {
            final Map< BlobIoFailureType, List< UUID > > failuresByCause = new HashMap<>();
            for ( final BlobIoFailure failure : failures.getFailures() )
            {
                final UUID blobId = failure.getBlobId();
                final UUID jobEntryId = blobIdToJobChunkMap.get( blobId ).getId();
                failedBlobIds.add( blobId );
                jobEntries.remove( jobEntryId );
                
                if ( !failuresByCause.containsKey( failure.getFailure() ) )
                {
                    failuresByCause.put( failure.getFailure(), new ArrayList<>() );
                }
                failuresByCause.get( failure.getFailure() ).add( blobId );
            }

            final RuntimeException readFailuresEx = new RuntimeException(  
                    "Failed to verify " + failedBlobIds.size() + " blobs on tape due to " 
                    + failuresByCause.keySet() + ": " 
                    + LogUtil.getShortVersion( failuresByCause.toString() ) );
            LOG.warn( "Failed to verify " + failedBlobIds.size() + " blobs from tape.", readFailuresEx );
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
        for ( final Blob b : getServiceManager().getRetriever( Blob.class ).retrieveAll( 
                BeanUtils.extractPropertyValues(
                        jobEntries.values(), BlobObservable.BLOB_ID ) ).toSet() )
        {
            final JobEntry chunk = blobIdToJobChunkMap.get( b.getId() );
            m_jobProgressManager.workCompleted( chunk.getJobId(), b.getLength() );
        }
        //NOTE: we immediately delete the entries here instead of just marking them complete because they do
        //not need to be read by the client. Once they're verified they're completely done.
        getServiceManager().getService( JobEntryService.class ).delete( jobEntries.keySet() );
        m_remainingEntries.removeAll(jobEntries.values());
        if ( 0 != failures.getFailures().length )
        {
            m_drivesFailedOn.add(getDriveId());
            if ( ++m_failureCount < MAX_FAILURES && m_drivesFailedOn.size() < MAX_DRIVES_TO_FAIL_ON )
            {
                doNotTreatReadyReturnValueAsFailure();
                return BlobStoreTaskState.READY;
            }
            final BeansServiceManager transaction = getServiceManager().startTransaction();
            try
            {
                transaction.getService( BlobTapeService.class ).blobsSuspect(
                        "failures verifying blobs", 
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
            LOG.info( "Job entry " + entry + " failed to verify from tape. Marking it as requiring re-read from other source." );
            entryService.update( entry.setReadFromTapeId(null), JobEntry.READ_FROM_TAPE_ID );
        }
        m_remainingEntries.removeIf(e -> failedEntriesById.keySet().contains(e.getId()));
        return BlobStoreTaskState.COMPLETED;
    }

    @Override
    public boolean allowMultiplePerTape() {
        return true;
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_remainingEntries, JobEntry.JOB_ID).toArray(new UUID[0]);
    }


    public String getDescription()
    {
        return "Verify Chunks [" + BeanUtils.extractPropertyValues( m_remainingEntries, Identifiable.ID ) + "]";
    }


    private final ReadDirective m_readDirective;
    private final List<JobEntry> m_remainingEntries;
    private int m_failureCount = 0;
    private Set<UUID> m_drivesFailedOn = new HashSet<>();
    private static int MAX_DRIVES_TO_FAIL_ON = 3;
    private static int MAX_FAILURES = 4;
}
