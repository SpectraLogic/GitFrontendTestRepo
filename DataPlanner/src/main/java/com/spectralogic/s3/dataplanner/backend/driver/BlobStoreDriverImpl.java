/*
 *
 * Copyright C 2019, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.driver;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.dataplanner.backend.api.*;
import com.spectralogic.s3.common.platform.persistencetarget.BlobDestinationUtils;
import com.spectralogic.s3.dataplanner.frontend.dataorder.Ds3TargetBlobPhysicalPlacementImpl;
import com.spectralogic.s3.dataplanner.frontend.dataorder.GetByPhysicalPlacementDataOrderingStrategy;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.orm.JobRM;
import com.spectralogic.s3.common.platform.cache.CacheListener;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class BlobStoreDriverImpl extends BaseShutdownable implements BlobStoreDriver, CacheListener
{
    public BlobStoreDriverImpl( 
            final TapeBlobStore tapeBlobStore,
            final PoolBlobStore poolBlobStore,
            final TargetBlobStore ds3TargetBlobStore,
            final TargetBlobStore azureTargetBlobStore,
            final TargetBlobStore s3TargetBlobStore,
            final BeansServiceManager serviceManager,
            final DiskManager diskManager,
            final JobCreator jobCreator,
            final JobProgressManager jobProgressManager,
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final int intervalInMillisToCheckForNewActivity )
    {
        Validations.verifyNotNull( "Tape blob store", tapeBlobStore );
        Validations.verifyNotNull( "Pool blob store", poolBlobStore );
        Validations.verifyNotNull( "DS3 target blob store", ds3TargetBlobStore );
        Validations.verifyNotNull( "Azure target blob store", azureTargetBlobStore );
        Validations.verifyNotNull( "S3 target blob store", s3TargetBlobStore );
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Cache manager", diskManager );
        Validations.verifyNotNull( "Job creator", jobCreator );
        Validations.verifyNotNull( "Job progress manager", jobProgressManager );
        Validations.verifyNotNull( "DS3 connection factory", ds3ConnectionFactory );
        m_tapeBlobStore = tapeBlobStore;
        m_poolBlobStore = poolBlobStore;
        m_ds3TargetBlobStore = ds3TargetBlobStore;
        m_azureTargetBlobStore = azureTargetBlobStore;
        m_s3TargetBlobStore = s3TargetBlobStore;
        m_serviceManager = serviceManager;
        m_diskManager = diskManager;
        m_jobProgressManager = jobProgressManager;
        m_ds3ConnectionFactory = ds3ConnectionFactory;
        m_jobReshapingLock = jobCreator.getJobReshapingLock();

        LOG.info( getClass().getSimpleName() + " is starting up..." );
        m_serviceManager.getService( JobService.class ).cleanUpCompletedJobsAndJobChunks( 
                m_jobProgressManager,
                m_tapeBlobStore,
                m_jobReshapingLock );
        processChunksRequiringRechunking();
        resetEntriesThatWereBeingWorkedOn();
        
        m_executor = new RecurringRunnableExecutor(
                new BlobStoreDriverWorker(),
                intervalInMillisToCheckForNewActivity );
        m_executor.start();
        addShutdownListener( m_executor );
        diskManager.registerCacheListener( this );
        
        LOG.info( getClass().getSimpleName() + " is online and ready." );
    }
    
    
    private final class BlobStoreDriverWorker implements Runnable
    {
        public void run()
        {
            cleanUpAndProcessChunks();
        }
    } // end inner class def
    
    
    private void resetEntriesThatWereBeingWorkedOn()
    {
        final MonitoredWork work = new MonitoredWork( 
                StackTraceLogging.SHORT, "Re-schedule chunks that were previously running" );
        m_serviceManager.getService( JobEntryService.class ).update(
                Require.beanPropertyEquals(
                        JobEntry.BLOB_STORE_STATE,
                        JobChunkBlobStoreState.IN_PROGRESS ),
                (entry) -> entry.setBlobStoreState( JobChunkBlobStoreState.PENDING ),
                JobEntry.BLOB_STORE_STATE );
        m_serviceManager.getService( LocalBlobDestinationService.class ).update(
                Require.beanPropertyEquals(
                        LocalBlobDestination.BLOB_STORE_STATE,
                        JobChunkBlobStoreState.IN_PROGRESS ),
                (destination) -> destination.setBlobStoreState( JobChunkBlobStoreState.PENDING ),
                LocalBlobDestination.BLOB_STORE_STATE );
        m_serviceManager.getService( AzureBlobDestinationService.class ).update(
                Require.beanPropertyEquals(
                        RemoteBlobDestination.BLOB_STORE_STATE,
                        JobChunkBlobStoreState.IN_PROGRESS ),
                (destination) -> destination.setBlobStoreState( JobChunkBlobStoreState.PENDING ),
                RemoteBlobDestination.BLOB_STORE_STATE );
        m_serviceManager.getService( S3BlobDestinationService.class ).update(
                Require.beanPropertyEquals(
                        RemoteBlobDestination.BLOB_STORE_STATE,
                        JobChunkBlobStoreState.IN_PROGRESS ),
                (destination) -> destination.setBlobStoreState( JobChunkBlobStoreState.PENDING ),
                RemoteBlobDestination.BLOB_STORE_STATE );
        m_serviceManager.getService( Ds3BlobDestinationService.class ).update(
                Require.beanPropertyEquals(
                        RemoteBlobDestination.BLOB_STORE_STATE,
                        JobChunkBlobStoreState.IN_PROGRESS ),
                (destination) -> destination.setBlobStoreState( JobChunkBlobStoreState.PENDING ),
                RemoteBlobDestination.BLOB_STORE_STATE );
        work.completed();
    }


    void cleanUpAndProcessChunks()
    {
        BlobDestinationUtils.cleanupCompletedEntriesAndDestinations( m_serviceManager, m_jobProgressManager);
        m_serviceManager.getService( JobService.class ).cleanUpCompletedJobsAndJobChunks(
                m_jobProgressManager,
                m_tapeBlobStore,
                m_jobReshapingLock );
        synchronized ( m_jobChunkProcessingLock )
        {
            processChunksRequiringRechunking();
        }
        try (final NestableTransaction transaction = m_serviceManager.startNestableTransaction()) {
            transaction.getService(JobService.class).closeOldAggregatingJobs(30);
            transaction.commitTransaction();
        }
    }


    private void processChunksRequiringRechunking()
    {
        final RetrieveBeansResult<JobEntry> retrieveBeansResult =
                m_serviceManager.getRetriever(JobEntry.class).retrieveAll(Require.all(
                        Require.beanPropertyEquals(
                                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.IN_PROGRESS),
                        Require.exists(
                                JobEntry.JOB_ID,
                                Require.beanPropertyEqualsOneOf(
                                        JobObservable.REQUEST_TYPE,
                                        JobRequestType.GET,
                                        JobRequestType.VERIFY)),
                        Require.beanPropertyEquals(ReadFromObservable.READ_FROM_POOL_ID, null),
                        Require.beanPropertyEquals(ReadFromObservable.READ_FROM_TAPE_ID, null),
                        Require.beanPropertyEquals(ReadFromObservable.READ_FROM_DS3_TARGET_ID, null),
                        Require.beanPropertyEquals(ReadFromObservable.READ_FROM_AZURE_TARGET_ID, null),
                        Require.beanPropertyEquals(ReadFromObservable.READ_FROM_S3_TARGET_ID, null)));
        final Map<UUID, JobRM> jobsById = new HashMap<>();
        final Map<UUID, String> userNamesByJobId = new HashMap<>();
        try (final EnhancedIterable<JobEntry> chunks = retrieveBeansResult.toIterable()) {
            for (final JobEntry chunk : chunks) {
                final BeansServiceManager transaction = m_serviceManager.startTransaction();
                try {
                    final JobRM job = jobsById.computeIfAbsent(
                            chunk.getJobId(),
                            id -> new JobRM(id, transaction));
                    final String username = userNamesByJobId.computeIfAbsent(
                            job.getId(),
                            jobId -> jobsById.get(jobId).getUser().getName());
                    new GetByPhysicalPlacementDataOrderingStrategy(
                            Map.of(chunk.getBlobId(), chunk),
                            transaction,
                            job.getRequestType(),
                            job.getChunkClientProcessingOrderGuarantee(),
                            new Ds3TargetBlobPhysicalPlacementImpl(
                                    Set.of(chunk.getBlobId()),
                                    transaction,
                                    m_ds3ConnectionFactory),
                            m_diskManager,
                            username,
                            job.getRestore() == IomType.STANDARD_IOM).setReadSources(false);
                    transaction.getUpdater(JobEntry.class).update(
                            chunk.setBlobStoreState(JobChunkBlobStoreState.PENDING),
                            JobEntry.BLOB_STORE_STATE,
                            ReadFromObservable.READ_FROM_AZURE_TARGET_ID,
                            ReadFromObservable.READ_FROM_DS3_TARGET_ID,
                            ReadFromObservable.READ_FROM_POOL_ID,
                            ReadFromObservable.READ_FROM_S3_TARGET_ID,
                            ReadFromObservable.READ_FROM_TAPE_ID);
                    transaction.commitTransaction();
                    LOG.info("Rechunked " + chunk.getId() + ".");
                } catch (final RuntimeException ex) {
                    LOG.warn("Failed to re-chunk job chunk " + chunk.getId() + ".", ex);
                    transaction.closeTransaction();
                    handleRechunkingFailure(chunk, ExceptionUtil.getReadableMessage(ex));
                } finally {
                    transaction.closeTransaction();
                }
            }
        }
    }
    
    
    private void handleRechunkingFailure(final JobEntry chunk, final String failure )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final JobEntryService service = transaction.getService( JobEntryService.class );
            final Job job = transaction.getRetriever( Job.class ).attain( chunk.getJobId() );
            transaction.getService( JobService.class ).update( 
                    job.setTruncated( true ).setErrorMessage( ( null == job.getErrorMessage() ) ? 
                            failure 
                            : job.getErrorMessage() + Platform.NEWLINE + "============" 
                              + Platform.NEWLINE + failure )
                       .setOriginalSizeInBytes(
                               job.getOriginalSizeInBytes() - service.getSizeInBytes( chunk.getId() ) ), 
                    JobObservable.TRUNCATED, JobObservable.ORIGINAL_SIZE_IN_BYTES,
                    ErrorMessageObservable.ERROR_MESSAGE );
            service.delete( chunk.getId() );
            transaction.commitTransaction();
            LOG.info( "Truncated job chunk " + chunk.getId() + "." );
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    public void blobLoadedToCache(final  UUID blobId )
	{
        m_tapeBlobStore.taskSchedulingRequired();
        m_poolBlobStore.taskSchedulingRequired();
        m_azureTargetBlobStore.taskSchedulingRequired();
        m_s3TargetBlobStore.taskSchedulingRequired();
        m_ds3TargetBlobStore.taskSchedulingRequired();
	}
    
    
    private final TapeBlobStore m_tapeBlobStore;
    private final PoolBlobStore m_poolBlobStore;
    private final TargetBlobStore m_ds3TargetBlobStore;
    private final TargetBlobStore m_azureTargetBlobStore;
    private final TargetBlobStore m_s3TargetBlobStore;
    private final BeansServiceManager m_serviceManager;
    private final DiskManager m_diskManager;
    private final JobProgressManager m_jobProgressManager;
    private final Ds3ConnectionFactory m_ds3ConnectionFactory;
    private final RecurringRunnableExecutor m_executor;
    private final Object m_jobChunkProcessingLock = new Object();
    private final Object m_jobReshapingLock;
    private final static Logger LOG = Logger.getLogger( BlobStoreDriverImpl.class );
}
