/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.orm.JobRM;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.Ds3TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.S3ObjectsCachedNotificationPayloadGenerator;
import com.spectralogic.s3.common.platform.replicationtarget.TargetInitializationUtil;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.render.BytesRenderer;

public final class ReadChunkFromDs3TargetTask
    extends BaseTargetTask< Ds3Target, Ds3ConnectionFactory >
{
    public ReadChunkFromDs3TargetTask(final JobProgressManager jobProgressManager, final DiskManager diskManager, final BeansServiceManager serviceManager, final Ds3ConnectionFactory connectionFactory, final ReadDirective readDirective)
    {
        super( Ds3Target.class,
                readDirective.getReadSourceId(),
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory,
                readDirective.getPriority() );
        m_readDirective = readDirective;
        m_remainingEntries = BeanUtils.toMap( m_readDirective.getEntries() );
    }

    @Override
    protected boolean prepareForExecution()
    {
        m_serviceManager.getService(JobEntryService.class).verifyEntriesExist(BeanUtils.extractPropertyValues(m_readDirective.getEntries(), Identifiable.ID));
        final Ds3Target target = getTarget();
        final Job job = getServiceManager().getRetriever( Job.class ).attain( m_readDirective.getEntries().iterator().next().getJobId() );

        final Set<UUID> blobIds = BeanUtils.extractPropertyValues(m_readDirective.getEntries(), BlobObservable.BLOB_ID);
        synchronized (DS3_BLOB_READS_IN_PROGRESS) {
            for (final UUID blobId : blobIds) {
                if (DS3_BLOB_READS_IN_PROGRESS.contains(blobId)) {
                    LOG.info("Will wait to execute DS3 read task since blob " + blobId + " is already being read by another task.");
                    return false;
                }
            }
        }

        try
        {
            final Ds3Connection connection = getConnectionFactory().connect( job.getUserId(), target );
            try
            {
                m_replicationWork = getReplicationWork( job.getId(), connection );
                if (!m_replicationWork.m_remainingEntries.isEmpty()) {
                    final List<UUID> readyBlobIds = connection.getBlobsReady(job.getId());
                    final Set<UUID> readyBlobIdSet = new HashSet<>(readyBlobIds);// Filter to only entries whose blobs are ready
                    final Map<UUID, JobEntry> readyEntries = new HashMap<>();
                    for (final JobEntry entry : m_replicationWork.m_remainingEntries.values()) {
                        if (readyBlobIdSet.contains(entry.getBlobId())) {
                            readyEntries.put(entry.getId(), entry);
                        }
                    }// Update replication work with only ready entries
                    m_replicationWork = new ReplicationWork(
                            m_replicationWork.m_jobExists,
                            m_replicationWork.m_remainingEntries,
                            readyEntries);
                }

            }
            finally
            {
                connection.shutdown();
            }
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Not ready to replicate to target.", ex );
            m_serviceManager.getService( Ds3TargetFailureService.class ).create(
                    target.getId(),
                    TargetFailureType.WRITE_INITIATE_FAILED,
                    ex,
                    Integer.valueOf( 10 ) );
            return false;
        }
        return true;
    }


    private ReplicationWork getReplicationWork(final UUID jobId, final Ds3Connection connection )
    {
        final Map< UUID, JobEntry> entriesToReplicate = BeanUtils.toMap(m_readDirective.getEntries());
        final Set<UUID> blobIds = BeanUtils.extractPropertyValues(entriesToReplicate.values(), BlobObservable.BLOB_ID );

        final Set<UUID> blobsAlreadyReceived = checkForBlobsAlreadyInCache( blobIds );

        final Map<UUID, JobEntry> entriesAlreadyComplete = new HashMap<>();
        for ( final JobEntry e : entriesToReplicate.values() )
        {
            if ( blobsAlreadyReceived.contains( e.getBlobId() ) )
            {
                entriesAlreadyComplete.put( e.getId(), e );
            }
        }
        markEntriesComplete(entriesAlreadyComplete);
        entriesToReplicate.keySet().removeAll(entriesAlreadyComplete.keySet());
        if (!entriesToReplicate.isEmpty()) {
            final Job job = getServiceManager().getRetriever(Job.class).attain(m_readDirective.getEntries().iterator().next().getJobId());
            final String bucketName = new JobRM(job, m_serviceManager).getBucket().getName();
            if (m_remoteJobId == null) {
                if (connection.isJobExistant(job.getId())) {
                    m_remoteJobId = job.getId();
                    LOG.info("Get job " + m_remoteJobId + " already exists on target " + getTargetId() + ".");
                } else {
                    try {
                        m_remoteJobId = connection.createGetJob(job, entriesToReplicate.values(), bucketName);
                        LOG.info("Created get job " + m_remoteJobId + " on target " + getTargetId() + ".");
                    } catch (final Exception e) {
                        final String msg = "Failed to create job on target " + getTargetId() + ": " + e.getMessage();
                        LOG.warn(msg);
                        getServiceManager().getService(Ds3TargetFailureService.class).create(
                                getTargetId(),
                                TargetFailureType.READ_INITIATE_FAILED,
                                msg,
                                null);
                    }
                }
            }
        }
        return new ReplicationWork( m_remoteJobId != null, entriesToReplicate, Collections.emptyMap() );
    }

    private Set<UUID> checkForBlobsAlreadyInCache(Set<UUID> blobIds) {
        return blobIds.stream()
                .filter( blobId -> getDiskManager().isInCache( blobId ) )
                .collect( Collectors.toSet() );
    }


    protected BlobStoreTaskState runInternal()
    {
        final Job job = getServiceManager().getRetriever( Job.class ).attain( m_readDirective.getEntries().iterator().next().getJobId() );
        final Ds3Connection connection = getConnectionFactory().connect( job.getUserId(), getTarget() );
        try
        {
            if (m_replicationWork.workRemains()) {
                if (m_replicationWork.hasReadyEntries()) {
                    retrieveData( m_replicationWork.m_readyEntries, job, connection );
                } else {
                    requireTaskSuspension();
                }
            }
            if (m_replicationWork.workRemains()) {
                LOG.info( "Must replicate " + m_replicationWork.m_remainingEntries.size() + " more entries." );
                return BlobStoreTaskState.READY;
            } else {
                LOG.info( "All entries replicated to target." );
                return BlobStoreTaskState.COMPLETED;
            }
        }
        catch ( final RuntimeException ex )
        {
            requireTaskSuspension();
            handleFailure( ex );
            throw ExceptionUtil.toRuntimeException( ex );
        }
        finally
        {
            connection.shutdown();
        }
    }


    private void retrieveData(
            final Map< UUID, JobEntry> jobEntries,
            final Job job,
            final Ds3Connection connection )
    {
        final Map< UUID, Blob > blobs = BeanUtils.toMap(
                getServiceManager().getRetriever( Blob.class ).retrieveAll(
                        BeanUtils.< UUID >extractPropertyValues(
                                jobEntries.values(), BlobObservable.BLOB_ID ) ).toSet() );
        final Map< UUID, S3Object > objects = BeanUtils.toMap(
                getServiceManager().getRetriever( S3Object.class ).retrieveAll(
                                BeanUtils.< UUID >extractPropertyValues( blobs.values(), Blob.OBJECT_ID ) )
                        .toSet() );
        final Bucket bucket = getServiceManager().getRetriever( Bucket.class ).attain(
                objects.values().iterator().next().getBucketId() );

        // Calculate actual size being read (only for entries we're reading)
        long sizeBeingRead = 0;
        for ( final Blob blob : blobs.values() )
        {
            sizeBeingRead += blob.getLength();
        }

        final BytesRenderer bytesRenderer = new BytesRenderer();
        final String dataDescription =
                jobEntries.size() + " blobs (" + bytesRenderer.render( sizeBeingRead ) + ")";
        LOG.info( "Will read " + dataDescription + "..." );

        final Set< UUID > blobsToRead;
        synchronized ( DS3_BLOB_READS_IN_PROGRESS )
        {
            blobsToRead = blobs.keySet();
            for ( final UUID id : blobsToRead )
            {
                if ( DS3_BLOB_READS_IN_PROGRESS.contains( id ) )
                {
                    LOG.info( "This DS3 read task must be retried later since blob " + id
                            + " is actively being read from DS3 by another task." );
                    requireTaskSuspension();
                    return;
                }
            }
            DS3_BLOB_READS_IN_PROGRESS.addAll( blobsToRead );
        }
        try {
            final Duration duration = new Duration();
            for ( final JobEntry jobEntry : jobEntries.values() )
            {
                final Blob blob = blobs.get( jobEntry.getBlobId() );
                final S3Object object = objects.get( blob.getObjectId() );
                connection.getBlob(
                        job.getId(),
                        bucket.getName(),
                        object.getName(),
                        blob,
                        new File( getDiskManager().allocateChunksForBlob( blob.getId() ) ) );
                getDiskManager().blobLoadedToCache( blob.getId() );
                m_replicationWork.m_remainingEntries.remove(jobEntry.getId());
            }

            LOG.info( dataDescription + " read from target at "
                    + bytesRenderer.render( sizeBeingRead, duration ) + "." );

            markEntriesComplete(jobEntries);
            doNotTreatReadyReturnValueAsFailure();
        } finally {
            synchronized ( DS3_BLOB_READS_IN_PROGRESS )
            {
                DS3_BLOB_READS_IN_PROGRESS.removeAll( blobsToRead );
            }
        }
    }


    private void markEntriesComplete(Map<UUID, JobEntry> jobEntries) {
        try ( final NestableTransaction transaction = m_serviceManager.startNestableTransaction() )
        {
            final Collection< UUID > blobIds = jobEntries.values().stream()
                    .map( e -> e.getBlobId() )
                    .collect( Collectors.toSet() );
            final JobEntryService service = transaction.getService( JobEntryService.class );
            service.update(
                    Require.all(
                            Require.beanPropertyEqualsOneOf( JobEntry.ID, jobEntries.keySet() )
                    ),
                    (x) -> x.setBlobStoreState(JobChunkBlobStoreState.COMPLETED ),
                    RemoteBlobDestination.BLOB_STORE_STATE );
            transaction.commitTransaction();
        }
    }


    private void handleFailure(final Exception ex)
    {
        getServiceManager().getService( Ds3TargetFailureService.class ).create(
                getTargetId(), TargetFailureType.READ_FAILED, ex, null );
    }


    public List<JobEntry> getEntries()
    {
        return m_readDirective.getEntries();
    }


    public String getDescription() {
        return "Read " + m_readDirective.getEntries().size() + " chunks from target " + getTargetId() + ".";
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_readDirective.getEntries(), JobEntry.JOB_ID).toArray(new UUID[0]);
    }

    private void dispatchNotifications() {
        final Map<UUID, List<JobEntry>> remainingEntriesByJobId = new HashMap<>();
        for (final JobEntry entry : getEntries()) {
            final UUID jobId = entry.getJobId();
            if (!remainingEntriesByJobId.containsKey(jobId)) {
                remainingEntriesByJobId.put(jobId, new ArrayList<>());
            }
            remainingEntriesByJobId.get(jobId).add(entry);
        }
        for (final UUID jobId : remainingEntriesByJobId.keySet()) {
            getServiceManager().getNotificationEventDispatcher().fire(new JobNotificationEvent(
                    getServiceManager().getRetriever(Job.class).attain(jobId),
                    getServiceManager().getRetriever(S3ObjectCachedNotificationRegistration.class),
                    new S3ObjectsCachedNotificationPayloadGenerator(
                            jobId,
                            remainingEntriesByJobId.get(jobId),
                            getServiceManager().getRetriever(S3Object.class),
                            getServiceManager().getRetriever(Blob.class))));
        }
    }


    private final static class ReplicationWork
    {
        private ReplicationWork(
                final boolean jobExists,
                final Map< UUID, JobEntry> remainingEntries,
                final Map< UUID, JobEntry> readyEntries )
        {
            m_jobExists = jobExists;
            m_remainingEntries = remainingEntries;
            m_readyEntries = readyEntries;
        }

        private boolean workRemains() {
            return !m_remainingEntries.isEmpty();
        }

        private boolean hasReadyEntries() {
            return !m_readyEntries.isEmpty();
        }


        private final boolean m_jobExists;
        // Remaining entries that still need to be replicated (entries by entry ID)
        private final Map< UUID, JobEntry> m_remainingEntries;
        // Subset of remaining entries that are ready to write now (entries by entry ID)
        private final Map< UUID, JobEntry> m_readyEntries;
    } // end inner class def


    private volatile ReplicationWork m_replicationWork;
    private final ReadDirective m_readDirective;
    private final Map<UUID, JobEntry> m_remainingEntries;
    private final static Set< UUID > DS3_BLOB_READS_IN_PROGRESS = new HashSet<>();
    private UUID m_remoteJobId = null;
}
