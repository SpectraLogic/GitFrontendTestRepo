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
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.ds3.Ds3BlobDestinationService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.target.BlobDs3TargetService;
import com.spectralogic.s3.common.dao.service.target.Ds3TargetFailureService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobDs3TargetService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.replicationtarget.TargetInitializationUtil;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.render.BytesRenderer;

public final class WriteChunkToDs3TargetTask extends BaseTargetTask< Ds3Target, Ds3ConnectionFactory >
{
    public WriteChunkToDs3TargetTask(
            final JobProgressManager jobProgressManager, final DiskManager diskManager, final BeansServiceManager serviceManager, final Ds3ConnectionFactory connectionFactory, final TargetWriteDirective<Ds3Target, Ds3BlobDestination> writeDirective )
    {
        super( Ds3Target.class,
                writeDirective.getTarget().getId(),
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory,
                writeDirective.getPriority() );
        m_writeDirective = writeDirective;
    }


    @Override
    protected boolean prepareForExecution()
    {
        m_serviceManager.getService(JobEntryService.class).verifyEntriesExist(BeanUtils.extractPropertyValues(m_writeDirective.getEntries(), Identifiable.ID));
        final Ds3Target target = m_writeDirective.getTarget();
        final Job job = getServiceManager().getRetriever( Job.class ).attain( m_writeDirective.getEntries().iterator().next().getJobId() );
        try
        {
            final Ds3Connection connection = getConnectionFactory().connect( job.getUserId(), target );
            try
            {
                m_replicationWork = getReplicationWork( job.getId(), connection );
                if (!m_replicationWork.m_remainingEntries.isEmpty()) {
                    if (!m_replicationWork.m_jobExists) {
                        TargetInitializationUtil.getInstance().prepareForPutReplication(
                                getServiceManager(), target, job, true, connection);
                    }// Get all blobs that are ready for this job
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
                    if (readyEntries.isEmpty()) {
                        final int notReadyCount = m_replicationWork.m_remainingEntries.size();
                        LOG.info("No entries are ready yet. " + notReadyCount + " entries waiting for allocation.");
                        return false;
                    } else {
                        final int notReadyCount = m_replicationWork.m_remainingEntries.size() - readyEntries.size();
                        if (notReadyCount > 0) {
                            LOG.info(readyEntries.size() + " entries ready to write, " + notReadyCount
                                    + " entries not yet ready.");
                        }
                    }
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
    

    private ReplicationWork getReplicationWork( final UUID jobId, final Ds3Connection connection )
    {
        final Map< UUID, JobEntry> entriesToReplicate = BeanUtils.toMap(m_writeDirective.getEntries());
        final Set<UUID> blobIds = BeanUtils.extractPropertyValues(entriesToReplicate.values(), BlobObservable.BLOB_ID );

        final BlobPersistenceContainer bs = connection.getBlobPersistence( jobId, blobIds );
        final Set<UUID> blobsAlreadyReceived = checkForBlobsAlreadyReceived(bs);

        final Map<UUID, JobEntry> entriesAlreadyComplete = new HashMap<>();
        for ( final JobEntry e : entriesToReplicate.values() )
        {
            if ( blobsAlreadyReceived.contains( e.getBlobId() ) )
            {
                entriesAlreadyComplete.put( e.getId(), e );
            }
        }
        markDestinationsCompleteForEntries(entriesAlreadyComplete);
        entriesToReplicate.keySet().removeAll(entriesAlreadyComplete.keySet());
        return new ReplicationWork( bs.isJobExistant(), entriesToReplicate, Collections.emptyMap() );
    }

    /**
     *  Here we compare checksums of blobs that are already fully received on the target with the checksums we have
     * for local blobs. If there are any mismatches we throw. Otherwise we return the set of blob ids that are already
     * fully received on the target
     * */
    private Set<UUID> checkForBlobsAlreadyReceived(final BlobPersistenceContainer bs) {

        final Map<UUID, BlobPersistence> remoteBlobs = Arrays.stream(bs.getBlobs())
                .filter( b -> b.getChecksum() != null && b.getChecksumType() != null )
                .collect(Collectors.toMap( BlobPersistence::getId, b -> b) );

        if (!remoteBlobs.isEmpty()) {
            final Set< Blob > localBlobs = getServiceManager().getRetriever( Blob.class ).retrieveAll(remoteBlobs.keySet()).toSet();
            for ( final Blob lb : localBlobs )
            {
                final BlobPersistence rb = remoteBlobs.get( lb.getId() );
                if ( !rb.getChecksum().equals( lb.getChecksum() )
                        || !rb.getChecksumType().equals( lb.getChecksumType() ) )
                {
                    throw new RuntimeException(
                            "Blob " + lb.getId()
                            + " already resides on the target, but with a wrong checksum." );
                }
            }
            LOG.info( remoteBlobs.size() + " of " + ( m_writeDirective.getEntries().size())
                    + " blobs are already on the target and don't need to be replicated." );
        }
        return remoteBlobs.keySet();
    }


    @Override
    protected BlobStoreTaskState runInternal()
    {
        final Job job = getServiceManager().getRetriever( Job.class ).attain( m_writeDirective.getEntries().iterator().next().getJobId() );
        final Ds3Connection connection = getConnectionFactory().connect( job.getUserId(), getTarget() );
        try
        {
            if (m_replicationWork.workRemains()) {
                if (m_replicationWork.hasReadyEntries()) {
                    replicateData( m_replicationWork.m_readyEntries, job, connection );
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


    private void replicateData(
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

        // Calculate actual size being written (only for entries we're writing)
        long sizeBeingWritten = 0;
        for ( final JobEntry entry : jobEntries.values() )
        {
            sizeBeingWritten += blobs.get( entry.getBlobId() ).getLength();
        }

        final BytesRenderer bytesRenderer = new BytesRenderer();
        final String dataDescription =
                jobEntries.size() + " blobs (" + bytesRenderer.render( sizeBeingWritten ) + ")";
        LOG.info( "Will write " + dataDescription + "..." );

        final Duration duration = new Duration();
        for ( final JobEntry jobEntry : jobEntries.values() )
        {
            final Blob blob = blobs.get( jobEntry.getBlobId() );
            final S3Object object = objects.get( blob.getObjectId() );
            final Set< S3ObjectProperty > metadata =
                    getServiceManager().getRetriever( S3ObjectProperty.class ).retrieveAll(
                            S3ObjectProperty.OBJECT_ID, object.getId() ).toSet();
            final DiskFileInfo diskFileInfo = getDiskManager().getDiskFileFor( blob.getId() );
            final File file;
            try {
                file = new File( diskFileInfo.getFilePath() );
            } catch (final Exception e) {
                getServiceManager().getService(BlobPoolService.class).registerFailureToRead(diskFileInfo);
                throw e;
            }
            connection.putBlob(
                    job.getId(),
                    bucket.getName(),
                    object.getName(),
                    blob,
                    file,
                    object.getCreationDate(),
                    metadata );
        }

        LOG.info( dataDescription + " written to target at "
                  + bytesRenderer.render( sizeBeingWritten, duration ) + "." );

        markDestinationsCompleteForEntries(jobEntries);
        doNotTreatReadyReturnValueAsFailure();
    }

    private void markDestinationsCompleteForEntries(Map<UUID, JobEntry> jobEntries) {
        try ( final NestableTransaction transaction = m_serviceManager.startNestableTransaction() )
        {
            final Collection< UUID > blobIds = jobEntries.values().stream()
                    .map( e -> e.getBlobId() )
                    .collect( Collectors.toSet() );
            recordBlobTargets(transaction, getTargetId(), blobIds);
            final Ds3BlobDestinationService service = transaction.getService( Ds3BlobDestinationService.class );
            service.update(
                    Require.all(
                            Require.beanPropertyEqualsOneOf(Ds3BlobDestination.TARGET_ID, this.getTargetId()),
                            Require.beanPropertyEqualsOneOf( Ds3BlobDestination.ENTRY_ID, jobEntries.keySet() )
                    ),
                    (x) -> x.setBlobStoreState(JobChunkBlobStoreState.COMPLETED ),
                    RemoteBlobDestination.BLOB_STORE_STATE );
            transaction.commitTransaction();
        }
    }


    private static void recordBlobTargets(final BeansServiceManager transaction, final UUID targetId, final Collection< UUID > blobIds)
    {
        final Map< UUID, UUID > alreadyRecordedBlobs = BeanUtils.toMap( 
                transaction.getRetriever( BlobDs3Target.class ).retrieveAll( Require.all(
                Require.beanPropertyEquals( BlobTarget.TARGET_ID, targetId ),
                Require.beanPropertyEqualsOneOf( 
                        BlobObservable.BLOB_ID, 
                        blobIds ) ) ).toSet(), BlobObservable.BLOB_ID );
        final Set< BlobDs3Target > blobTargets = new HashSet<>();
        for ( final UUID blobId : blobIds )
        {
            if ( !alreadyRecordedBlobs.values().contains( blobId ) )
            {
                blobTargets.add( BeanFactory.newBean( BlobDs3Target.class )
                        .setBlobId( blobId )
                        .setTargetId( targetId ) );
            }
        }

        transaction.getService( BlobDs3TargetService.class ).create( blobTargets );
        Set< UUID > suspectBlobDs3TargetIds =
                BeanUtils.toMap( transaction.getService( SuspectBlobDs3TargetService.class ).retrieveAll( alreadyRecordedBlobs.keySet() ).toSet() ).keySet();
        transaction.getService( SuspectBlobDs3TargetService.class ).delete( suspectBlobDs3TargetIds );
    }
    
    
    protected void handleFailure( final Exception ex )
    {
        getServiceManager().getService( Ds3TargetFailureService.class ).create(
                getTargetId(), TargetFailureType.WRITE_FAILED, ex, null );
    }

    
    public String getDescription()
    {
        return "Write " + m_writeDirective.getEntries().size() + " blobs to " + getTarget().getName();
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_writeDirective.getEntries(), JobEntry.JOB_ID).toArray(new UUID[0]);
    }


    private volatile ReplicationWork m_replicationWork;
    
    private final TargetWriteDirective<Ds3Target, Ds3BlobDestination> m_writeDirective;
}
