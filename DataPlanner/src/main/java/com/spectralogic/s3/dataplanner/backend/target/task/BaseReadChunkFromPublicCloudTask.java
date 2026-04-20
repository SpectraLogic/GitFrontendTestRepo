/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.orm.JobEntryRM;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.PublicCloudBucketNameService;
import com.spectralogic.s3.common.dao.service.target.TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.*;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.target.api.OfflineDataStagingWindowManager;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;

import com.spectralogic.util.exception.BlobReadFailedException;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;

abstract class BaseReadChunkFromPublicCloudTask
    < T extends DatabasePersistable & PublicCloudReplicationTarget< T >,
      CF extends PublicCloudConnectionFactory< ?, T > >
    extends BasePublicCloudTask< T, CF >
{
    abstract protected void markBlobSuspect( UUID blobId);

    protected BaseReadChunkFromPublicCloudTask(
            final OfflineDataStagingWindowManager offlineDataStagingWinodwManager,
            final Class< T > targetType,
            final Class< ? extends PublicCloudBucketNameService< ? > > cloudBucketNameServiceType,
            final Class< ? extends TargetFailureService< ? > > failureServiceType,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager,
            final CF connectionFactory,
            final ReadDirective readDirective)
    {
        super( cloudBucketNameServiceType,
                targetType,
                readDirective.getReadSourceId(),
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory,
                readDirective.getPriority());
        m_readDirective = readDirective;
        m_failureServiceType = failureServiceType;
        m_offlineDataStagingWindowManager = offlineDataStagingWinodwManager;
        Validations.verifyNotNull( "Failure service type", m_failureServiceType );
        Validations.verifyNotNull( "Offline data staging window manager", m_offlineDataStagingWindowManager );
    }

    
    @Override
    final protected boolean prepareForExecution()
    {
        m_serviceManager.getService(JobEntryService.class).verifyEntriesExist(BeanUtils.extractPropertyValues(m_readDirective.getEntries(), Identifiable.ID));
        return true;
    }


    @Override
    final protected BlobStoreTaskState runInternal()
    {
        final Bucket bucket = new JobEntryRM( m_readDirective.getEntries().iterator().next(), getServiceManager() ).getJob().getBucket().unwrap();
        final String cloudBucketName = getCloudBucketSupport().attainCloudBucketName(
                bucket.getId(), getTargetId() );
        final Map< UUID, JobEntry> jobEntries = BeanUtils.toMap( m_readDirective.getEntries() );
        for ( final JobEntry e : new HashSet<>( jobEntries.values() ) )
        {
            if ( getDiskManager().isInCache( e.getBlobId() ) )
            {
                jobEntries.remove( e.getId() );
            }
        }
        if ( jobEntries.isEmpty() )
        {
            LOG.info( "No data replication is required to cloud bucket '" + cloudBucketName + "'." );
            markChunkAsCompleted();
            return BlobStoreTaskState.COMPLETED;
        }

        getDiskManager().allocateChunks( jobEntries.keySet() );
        final PublicCloudConnection connection = getConnectionFactory().connect( getTarget() );
        try
        {
            final PublicCloudBucketInformation cloudBucket = getCloudBucketSupport().verifyBucket( 
                    connection, bucket.getId(), getTargetId() );
            if ( null == cloudBucket )
            {
                throw new RuntimeException( 
                        "Bucket '" + cloudBucketName + "' does not exist on cloud." );
            }
            
            final BlobStoreTaskState retval = replicateData( 
                    jobEntries,
                    cloudBucket,
                    connection,
                    DataOfflineablePublicCloudReplicationTarget.class.isAssignableFrom( m_targetType ) );

            if ( BlobStoreTaskState.COMPLETED == retval  )
            {
                markChunkAsCompleted();
            }
            return retval;
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

    protected PublicCloudConnection createConnection()
    {
        return getConnectionFactory().connect( getTarget() );
    }

    private BlobStoreTaskState replicateData( 
            final Map< UUID, JobEntry> jobEntries,
            final PublicCloudBucketInformation cloudBucket,
            final PublicCloudConnection connection,
            final boolean checkForDataOffline )
    {
        final Map< UUID, Blob > blobs = BeanUtils.toMap( 
                getServiceManager().getRetriever( Blob.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( 
                                jobEntries.values(), BlobObservable.BLOB_ID ) ).toSet() );
        final Map< UUID, S3Object > objects = BeanUtils.toMap( 
                getServiceManager().getRetriever( S3Object.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( blobs.values(), Blob.OBJECT_ID ) )
                        .toSet() );
        final long bytesToRead = getBytesToRead(blobs );
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final String dataDescription = 
                blobs.size() + " blobs (" + bytesRenderer.render( bytesToRead ) + ")";

        // checkForDataOffline is true for S3Connection and S3NativeConnection
        if ( checkForDataOffline )
        {
            LOG.info( "Determining if " + dataDescription + " online and ready to be read from "
                    + cloudBucket.getName() + "..." );

            Set<UUID> offlineBlobIds = getOfflineBlobs(
                    cloudBucket,
                    (DataOfflineablePublicCloudConnection) connection,
                    jobEntries,
                    objects,
                    blobs );

            //Offline blobs with be missing blobs or blobs available in glacier.
            // Check if there are unavailable blobs among the offline blobs.
            if ( !offlineBlobIds.isEmpty() )
            {
                stripBlobs( jobEntries, offlineBlobIds );
                replicateData( jobEntries, cloudBucket, connection, false );

                Set<UUID> missingBlobIds = getMissingBlobs();
                if (!missingBlobIds.isEmpty()) {
                    offlineBlobIds.removeAll(missingBlobIds);
                }
                if (!offlineBlobIds.isEmpty()) {
                    requireTaskSuspension();
                    return BlobStoreTaskState.READY;
                } else {
                    //This means the failure is due to missing blobs. Already marked those blobs as suspect.
                    // So marking TASK as completed.
                    return BlobStoreTaskState.COMPLETED;
                }

            }
        }

        final Set< UUID > blobsToRead;
        synchronized ( CLOUD_BLOB_READS_IN_PROGRESS )
        {
            blobsToRead = blobs.keySet();
            for ( final UUID id : blobsToRead )
            {
                if ( CLOUD_BLOB_READS_IN_PROGRESS.contains( id ) )
                {
                    LOG.info( "This cloud read task must be retried later since blob " + id
                              + " is actively being read from cloud by another task." );
                    return BlobStoreTaskState.READY;
                }
            }
            CLOUD_BLOB_READS_IN_PROGRESS.addAll( blobsToRead );
        }
        
        try
        {
            LOG.info( "Will read " + dataDescription + " from " + cloudBucket.getName() + "..." );
            final Duration duration = new Duration();
            final Map<UUID, List< Future<? >>> readThreadsMap = new HashMap<>();
            Set<UUID> readErrorBlobs = new HashSet<>();
            for ( final Blob blob : blobs.values() )
            {
                final S3Object object = objects.get( blob.getObjectId() );

                final List< Future< ? > > tmpThreads = connection.readBlobFromCloud(
                        cloudBucket,
                        object,
                        blob,
                        new File( getDiskManager().allocateChunksForBlob( blob.getId() ) ) );
                if ( null != tmpThreads && !tmpThreads.isEmpty() )
                {
                    readThreadsMap.put( blob.getId(), tmpThreads );
                } else {
                    //Blob does not exist in the cloud bucket.
                    blobs.remove(blob.getId());
                    readErrorBlobs.add(blob.getId());
                }
            }
            Exception readException = null;

            for ( final Map.Entry< UUID, List< Future< ? > > > entry : readThreadsMap.entrySet() ) {
                for ( final Future< ? > future : entry.getValue() ) {
                    try
                    {
                        future.get();
                    } catch ( Exception e )
                    {
                        // If there is a BlobReadFailedException that means blob is missing.
                        readException = e;
                    }
                }
            }
            if (readException != null) {
                throw ExceptionUtil.toRuntimeException(readException);
            }
            if (!readErrorBlobs.isEmpty()) {
                stripBlobs( jobEntries, readErrorBlobs );

            }
            for ( final UUID blobId : blobs.keySet() )
            {
                getDiskManager().blobLoadedToCache(blobId);
            }

            LOG.info( dataDescription + " read from target at "
                      + bytesRenderer.render( bytesToRead, duration ) + "." );

            return BlobStoreTaskState.COMPLETED;
        }
        finally
        {
            synchronized ( CLOUD_BLOB_READS_IN_PROGRESS )
            {
                CLOUD_BLOB_READS_IN_PROGRESS.removeAll( blobsToRead );
            }
        }

    }


    abstract Set<UUID> getMissingBlobs();

    private long getBytesToRead(final Map< UUID, Blob > blobs )
    {
        long retval = 0;
        for ( final Blob blob : blobs.values() )
        {
            retval += blob.getLength();
        }

        return retval;
    }


    /**
     * @return an empty set to indicate no blobs are offline, or a non-empty set of the blobs that are offline
     */
    private Set< UUID > getOfflineBlobs(
            final PublicCloudBucketInformation cloudBucket,
            final DataOfflineablePublicCloudConnection connection,
            final Map< UUID, JobEntry> jobEntries,
            final Map< UUID, S3Object > objects,
            final Map< UUID, Blob > blobs)
    {
        final Set< UUID > retval = new HashSet<>();
        final DataOfflineablePublicCloudReplicationTarget< ? > target =
                (DataOfflineablePublicCloudReplicationTarget< ? >)getTarget();
        boolean anyStaged = false;
        boolean acquiredAnyStagingLock = false;
        List<Exception> readExceptions = new ArrayList<>();

        final Map<UUID, UUID> blobIdToJobEntryMap = new HashMap<>();
        for (JobEntry entry : jobEntries.values()) {
            blobIdToJobEntryMap.put(entry.getBlobId(), entry.getId());
        }
        // For each blob, check if the blob is available in the cloud. If it is available, then
        // check if it is online.
        for ( final Blob blob : blobs.values() )
        {
            final S3Object object = objects.get( blob.getObjectId() );
            try {
                if (!connection.isBlobAvailableOnCloud(cloudBucket, object, blob)) {
                    retval.add( blob.getId() );
                } else {
                    // This call checks if the blob is offline or not.
                    if ( !connection.isBlobReadyToBeReadFromCloud( cloudBucket, object,blob ) )
                    {
                        retval.add( blob.getId() );
                        if ( m_offlineDataStagingWindowManager.tryLock(
                                m_targetType, getTargetId(), blob.getId())  )
                        {
                            acquiredAnyStagingLock = true;
                            if (MAX_STAGE_REQUEST_FREQUENCY_IN_HOURS * 60
                                    < m_durationSinceStageRequested.getElapsedMinutes()) {
                                connection.beginStagingToRead(
                                        cloudBucket, object, blob, target.getStagedDataExpirationInDays() );
                                anyStaged = true;
                            }
                        }
                    } else {
                        m_offlineDataStagingWindowManager.releaseLock( blobIdToJobEntryMap.get(blob.getId()) );
                    }
                }

            } catch (Exception ex) {
                readExceptions.add(ex);
            }
        }
        if (!readExceptions.isEmpty() ) {
            throw ExceptionUtil.toRuntimeException(readExceptions.get(0));
        }
        if (anyStaged) {
            m_durationSinceStageRequested.reset();
        }
        if ( retval.isEmpty() )
        {
            LOG.info( "All data to read is online in the cloud." );
        }
        else if ( !acquiredAnyStagingLock )
        {
            LOG.info( "Some data to read is offline in the cloud (could not acquire staging lock)." );
        }
        else if ( anyStaged )
        {
            LOG.info( "Some data to read is offline in the cloud (stage requested)." );
        }
        else
        {
            LOG.info( "Some data to read is offline in the cloud (last requested staging "
                    + m_durationSinceStageRequested + " ago)." );
        }

        return retval;
    }

    private void stripBlobs(final Map< UUID, JobEntry> jobEntries, final Set< UUID > blobIdsToRemove )
    {
        for ( final Map.Entry< UUID, JobEntry> e : new HashSet<>( jobEntries.entrySet() ) )
        {
            if ( blobIdsToRemove.contains( e.getValue().getBlobId() ) )
            {
                jobEntries.remove( e.getKey() );
            }
        }
    }


    protected void markChunkAsCompleted()
    {
        Set<UUID> blobIds = getMissingBlobs();
        List<JobEntry> entries = m_readDirective.getEntries();
        List<JobEntry> filteredEntries = new ArrayList<>();
        for (JobEntry entry : entries) {
            if (!blobIds.contains(entry.getBlobId())) {
                filteredEntries.add(entry);
            }
        }
        getJobProgressManager().entriesLoadedToCache(getServiceManager(), filteredEntries);
        final Set<UUID> jobEntryIds = BeanUtils.toMap( filteredEntries ).keySet();
        getServiceManager().getService( JobEntryService.class ).update(
                Require.beanPropertyEqualsOneOf(Identifiable.ID, jobEntryIds),
                (chunk) -> chunk.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                JobEntry.BLOB_STORE_STATE );
    }


    final protected void handleFailure( final Exception ex )
    {
        getServiceManager().getService( m_failureServiceType ).create(
                getTargetId(), TargetFailureType.READ_FAILED, ex, null );
    }


    public List<JobEntry> getEntries()
    {
        return m_readDirective.getEntries();
    }


    final public String getDescription()
    {
        return "Read " + m_readDirective.getEntries().size() + " chunks from target " + getTargetId() + ".";
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_readDirective.getEntries(), JobEntry.JOB_ID).toArray(new UUID[0]);
    }


    private final ReadDirective m_readDirective;
    private final Class< ? extends TargetFailureService< ? > > m_failureServiceType;
    private final OfflineDataStagingWindowManager m_offlineDataStagingWindowManager;
    private final Duration m_durationSinceStageRequested = new Duration( 0 );
    
    private final static int MAX_STAGE_REQUEST_FREQUENCY_IN_HOURS = 6;
    private final static Set< UUID > CLOUD_BLOB_READS_IN_PROGRESS = new HashSet<>();
}
