/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ChunkReadingTask;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.render.BytesRenderer;

public final class VerifyChunkOnPoolTask extends BasePoolTask implements ChunkReadingTask
{
    public VerifyChunkOnPoolTask(final ReadDirective readDirective,
                                 final BeansServiceManager serviceManager,
                                 final boolean preserveEntries,
                                 final boolean isVerifyAfterWrite,
                                 final PoolEnvironmentResource poolEnvironmentResource,
                                 final PoolLockSupport<PoolTask> lockSupport,
                                 final DiskManager diskManager,
                                 final JobProgressManager jobProgressManager)
    {
        super( readDirective.getPriority(), serviceManager, poolEnvironmentResource, lockSupport, diskManager, jobProgressManager );
        m_readDirective = readDirective;
        m_preserveEntries = preserveEntries;
        m_isVerifyAfterWrite = isVerifyAfterWrite;
    }

    public VerifyChunkOnPoolTask(final ReadDirective readDirective,
                                 final BeansServiceManager serviceManager,
                                 final boolean preserveEntries,
                                 final PoolEnvironmentResource m_poolEnvironmentResource,
                                 final PoolLockSupport<PoolTask> m_lockSupport,
                                 final DiskManager m_diskManager,
                                 final JobProgressManager m_jobProgressManager)
    {
        this(readDirective, serviceManager, preserveEntries, false, m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager);
    }

    public VerifyChunkOnPoolTask(final ReadDirective readDirective,
                                 final BeansServiceManager serviceManager,
                                 final PoolEnvironmentResource m_poolEnvironmentResource,
                                 final PoolLockSupport<PoolTask> m_lockSupport,
                                 final DiskManager m_diskManager,
                                 final JobProgressManager m_jobProgressManager)
    {
        this(readDirective, serviceManager, false,
                m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager);
    }


    public List<JobEntry> getEntries() {
        return m_readDirective.getEntries();
    }
    
    
    @Override
    protected UUID selectPool()
    {
        m_serviceManager.getService(JobEntryService.class).verifyEntriesExist(BeanUtils.extractPropertyValues(m_readDirective.getEntries(), Identifiable.ID));
        final JobEntry chunk = m_readDirective.getEntries().iterator().next();
        if ( m_runningAsNestedTask )
        {
            return chunk.getReadFromPoolId();
        }
        
        try
        {
            getLockSupport().acquireReadLock( chunk.getReadFromPoolId(), this );
            return chunk.getReadFromPoolId();
        }
        catch ( final IllegalStateException ex )
        {
            LOG.debug( "Cannot acquire read lock.", ex );
            return null;
        }
    }
    
    
    void runAsNestedTaskInsideAnotherTask( final UUID poolId )
    {
        m_readDirective.getEntries().iterator().next().setReadFromPoolId( poolId );
        m_runningAsNestedTask = true;
        prepareForExecutionIfPossible();
        run();
    }
    

    @Override
    protected BlobStoreTaskState runInternal()
    {
        final Map< UUID, JobEntry> jobEntries = BeanUtils.toMap(m_readDirective.getEntries());
        final Set< UUID > blobsToRead = 
                BeanUtils.extractPropertyValues( jobEntries.values(), BlobObservable.BLOB_ID );
        if ( blobsToRead.isEmpty() )
        {
            if (!m_runningAsNestedTask) {
            markChunksAsCompleted();
            }
            return BlobStoreTaskState.COMPLETED;
        }
        
        return performVerify( jobEntries, blobsToRead );
    }
    
    
    private BlobStoreTaskState performVerify(
            final Map< UUID, JobEntry> jobEntries,
            final Set< UUID > blobIds )
    {
        final Map< UUID, Blob > blobs = BeanUtils.toMap( 
                getServiceManager().getRetriever( Blob.class ).retrieveAll( blobIds ).toSet() );
        final Map< UUID, S3Object > objects = BeanUtils.toMap( 
                getServiceManager().getRetriever( S3Object.class ).retrieveAll(
                        BeanUtils.< UUID >extractPropertyValues( 
                                blobs.values(), Blob.OBJECT_ID ) ).toSet() );
        final Map< UUID, Bucket > buckets = BeanUtils.toMap( 
                getServiceManager().getRetriever( Bucket.class ).retrieveAll(
                        BeanUtils.< UUID >extractPropertyValues( 
                                objects.values(), S3Object.BUCKET_ID ) ).toSet() );
        long bytesToRead = 0;
        for ( final UUID blobId : blobIds )
        {
            final Blob blob = blobs.get( blobId );
            bytesToRead += blob.getLength();
        }
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final Duration duration = new Duration();
        final String dataDescription = 
                jobEntries.size() + " blobs (" + bytesRenderer.render( bytesToRead ) + ")";
        LOG.info( "Will verify " + dataDescription + "..." );

        final ThreadedBlobVerifier blobVerifier = new ThreadedBlobVerifier( getPool() );
        for ( final UUID blobId : blobIds )
        {
            final Blob blob = blobs.get( blobId );
            final S3Object object = objects.get( blob.getObjectId() );
            final Bucket bucket = buckets.get( object.getBucketId() );
            blobVerifier.verify( bucket, object, blob );
        }
        
        final Map< UUID, String > failures = blobVerifier.getFailures();
        LOG.info( dataDescription + " verified on pool at " 
                  + bytesRenderer.render( bytesToRead, duration ) + "." );

        final Set< UUID > failedBlobIds = new HashSet<>();
        final Map< UUID, JobEntry> blobIdToJobEntryMap = new HashMap<>();
        for ( final JobEntry e : jobEntries.values() )
        {
            blobIdToJobEntryMap.put( e.getBlobId(), e );
        }
        if ( !failures.isEmpty() )
        {
            if ( m_runningAsNestedTask )
            {
                throw new RuntimeException( "Failed to verify blobs." );
            }

            for ( final Map.Entry< UUID, String > failure : failures.entrySet() )
            {
                final UUID blobId = failure.getKey();
                final UUID jobEntryId = blobIdToJobEntryMap.get( blobId ).getId();
                failedBlobIds.add( blobId );
                jobEntries.remove( jobEntryId );
            }
        }

        if ( !m_isVerifyAfterWrite )
        {
            for ( final Blob b : getServiceManager().getRetriever( Blob.class ).retrieveAll(
                    BeanUtils.< UUID >extractPropertyValues(
                            jobEntries.values(), BlobObservable.BLOB_ID ) ).toSet() )
            {
                getJobProgressManager().workCompleted( blobIdToJobEntryMap.get( b.getId() ).getJobId(), b.getLength() );
            }
        }

        if ( !m_preserveEntries ) {
            getServiceManager().getService(JobEntryService.class).delete(jobEntries.keySet());
        }
        
        if ( failures.isEmpty() )
        {
            if (!m_runningAsNestedTask) {
                markChunksAsCompleted();
            }
            return BlobStoreTaskState.COMPLETED;
        }
        
        final BeansServiceManager transaction = getServiceManager().startTransaction();
        try
        {
            transaction.getService( BlobPoolService.class ).blobsSuspect(
                    "Verify " + m_readDirective.getEntries().size() + " blobs failed.",
                    getServiceManager().getRetriever( BlobPool.class ).retrieveAll( Require.all( 
                            Require.beanPropertyEquals( BlobPool.POOL_ID, getPoolId() ),
                            Require.beanPropertyEqualsOneOf( 
                                    BlobObservable.BLOB_ID, failedBlobIds ) ) ).toSet() );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final JobEntryService entryService = getServiceManager().getService( JobEntryService.class );
        final Map<UUID, JobEntry> failedEntriesById = entryService.retrieveAll(
                Require.all(
                        Require.beanPropertyEqualsOneOf(Identifiable.ID, getChunkIds()),
                        Require.beanPropertyEqualsOneOf(JobEntry.BLOB_ID, failedBlobIds)
                )).toMap();
        for (JobEntry entry : failedEntriesById.values()) {
            LOG.info( "Job entry " + entry + " failed to verify from pool. Marking it as requiring re-read from other source." );
            entryService.update( entry.setReadFromPoolId(null), JobEntry.READ_FROM_POOL_ID );
        }

        return BlobStoreTaskState.COMPLETED;
    }


    public String getDescription() {
        return "Verify " + m_readDirective.getEntries().size() + " blobs";
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_readDirective.getEntries(), JobEntry.JOB_ID).toArray(new UUID[0]);
    }

    private Collection<UUID> getChunkIds() {
        return BeanUtils.extractPropertyValues(getEntries(), Identifiable.ID);
    }

    private void markChunksAsCompleted()
    {
        final List<JobEntry> chunks = getEntries();
        getServiceManager().getService( JobEntryService.class ).update(
                Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunks).keySet()),
                e -> e.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                JobEntry.BLOB_STORE_STATE );
    }


    private volatile boolean m_runningAsNestedTask;
    private final ReadDirective m_readDirective;
    private final boolean m_preserveEntries;
    private final boolean m_isVerifyAfterWrite;
}
