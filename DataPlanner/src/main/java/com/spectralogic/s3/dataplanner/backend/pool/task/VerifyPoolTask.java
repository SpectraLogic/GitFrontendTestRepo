/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolFailureService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolService.PoolAccessType;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;

public final class VerifyPoolTask extends BasePoolTask
{
    
    
    public VerifyPoolTask(
            final BlobStoreTaskPriority priority,
            final UUID poolId,
            final BeansServiceManager beansServiceManager,
            final int maxNumberOfBlobsToVerifyAtOnce,
            final PoolEnvironmentResource poolEnvironmentResource,
            final PoolLockSupport<PoolTask> lockSupport,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager)
    {
        super( priority, beansServiceManager, poolEnvironmentResource, lockSupport, diskManager, jobProgressManager);
        m_poolId = poolId;
        m_maxNumberOfBlobsToVerifyAtOnce = maxNumberOfBlobsToVerifyAtOnce;
        Validations.verifyNotNull( "Pool", m_poolId );
    }

    public VerifyPoolTask(BlobStoreTaskPriority priority, UUID poolId, BeansServiceManager serviceManager,
                          final PoolEnvironmentResource poolEnvironmentResource,
                          final PoolLockSupport<PoolTask> lockSupport,
                          final DiskManager diskManager,
                          final JobProgressManager jobProgressManager) {
        this( priority, poolId, serviceManager, 10000, poolEnvironmentResource, lockSupport, diskManager, jobProgressManager);
    }


    @Override
    protected UUID selectPool()
    {
        try
        {
            getLockSupport().acquireReadLock( m_poolId, this );
            return m_poolId;
        }
        catch ( final IllegalStateException ex )
        {
            LOG.debug( "Cannot acquire read lock.", ex );
            return null;
        }
    }

    
    @Override
    protected BlobStoreTaskState runInternal()
    {
        try
        {
            final Duration duration = new Duration();
            BlobStoreTaskState retval = initializeVerify();
            if ( null != retval )
            {
                return retval;
            }
            
            performLowLevelVerify();
            verify();
            m_lastVerifiedBlob = m_blobPools.get( m_blobPools.size() - 1 ).getBlobId();
            LOG.info( "Verification round completed in " + duration 
                      + ".  Last verified blob: " + m_lastVerifiedBlob );
            retval = initializeVerify();
            if ( null == retval )
            {
                doNotTreatReadyReturnValueAsFailure();
                return BlobStoreTaskState.READY;
            }
            return retval;
        }
        catch ( final RuntimeException ex )
        {
            handleFailure( ex );
            throw ex;
        }
        finally
        {
            getServiceManager().getService( PoolService.class ).updateDates( 
                    getPoolId(), PoolAccessType.ACCESSED );
        }
    }
    
    
    private BlobStoreTaskState initializeVerify()
    {
        if ( null == m_verifyStartDate )
        {
            m_verifyStartDate = new Date();
        }
        
        final PoolService poolService = getServiceManager().getService( PoolService.class );
        m_pool = poolService.attain( m_poolId );
        if ( null != m_pool.getLastModified() 
                && m_pool.getLastModified().getTime() > m_verifyStartDate.getTime() )
        {
            LOG.info( "Verification of pool interrupted by modifications to the pool.  "
                      + "Verification must be restarted." );
            m_verifyStartDate = null;
            m_lastVerifiedBlob = null;
            return BlobStoreTaskState.READY;
        }
        
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( BlobObservable.BLOB_ID, SortBy.Direction.ASCENDING );
        m_blobPools =
                getServiceManager().getRetriever( BlobPool.class ).retrieveAll( Query.where( Require.all( 
                        Require.beanPropertyEquals( BlobPool.POOL_ID, m_poolId ),
                        ( null == m_lastVerifiedBlob ) ? 
                                null 
                                : Require.beanPropertyGreaterThan( 
                                        BlobObservable.BLOB_ID, m_lastVerifiedBlob ) ) )
                        .orderBy( ordering )
                        .limit( m_maxNumberOfBlobsToVerifyAtOnce ) ).toList();
        if ( m_blobPools.isEmpty() )
        {
            poolService.updateDates( m_poolId, PoolAccessType.VERIFIED );
            return BlobStoreTaskState.COMPLETED;
        }
        
        return null;
    }
    
    
    private void performLowLevelVerify()
    {
        if ( !m_lowLevelVerifyRequired )
        {
            return;
        }
        
        getPoolEnvironmentResource().verifyPool( getPool().getGuid() ).get( 30, TimeUnit.DAYS );
        m_lowLevelVerifyRequired = false;
    }
    
    
    private void verify()
    {
        m_blobs = BeanUtils.toMap( getServiceManager().getRetriever( Blob.class ).retrieveAll( 
                BeanUtils.extractPropertyValues(
                        m_blobPools, BlobObservable.BLOB_ID ) ).toSet() );
        m_objects = BeanUtils.toMap( getServiceManager().getRetriever( S3Object.class ).retrieveAll( 
                BeanUtils.extractPropertyValues(
                        m_blobs.values(), Blob.OBJECT_ID ) ).toSet() );
        m_buckets = BeanUtils.toMap( getServiceManager().getRetriever( Bucket.class ).retrieveAll( 
                BeanUtils.extractPropertyValues(
                        m_objects.values(), S3Object.BUCKET_ID ) ).toSet() );
        
        m_numBlobsLost = 0;
        final ThreadedBlobVerifier blobVerifier = new ThreadedBlobVerifier( m_pool );
        m_transaction = getServiceManager().startTransaction();
        try
        {
            verifyBuckets();
            verifyObjects();
            verifyBlobs( blobVerifier );
            
            for ( final Map.Entry< UUID, String > failure : blobVerifier.getFailures().entrySet() )
            {
                blobsLost( 
                        failure.getValue(),
                        CollectionFactory.toSet( failure.getKey() ) );
            }
            
            if ( 0 < m_numBlobsLost )
            {
                getServiceManager().getService( PoolFailureService.class ).create(
                        getPoolId(), 
                        PoolFailureType.BLOB_READ_FAILED, 
                        "Failed to verify " + m_numBlobsLost + " blobs on pool." );
            }
            m_transaction.commitTransaction();
        }
        finally
        {
            m_transaction.closeTransaction();
        }
    }
    
    
    private void verifyBuckets()
    {
        final Duration duration = new Duration();
        for ( final Bucket bucket : m_buckets.values() )
        {
            verifyBucket( bucket );
        }
        LOG.info( "Verified " + m_buckets.size() + " buckets in " + duration + "." );
    }
    
    
    private void verifyBucket( final Bucket bucket )
    {
        final Path dir = PoolUtils.getPath( m_pool, bucket.getName(), null, null );
        if ( !Files.exists( dir ) )
        {
            final Set< UUID > lostObjects = new HashSet<>();
            for ( final S3Object o : m_objects.values() )
            {
                if ( o.getBucketId().equals( bucket.getId() ) )
                {
                    lostObjects.add( o.getId() );
                }
            }
            objectsLost( 
                    "Bucket " + bucket.getName() + " is not persisted: " + dir,
                    lostObjects );
        }
    }
    
    
    private void verifyObjects()
    {
        final Duration duration = new Duration();
        for ( final S3Object object : m_objects.values() )
        {
            final Bucket bucket = m_buckets.get( object.getBucketId() );
            verifyObject( bucket, object );
        }
        LOG.info( "Verified " + m_objects.size() + " objects in " + duration + "." );
    }
    
    
    private void verifyObject( 
            final Bucket bucket, 
            final S3Object object )
    {
        final File hashDir = new File( 
                PoolUtils.getPath( m_pool, bucket.getName(), null, null ) + Platform.FILE_SEPARATOR
                + object.getId().toString().substring( 0, 2 ) );
        if ( !hashDir.exists() )
        {
            objectsLost(
                    "Object cache dir does not exist for object " + object.getId() + ": " + hashDir,
                    CollectionFactory.toSet( object.getId() ) );
        }
    
        final Path objectDir = PoolUtils.getPath( m_pool, bucket.getName(), object.getId(), null );
        PoolUtils.verifyObjectDir( objectDir );
        if ( !Files.exists( objectDir ) )
        {
            objectsLost(
                    "Object dir does not exist for object " + object.getId() + ": " + objectDir,
                    CollectionFactory.toSet( object.getId() ) );
        }
    }
    
    
    private void objectsLost(
            final String cause,
            final Set< UUID > objectIds )
    {
        final Set< UUID > lostBlobs = new HashSet<>();
        for ( final Blob blob : m_blobs.values() )
        {
            if ( objectIds.contains( blob.getObjectId() ) )
            {
                lostBlobs.add( blob.getId() );
            }
        }
        
        blobsLost( cause, lostBlobs );
    }
    
    
    private void verifyBlobs( final ThreadedBlobVerifier blobVerifier )
    {
        final Duration duration = new Duration();
        for ( final BlobPool bp : m_blobPools )
        {
            final Blob blob = m_blobs.get( bp.getBlobId() );
            final S3Object object = m_objects.get( blob.getObjectId() );
            final Bucket bucket = m_buckets.get( object.getBucketId() );
            blobVerifier.verify( bucket, object, blob );
        }
        LOG.info( "Verified " + m_blobs.size() + " blobs in " + duration + "." );
    }
    
    
    private void blobsLost( final String cause, final Set< UUID > blobIds )
    {
        m_numBlobsLost += blobIds.size();
        m_transaction.getService( BlobPoolService.class ).blobsSuspect( 
                cause, 
                getServiceManager().getRetriever( BlobPool.class ).retrieveAll( Require.all( 
                        Require.beanPropertyEquals( BlobPool.POOL_ID, m_poolId ),
                        Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ) ).toSet() );
    }
    
    
    private void handleFailure( final Exception ex )
    {
        getServiceManager().getService( PoolFailureService.class ).create(
                getPoolId(), PoolFailureType.VERIFY_FAILED, ex );
    }

    
    public String getDescription()
    {
        return "Verify Pool " + m_poolId;
    }
    
    
    private volatile boolean m_lowLevelVerifyRequired = true;
    private volatile Map< UUID, Bucket > m_buckets;
    private volatile Map< UUID, S3Object > m_objects;
    private volatile Map< UUID, Blob > m_blobs;
    private volatile UUID m_lastVerifiedBlob;
    private volatile Date m_verifyStartDate;
    private volatile List< BlobPool > m_blobPools;
    private volatile Pool m_pool;
    private volatile BeansServiceManager m_transaction;
    private volatile int m_numBlobsLost;
    
    private final UUID m_poolId;
    private final int m_maxNumberOfBlobsToVerifyAtOnce;
}
