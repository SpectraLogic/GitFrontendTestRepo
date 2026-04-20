/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3Utils;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

final class BucketServiceImpl extends BaseService< Bucket > implements BucketService
{
    BucketServiceImpl()
    {
        super( Bucket.class, 
               AWSFailure.NO_SUCH_BUCKET );
    }
    
    
    @Override
    synchronized public void create( final Bucket bucket )
    {
        if ( S3Utils.REST_REQUEST_REQUIRED_PREFIX.equalsIgnoreCase( bucket.getName() ) )
        {
            throw new DaoException(
                    GenericFailure.BAD_REQUEST, 
                    "Bucket name '" + bucket.getName() + "' conflicts with " 
                    + "the REST request required prefix: "
                    + S3Utils.REST_REQUEST_REQUIRED_PREFIX );
        }
        if ( null != retrieve( Bucket.NAME, bucket.getName() ) )
        {
            throw new DaoException( 
                    GenericFailure.CONFLICT, 
                    "A bucket with the name " + bucket.getName() + " already exists.");
        }
        
        super.create( bucket );
        createImplicitOwnerAcl( bucket );
    }
    
    
    @Override
    public void update( final Bucket bucket, final String... propertiesToUpdate )
    {
        super.update( bucket, propertiesToUpdate );
        if ( CollectionFactory.toSet( propertiesToUpdate ).contains( UserIdObservable.USER_ID ) )
        {
            createImplicitOwnerAcl( bucket );
        }
    }


    @Override
    public void update(final WhereClause whereClause, final Consumer<Bucket> beanConsumer, final String... propertiesToUpdate)
    {
        final Bucket bean = BeanFactory.newBean( Bucket.class );
        beanConsumer.accept(bean);
        getDataManager().updateBeans(CollectionFactory.toSet(propertiesToUpdate), bean, whereClause);
        if (CollectionFactory.toSet(propertiesToUpdate).contains(UserIdObservable.USER_ID)) {
            try (final EnhancedIterable<Bucket> iter = retrieveAll(whereClause).toIterable()) {
                for (final Bucket bucket : iter) {
                    createImplicitOwnerAcl(bucket);
                }
            }
        }
    }
    
    
    private void createImplicitOwnerAcl( final Bucket bucket )
    {
        try
        {
            getServiceManager().getService( BucketAclService.class ).create( 
                    BeanFactory.newBean( BucketAcl.class )
                        .setBucketId( bucket.getId() )
                        .setUserId( bucket.getUserId() )
                        .setPermission( BucketAclPermission.OWNER ) );
        }
        catch ( final DaoException ex )
        {
            LOG.info( "Implicit ownership ACL already exists for user " + bucket.getUserId() 
                      + " on bucket " + bucket.getId() + ".", ex );
        }
    }
    
    
    public long getLogicalCapacity( final UUID bucketId )
    {
        return getDataManager().getSum( Blob.class, Blob.LENGTH, Require.exists(
                        Blob.OBJECT_ID, 
                        Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ) ) );
    }


    public long getPendingPutWorkInBytes(final UUID isolatedBucketId, final UUID storageDomainId)
    {
        final WhereClause chunkMustGoToStorageDomain = Require.exists(
                LocalBlobDestination.class,
                LocalBlobDestination.ENTRY_ID,
                Require.all(
                        Require.beanPropertyEquals(LocalBlobDestination.STORAGE_DOMAIN_ID, storageDomainId),
                        Require.not(Require.beanPropertyEquals(LocalBlobDestination.BLOB_STORE_STATE, JobChunkBlobStoreState.COMPLETED))
                )

        );
        final WhereClause chunkIsForBucketIfIsolated;
        if (isolatedBucketId == null) {
            chunkIsForBucketIfIsolated = Require.nothing();
        } else {
            chunkIsForBucketIfIsolated = Require.exists(
                    JobEntry.JOB_ID,
                    Require.beanPropertyEquals(JobObservable.BUCKET_ID, isolatedBucketId));
        }
        final WhereClause blobFilter = Require.exists(
                        JobEntry.class,
                        BlobObservable.BLOB_ID,
                        Require.all(
                                chunkMustGoToStorageDomain,
                                chunkIsForBucketIfIsolated
                                ) );

        return getDataManager().getSum(Blob.class, Blob.LENGTH, blobFilter);
    }

    
    public ReentrantReadWriteLock getLock()
    {
        if ( getServiceManager().isTransaction() )
        {
            return getServiceManager().getTransactionSource().getService( BucketService.class ).getLock();
        }
        return m_lock;
    }
    
    
    public BucketLogicalSizeCache getLogicalSizeCache()
    {
        if ( null == m_logicalSizeCache )
        {
            throw new IllegalStateException( "Logical size cache not configured." );
        }
        return m_logicalSizeCache;
    }
    
    
    public void initializeLogicalSizeCache()
    {
        verifyNotTransaction();
        m_logicalSizeCache = new BucketLogicalSizeCacheImpl( getServiceManager() );
    }
    
    
    public void initializeLogicalSizeCache( final BucketService source )
    {
        verifyInsideTransaction();
        m_logicalSizeCache = ( (BucketServiceImpl)source ).m_logicalSizeCache;
    }
    
    
    private volatile BucketLogicalSizeCache m_logicalSizeCache;
    private final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();
}
