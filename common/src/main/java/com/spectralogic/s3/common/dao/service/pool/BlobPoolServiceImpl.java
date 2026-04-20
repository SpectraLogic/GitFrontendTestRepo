/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ObsoleteBlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.ds3.DegradedBlobService;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

final class BlobPoolServiceImpl extends BaseService< BlobPool > implements BlobPoolService
{
    BlobPoolServiceImpl()
    {
        super( BlobPool.class );
    }
    

    @Override
    public void obsoleteBlobPools(  final Set< BlobPool > blobTargets, final UUID obsoletion )
    {
        final Set< ObsoleteBlobPool > beans = new HashSet<>();
        for ( final BlobPool blobTarget : blobTargets )
        {
            final ObsoleteBlobPool bean = BeanFactory.newBean( ObsoleteBlobPool.class );
            BeanCopier.copy( bean, blobTarget );
            bean.setObsoletionId( obsoletion );
            beans.add( bean );
        }

        getServiceManager().getService( ObsoleteBlobPoolService.class ).create( beans );
    }
    
    
    @Override
    public void reclaimForDeletedPersistenceRule( final UUID dataPolicyId, final UUID storageDomainId )
    {
        getDataManager().updateBeans( 
                CollectionFactory.toSet( PersistenceTarget.BUCKET_ID ),
                BeanFactory.newBean( Pool.class ), 
                Require.all( 
                        Require.exists(
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                                Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ) ),
                        Require.exists( 
                                PersistenceTarget.BUCKET_ID,
                                Require.beanPropertyEquals( Bucket.DATA_POLICY_ID, dataPolicyId ) ) ) );
        getDataManager().deleteBeans( BlobPool.class, Require.all( 
                Require.exists(
                        BlobPool.POOL_ID, 
                        Require.exists(
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                                Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID,
                                        storageDomainId ) ) ),
                Require.exists( 
                        BlobObservable.BLOB_ID, 
                        Require.exists( 
                                Blob.OBJECT_ID,
                                Require.exists(
                                        S3Object.BUCKET_ID,
                                        Require.beanPropertyEquals( 
                                                Bucket.DATA_POLICY_ID, dataPolicyId ) ) ) ) ) );
    }
    
    
    @Override
    public void reclaimForTemporaryPersistenceRule( 
            final UUID poolId, 
            final UUID bucketId,
            final int minDaysOldToReclaim,
            final String beanPropertyToCheckForMinDaysOld )
    {
        Validations.verifyNotNull( "Pool", poolId );
        Validations.verifyNotNull( "Bucket", bucketId );
        getDataManager().deleteBeans( BlobPool.class, Require.all( 
                Require.beanPropertyEquals( BlobPool.BUCKET_ID, bucketId ),
                Require.beanPropertyEquals( BlobPool.POOL_ID, poolId ),
                Require.beanPropertyLessThan( 
                      beanPropertyToCheckForMinDaysOld, 
                      new Date( System.currentTimeMillis() - minDaysOldToReclaim * 3600L * 1000 * 24 ) ),
                Require.not( Require.exists(
                        BlobObservable.BLOB_ID,
                        JobEntry.class,
                        BlobObservable.BLOB_ID,
                        Require.nothing()))));
    }
    
    
    @Override
    public void updateLastAccessed( final Set< UUID > blobIds )
    {
        Validations.verifyNotNull( "Blob ids", blobIds );
        getDataManager().updateBeans( 
                CollectionFactory.toSet( BlobPool.LAST_ACCESSED ),
                BeanFactory.newBean( BlobPool.class ).setLastAccessed( new Date() ),
                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) );
    }
    
    
    @Override
    public void blobsLost( String error, final UUID poolId, final Set< UUID > blobIds )
    {
        getServiceManager().getService( DegradedBlobService.class ).blobsLostLocally( 
                Pool.class, BlobPool.class, poolId, error, blobIds );
    }

    
    public void blobsSuspect( final String error, final Set< BlobPool > blobTargets )
    {
        LOG.warn( blobTargets.size() + " blobs are suspected to be degraded since " + error + "." );
        
        final Set< SuspectBlobPool > beans = new HashSet<>();
        for ( final BlobPool blobTarget : blobTargets )
        {
            final SuspectBlobPool bean = BeanFactory.newBean( SuspectBlobPool.class );
            BeanCopier.copy( bean, blobTarget );
            beans.add( bean );
        }
        
        getServiceManager().getService( SuspectBlobPoolService.class ).create( beans );
    }
    
    
    @Override
	public void delete( final Set< UUID > ids )
	{
    	super.delete( ids );
		final Set< UUID > poolIds = BeanUtils.extractPropertyValues( retrieveAll( ids ).toSet() , BlobPool.POOL_ID );
		for ( final UUID poolId : poolIds )
		{
			getServiceManager().getService( PoolService.class ).updateAssignment( poolId );
		}
	}


    public void registerFailureToRead( final DiskFileInfo diskFileInfo) {
        final UUID blobPoolId = diskFileInfo.getBlobPoolId();
        if (blobPoolId == null) {
            LOG.warn("Failed to read blob from cache: " + diskFileInfo.getFilePath());
            return;
        }
        final int numFailures = m_readFailures.compute(blobPoolId, (key, value) -> (value == null) ? 1 : value + 1);
        if (numFailures >= MAX_FAILURES) {
            final BlobPool blobPool = retrieve(blobPoolId);
            if (blobPool != null) {
                try (final NestableTransaction transaction = getServiceManager().startNestableTransaction())
                {
                    transaction.getService( BlobPoolService.class ).blobsSuspect(
                            "failures reading blobs",
                            CollectionFactory.toSet(blobPool));
                    markIomMigrationsInErrorForBlob(transaction, blobPool.getBlobId());
                    transaction.commitTransaction();
                }
            }
            m_readFailures.remove(blobPoolId);
        }
    }


    private void markIomMigrationsInErrorForBlob(final NestableTransaction transaction, final UUID blobId) {
        final Set<UUID> jobIds = BeanUtils.extractPropertyValues(
                transaction.getRetriever(JobEntry.class).retrieveAll(
                        Require.all(
                                Require.beanPropertyEquals(BlobObservable.BLOB_ID, blobId),
                                Require.exists(
                                        JobEntry.JOB_ID,
                                        Require.beanPropertyEqualsOneOf(
                                                Job.IOM_TYPE,
                                                IomType.STANDARD_IOM,
                                                IomType.STAGE)))).toSet(),
                JobEntry.JOB_ID);
        if (jobIds.isEmpty()) {
            return;
        }
        final Set<DataMigration> migrations = transaction.getRetriever(DataMigration.class).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals(DataMigration.IN_ERROR, false),
                        Require.any(
                                Require.beanPropertyEqualsOneOf(DataMigration.PUT_JOB_ID, jobIds),
                                Require.beanPropertyEqualsOneOf(DataMigration.GET_JOB_ID, jobIds)))).toSet();
        for (final DataMigration migration : migrations) {
            LOG.warn("Marking data migration " + migration.getId()
                    + " in error due to suspect blob " + blobId + " on pool.");
            transaction.getUpdater(DataMigration.class).update(
                    migration.setInError(true),
                    DataMigration.IN_ERROR);
        }
    }

    private static final ConcurrentHashMap<UUID, Integer> m_readFailures = new ConcurrentHashMap<>();
    private static int MAX_FAILURES = 3;
}
