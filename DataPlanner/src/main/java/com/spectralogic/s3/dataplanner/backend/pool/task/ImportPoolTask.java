/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.dao.service.pool.ImportPoolDirectiveService;
import com.spectralogic.s3.common.dao.service.pool.PoolFailureService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.importer.BaseImportHandler;
import com.spectralogic.s3.dataplanner.backend.importer.PersistenceTargetImportHandler;
import com.spectralogic.s3.dataplanner.backend.importer.PersistenceTargetImporter;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ImportPoolTask extends BasePoolTask
{
    public ImportPoolTask( 
            final BlobStoreTaskPriority priority, 
            final ImportPoolDirective directive,
            final BlobStore blobStore,
            final BeansServiceManager serviceManager,
            final PoolEnvironmentResource m_poolEnvironmentResource,
            final PoolLockSupport<PoolTask> m_lockSupport,
            final DiskManager m_diskManager,
            final JobProgressManager m_jobProgressManager)
    {
        this( priority, directive, 10000, blobStore, serviceManager, m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager );
    }
    
    
    public ImportPoolTask(
            final BlobStoreTaskPriority priority, 
            final ImportPoolDirective directive,
            final int maxBlobsPerWorkChunk,
            final BlobStore blobStore,
            final BeansServiceManager serviceManager,
            final PoolEnvironmentResource poolEnvironmentResource,
            final PoolLockSupport<PoolTask> lockSupport,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager )
    {
        super( priority, serviceManager, poolEnvironmentResource, lockSupport, diskManager, jobProgressManager );
        m_directive = directive;
        m_maxBlobsPerWorkChunk = maxBlobsPerWorkChunk;
        m_blobStore = blobStore;
        Validations.verifyNotNull( "Directive", m_directive );
    }


    @Override
    protected UUID selectPool()
    {
        try
        {
            getLockSupport().acquireExclusiveLock( m_directive.getPoolId(), this );
            return m_directive.getPoolId();
        }
        catch ( final IllegalStateException ex )
        {
            LOG.debug( "Cannot acquire lock.", ex );
            return null;
        }
    }
    
    
    @Override
    protected void handlePreparedForExecution()
    {
        final Pool pool = getPool();
        if ( PoolState.IMPORT_PENDING != pool.getState() )
        {
            invalidateTaskAndThrow( "Pool is in state " + pool.getState() + "." );
        }
        
        getServiceManager().getService( PoolService.class ).update(
                pool.setState( PoolState.IMPORT_IN_PROGRESS ), Pool.STATE );
    }

    
    @Override
    protected BlobStoreTaskState runInternal()
    {
        final Pool pool = getPool();
        final WhereClause candidateStorageDomainFilterForPersistenceTarget = Require.beanPropertyEquals( 
                StorageDomainMember.POOL_PARTITION_ID, 
                pool.getPartitionId() );
        final ImportPoolHandler importHandler = new ImportPoolHandler();
        final PersistenceTargetImporter< BlobPool, Pool, ImportPoolDirective, PoolFailureType > importer =
                new PersistenceTargetImporter<>(
                        BlobPool.class,
                        Pool.class,
                        getPoolId(), 
                        candidateStorageDomainFilterForPersistenceTarget, 
                        ImportPoolDirectiveService.class,
                        ImportPoolDirective.POOL_ID, 
                        PoolFailureType.IMPORT_FAILED,
                        PoolFailureType.IMPORT_INCOMPLETE,
                        importHandler, 
                        getServiceManager(),
                        m_blobStore );
        try
        {
            return importer.run();
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to import pool " + getPoolId() + ".", ex );
            getServiceManager().getService( PoolService.class ).update(
                    getPool().setState( PoolState.IMPORT_PENDING ), Pool.STATE );
            return importHandler.failed( PoolFailureType.IMPORT_FAILED, ex );
        }
    }
    
    
    private final class ImportPoolHandler 
        extends BaseImportHandler< PoolFailureType >
        implements PersistenceTargetImportHandler< PoolFailureType >
    {
        @Override
        public void openForRead()
        {
            // no op
        }
        
        
        @Override
        public S3ObjectsOnMedia read()
        {
            return m_reader.getNextChunk( m_maxBlobsPerWorkChunk );
        }

        
        @Override
        public PoolFailureType verify( 
                final ImportPersistenceTargetDirective< ? > importDirective,
                final S3ObjectsOnMedia objects )
        {
            if ( !importDirective.isVerifyDataPriorToImport() )
            {
                return null;
            }
            
            final ThreadedBlobVerifier verifier = new ThreadedBlobVerifier( getPool() );
            for ( final BucketOnMedia mBucket : objects.getBuckets() )
            {
                final Bucket bucket = BeanFactory.newBean( Bucket.class );
                bucket.setName( mBucket.getBucketName() );
                BeanCopier.copy( bucket, mBucket );
                for ( final S3ObjectOnMedia mObject : mBucket.getObjects() )
                {
                    final S3Object object = BeanFactory.newBean( S3Object.class );
                    object.setName( mObject.getObjectName() );
                    BeanCopier.copy( object, mObject );
                    for ( final BlobOnMedia mBlob : mObject.getBlobs() )
                    {
                        final Blob blob = BeanFactory.newBean( Blob.class );
                        BeanCopier.copy( blob, mBlob );
                        
                        verifier.verify( bucket, object, blob );
                    }
                }
            }
            
            final Map< UUID, String > failures = verifier.getFailures();
            if ( failures.isEmpty() )
            {
                return null;
            }
            
            LOG.warn( "Failed to import pool " + getPoolId() + " due to data integrity problems: " 
                      + LogUtil.getShortVersion( failures.toString() ) );
            return PoolFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY;
        }

        
        @Override
        public void closeRead()
        {
            // no op
        }
        
        
        @Override
        public BlobStoreTaskState finalizeImport( final UUID storageDomainId, final UUID isolatedBucketId )
        {
            final Pool pool = getPool();
            try
            {
                getPoolEnvironmentResource().takeOwnershipOfPool( 
                        pool.getGuid(), pool.getId() ).get( Timeout.LONG );
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Failed to import pool " + getPoolId() + " due to take ownership failure.", ex );
                return failed( PoolFailureType.IMPORT_FAILED_DUE_TO_TAKE_OWNERSHIP_FAILURE, ex );
            }
            
            final BeansServiceManager transaction = getServiceManager().startTransaction();
            try
            {
                final UUID storageDomainMemberId = transaction.getService( StorageDomainService.class )
                        .selectAppropriateStorageDomainMember( getPool(), storageDomainId );
                transaction.getService( PoolService.class ).update( 
                        getPool().setState( PoolState.NORMAL ).setStorageDomainMemberId( storageDomainMemberId )
                        .setAssignedToStorageDomain( true )
                        .setBucketId( isolatedBucketId ),
                        Pool.STATE, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                        PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
                transaction.getService( PoolFailureService.class ).deleteAll( 
                        getPoolId(), PoolFailureType.IMPORT_FAILED );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
            return BlobStoreTaskState.COMPLETED;
        }
    
    
        @Override public void verifyCompatibleStorageDomain( final UUID storageDomainId )
        {
        }
    
    
        @Override
        public BlobStoreTaskState failedInternal(
                final PoolFailureType failureType,
                final RuntimeException ex )
        {
            if ( !m_generatedPoolFailure )
            {
                m_generatedPoolFailure = true;
                getServiceManager().getService( PoolFailureService.class ).create(
                        getPoolId(), failureType, ex );
            }
            
            final BeansServiceManager transaction = getServiceManager().startTransaction();
            try
            {
                transaction.getService( PoolService.class ).update(
                        getPool().setState( PoolState.FOREIGN ), Pool.STATE );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
            return BlobStoreTaskState.COMPLETED;
        }
        
        
        @Override
        public void warnInternal(
                final PoolFailureType failureType,
                final RuntimeException ex )
        { 
                getServiceManager().getService( PoolFailureService.class ).create(
                        getPoolId(), failureType, ex );
        }
        
        
        private volatile boolean m_generatedPoolFailure;
        private final PoolApplicationContentsReader m_reader =
                new PoolApplicationContentsReader( getPool() );
    } // end inner class def
    

    @Override
    public String getDescription()
    {
        return "Import Pool " + m_directive.getPoolId();
    }
    
    
    private final ImportPoolDirective m_directive;
    private final int m_maxBlobsPerWorkChunk;
    private final BlobStore m_blobStore;
}
