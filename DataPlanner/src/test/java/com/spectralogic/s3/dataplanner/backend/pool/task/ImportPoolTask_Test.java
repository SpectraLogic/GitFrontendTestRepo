/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.pool.PoolLockSupportImpl;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.s3.dataplanner.testfrmwrk.MockPoolPersistence;
import com.spectralogic.s3.dataplanner.testfrmwrk.PoolTaskBuilder;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public final class ImportPoolTask_Test
{
    @Test
    public void testImportWhenPoolInWrongStateForImportNotAllowed()
    {
        
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.FOREIGN ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                getBlobStore()
        );
        
        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class, new BlastContainer()
        {
    public void test() throws Throwable
            {
                task.prepareForExecutionIfPossible();
            }
        } );
        pp.shutdown();
    }
    
    
    @Test
    public void testImportWhenCannotLockPoolResultsInTaskWillNotExecuteYet()
    {
        
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        lockSupport.acquireExclusiveLock( 
                pp.getPool().getId(),
                InterfaceProxyFactory.getProxy( PoolTask.class, null ) );
        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                getBlobStore()
        );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda noted task isn't pending execution.");
        assertEquals(PoolState.IMPORT_PENDING, poolService.attain( pool.getId() ).getState(), "Should notta executed.");
        assertFalse(takeOwnershipCalled.get(), "Should notta executed.");

        pp.shutdown();
    }
    
    
    @Test
    public void testImportWhenNoImportDirectiveResultsInImportAborted()
    {
        
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                getBlobStore()
        );
        task.prepareForExecutionIfPossible();
        
        mockDaoDriver.deleteAll( ImportPoolDirective.class );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda aborted attempt to import pool.");
        assertEquals(PoolState.FOREIGN, poolService.attain( pool.getId() ).getState(), "Should notta executed.");
        assertFalse(takeOwnershipCalled.get(), "Should notta executed.");

        pp.shutdown();
    }
    
    
    @Test
    public void testImportWhenObjectBlobCountMetadataMissingNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final List< S3ObjectsOnMedia > contents = 
                constructResponses( Integer.MAX_VALUE, Integer.MAX_VALUE, false );
        buildPersistence( pp, contents );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                getBlobStore()
        );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(PoolState.FOREIGN, poolService.attain( pool.getId() ).getState(), "Should notta successfully imported pool.");
        assertFalse(takeOwnershipCalled.get(), "Should notta taken ownership of pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda been import failure.");

        pp.shutdown();
    }
    
    
    @Test
    public void testImportWhenSinglePageOfResultsAndEverythingImportedIsNewWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BucketService bucketService = dbSupport.getServiceManager().getService( BucketService.class );
        bucketService.initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final List< S3ObjectsOnMedia > contents = constructResponses( Integer.MAX_VALUE );
        buildPersistence( pp, contents );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                getBlobStore()
        );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(PoolState.NORMAL, poolService.attain( pool.getId() ).getState(), "Shoulda successfully imported pool.");
        assertTrue(takeOwnershipCalled.get(), "Shoulda taken ownership of pool.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Should notta been any failures.");
        final Object expected = poolService.attain( pool.getId() ).getUsedCapacity();
        assertEquals(expected, bucketService.getLogicalSizeCache().getSize( null ), "Should have updated the the bucket service's logical cache size to be the same size as the "+
                        "imported pool.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        pp.shutdown();
    }
    
    
    @Test
    public void testImportWhenMultiplePagesOfResultsAndEverythingImportedIsNewWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final List< S3ObjectsOnMedia > contents = constructResponses( Integer.MAX_VALUE );
        buildPersistence( pp, contents );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );

        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                5,
                getBlobStore());
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(PoolState.NORMAL, poolService.attain( pool.getId() ).getState(), "Shoulda successfully imported pool.");
        assertTrue(takeOwnershipCalled.get(), "Shoulda taken ownership of pool.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        pp.shutdown();
    }
    
    
    @Test
    public void testImportWhenDataIntegrityProblemFailsImport()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BucketService bucketService = dbSupport.getServiceManager().getService( BucketService.class );
        bucketService.initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final List< S3ObjectsOnMedia > contents = constructResponses( Integer.MAX_VALUE );
        contents.get( 0 ).getBuckets()[ 1 ].getObjects()[ 1 ].getBlobs()[ 0 ].setChecksum( "wrong" );
        buildPersistence( pp, contents );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                getBlobStore()
        );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(PoolState.FOREIGN, poolService.attain( pool.getId() ).getState(), "Shoulda failed to import pool.");
        assertFalse(takeOwnershipCalled.get(), "Should notta tried to take ownership of pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda been a failure.");
        assertEquals(0,  bucketService.getLogicalSizeCache().getSize(null), "Should not have added blob sizes to bucket service's logical cache.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Bucket.class).getCount(), "Should notta imported pool.");
        pp.shutdown();
    }
    
    
    @Test
    public void testImportWhenTakeOwnershipFailsFailsImport()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition poolPartition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, poolPartition.getId() );
        final List< S3ObjectsOnMedia > contents = constructResponses( Integer.MAX_VALUE );
        buildPersistence( pp, contents );
        final Pool pool = pp.getPool();
        
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );
        poolService.update( pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
        final ImportPoolDirective directive =
                mockDaoDriver.createImportPoolDirective( pool.getId(), user.getId(), dataPolicy.getId() );
        
        final AtomicBoolean takeOwnershipCalled = new AtomicBoolean();
        final ImportPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(createPoolEnvironmentResource( takeOwnershipCalled, true ))
                .buildImportPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], 
                directive,
                getBlobStore()
        );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(PoolState.FOREIGN, poolService.attain( pool.getId() ).getState(), "Shoulda failed to import pool.");
        assertTrue(takeOwnershipCalled.get(), "Shoulda tried to take ownership of pool.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda been a failure.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        pp.shutdown();
    }
    
    
    private void verifyEverythingHasBeenImported( final BeansServiceManager serviceManager )
    {
        final Bucket b2 = serviceManager.getRetriever( Bucket.class ).attain(
                Bucket.NAME, "b_2" );
        final Bucket b3 = serviceManager.getRetriever( Bucket.class ).attain( 
                Bucket.NAME, "b_3" );
        final S3Object o00 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_0blobs_0present" ); // b_1
        final S3Object o10 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_1blobs_0present" ); // b_2
        final S3Object o11 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_1blobs_1present" ); // b_2
        final S3Object o20 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_0present" ); // b_3
        final S3Object o21 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_1present" ); // b_3
        final S3Object o22 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_2present" ); // b_3
        assertEquals(2,  serviceManager.getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.BUCKET_ID, b2.getId())), "Shoulda imported all contents.");
        assertEquals(3,  serviceManager.getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.BUCKET_ID, b3.getId())), "Shoulda imported all contents.");

        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o00.getId())), "Shoulda imported all contents.");
        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o10.getId())), "Shoulda imported all contents.");
        assertEquals(1,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o11.getId())), "Shoulda imported all contents.");
        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o20.getId())), "Shoulda imported all contents.");
        assertEquals(1,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o21.getId())), "Shoulda imported all contents.");
        assertEquals(2,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o22.getId())), "Shoulda imported all contents.");

        final Object expected2 = b3.getId();
        assertEquals(expected2, o20.getBucketId(), "Shoulda imported all contents.");
        final Object expected1 = b3.getId();
        assertEquals(expected1, o21.getBucketId(), "Shoulda imported all contents.");
        final Object expected = b3.getId();
        assertEquals(expected, o22.getBucketId(), "Shoulda imported all contents.");

        assertEquals(108000,  o20.getCreationDate().getTime(), "Shoulda imported all contents.");
        assertEquals(110000,  o21.getCreationDate().getTime(), "Shoulda imported all contents.");
        assertEquals(113000,  o22.getCreationDate().getTime(), "Shoulda imported all contents.");

        final Object actual2 = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o20.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual2, "Shoulda imported all contents.");
        assertEquals("109", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o20.getId() ).getValue(), "Shoulda imported all contents.");
        final Object actual1 = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o21.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual1, "Shoulda imported all contents.");
        assertEquals("111", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o21.getId() ).getValue(), "Shoulda imported all contents.");
        final Object actual = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o22.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual, "Shoulda imported all contents.");
        assertEquals("114", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o22.getId() ).getValue(), "Shoulda imported all contents.");

        final List< Blob > blobs = new ArrayList<>( BeanUtils.sort(
                serviceManager.getRetriever( Blob.class ).retrieveAll( 
                        Blob.OBJECT_ID, o22.getId() ).toSet() ) );
        assertEquals(2,  blobs.size(), "Shoulda imported all contents.");
        assertEquals(0,  blobs.get(0).getByteOffset(), "Shoulda imported all contents.");
        assertEquals(1000,  blobs.get(0).getLength(), "Shoulda imported all contents.");
        assertEquals("2YXMb+EElVbEBf1D0wkWjg==", blobs.get( 0 ).getChecksum(), "Shoulda imported all contents.");
        assertEquals(ChecksumType.MD5, blobs.get( 0 ).getChecksumType(), "Shoulda imported all contents.");
        assertEquals(1000,  blobs.get(1).getByteOffset(), "Shoulda imported all contents.");
        assertEquals(1000,  blobs.get(1).getLength(), "Shoulda imported all contents.");
        assertEquals("2YXMb+EElVbEBf1D0wkWjg==", blobs.get( 1 ).getChecksum(), "Shoulda imported all contents.");
        assertEquals(ChecksumType.MD5, blobs.get( 1 ).getChecksumType(), "Shoulda imported all contents.");

        assertEquals(0,  serviceManager.getRetriever(ImportPoolDirective.class).getCount(Require.nothing()), "Shoulda whacked import directive from db.");
        assertEquals(20,  serviceManager.getRetriever(BlobPool.class).getCount(Require.nothing()), "Shoulda recorded blobs on pool.");

        assertNull(
                serviceManager.getRetriever( Bucket.class ).retrieve( Bucket.NAME, PoolUtils.TRASH_DIRECTORY ),
                "Should not have imported trash directory."
                );
    }
    
    
    private void buildPersistence( final MockPoolPersistence pp, final List< S3ObjectsOnMedia > allContents )
    {
        for ( final S3ObjectsOnMedia mContents : allContents )
        {
            for ( final BucketOnMedia mBucket : mContents.getBuckets() )
            {
                final Bucket bucket = BeanFactory.newBean( Bucket.class );
                bucket.setName( mBucket.getBucketName() );
                BeanCopier.copy( bucket, mBucket );
                pp.create( bucket );
                for ( final S3ObjectOnMedia mObject : mBucket.getObjects() )
                {
                    final S3Object object = BeanFactory.newBean( S3Object.class );
                    object.setName( mObject.getObjectName() );
                    object.setBucketId( bucket.getId() );
                    BeanCopier.copy( object, mObject );
                    pp.create( bucket, object, mObject.getMetadata() );
                    for ( final BlobOnMedia mBlob : mObject.getBlobs() )
                    {
                        final Blob blob = BeanFactory.newBean( Blob.class );
                        blob.setObjectId( object.getId() );
                        blob.setByteOffset( mBlob.getOffset() );
                        BeanCopier.copy( blob, mBlob );
                        pp.create( bucket, object, blob );
                    }
                }
            }
        }
    }
    
    
    private List< S3ObjectsOnMedia > constructResponses( final int maxBucketsPerResponse )
    {
        return constructResponses( maxBucketsPerResponse, Integer.MAX_VALUE, true );
    }
    
    
    private List< S3ObjectsOnMedia > constructResponses(
            final int maxBucketsPerResponse, 
            final int zeroLengthBlobStep,
            final boolean includeBlobCountMetadata )
    {
        int checksum = 100;
        final List< S3ObjectOnMedia > ooms = new ArrayList<>();
        for ( int i = 0; i < 5; ++i )
        {
            for ( int j = 0; j <= i; ++j )
            {
                final List< S3ObjectMetadataKeyValue > metadatas = new ArrayList<>();
                metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( KeyValueObservable.CREATION_DATE )
                        .setValue( String.valueOf( ( ++checksum ) * 1000 ) ) );
                metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                        .setValue( String.valueOf( ++checksum ) ) );
                
                final List< BlobOnMedia > bomsForObject = new ArrayList<>();
                final S3ObjectOnMedia oom = BeanFactory.newBean( S3ObjectOnMedia.class );
                oom.setId( UUID.randomUUID() );
                oom.setObjectName( "o_" + String.valueOf( i ) + "blobs_" + String.valueOf( j ) + "present" );
                if ( ( 0 < checksum % zeroLengthBlobStep ) )
                {
                    for ( int k = i - j; k < i; ++k )
                    {
                        ++checksum;
                        final BlobOnMedia bom = BeanFactory.newBean( BlobOnMedia.class );
                        bom.setChecksum( "2YXMb+EElVbEBf1D0wkWjg==" );
                        bom.setChecksumType( ChecksumType.MD5 );
                        bom.setId( UUID.randomUUID() );
                        bom.setLength( 1000 );
                        bom.setOffset( k * 1000 );
                        bomsForObject.add( bom );
                    }
                }
                else
                {
                    ++checksum;
                    final BlobOnMedia bom = BeanFactory.newBean( BlobOnMedia.class );
                    bom.setChecksum( "2YXMb+EElVbEBf1D0wkWjg==" );
                    bom.setChecksumType( ChecksumType.MD5 );
                    bom.setId( UUID.randomUUID() );
                    bom.setLength( 0 );
                    bom.setOffset( 0 );
                    bomsForObject.add( bom );
                }
                if ( includeBlobCountMetadata )
                {
                    metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                            .setKey( KeyValueObservable.TOTAL_BLOB_COUNT )
                            .setValue( String.valueOf( bomsForObject.size() ) ) );
                }
                oom.setMetadata( CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
                oom.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, bomsForObject ) );
                ooms.add( oom );
            }
        }
        
        final List< S3ObjectOnMedia > oomsInBucket = new ArrayList<>();
        final List< BucketOnMedia > buckets = new ArrayList<>();
        for ( final S3ObjectOnMedia oom : ooms )
        {
            oomsInBucket.add( oom );
            if ( buckets.size() < oomsInBucket.size() )
            {
                final BucketOnMedia bucket = BeanFactory.newBean( BucketOnMedia.class );
                bucket.setBucketName( "b_" + oomsInBucket.size() );
                bucket.setObjects( CollectionFactory.toArray( S3ObjectOnMedia.class, oomsInBucket ) );
                buckets.add( bucket );
                oomsInBucket.clear();
            }
        }

        final BucketOnMedia trashBucket = BeanFactory.newBean( BucketOnMedia.class );
        trashBucket.setBucketName( PoolUtils.TRASH_DIRECTORY );
        trashBucket.setObjects( CollectionFactory.toArray( S3ObjectOnMedia.class, CollectionFactory.toList() ) );
        buckets.add( trashBucket );
            
        final List< BucketOnMedia > bucketsInResponse = new ArrayList<>();
        final List< S3ObjectsOnMedia > retval = new ArrayList<>();
        for ( final BucketOnMedia bucket : buckets )
        {
            bucketsInResponse.add( bucket );
            if ( bucketsInResponse.size() == maxBucketsPerResponse )
            {
                final S3ObjectsOnMedia response = BeanFactory.newBean( S3ObjectsOnMedia.class );
                response.setBuckets( CollectionFactory.toArray( BucketOnMedia.class, bucketsInResponse ) );
                retval.add( response );
            }
        }
        if ( !bucketsInResponse.isEmpty() )
        {
            final S3ObjectsOnMedia response = BeanFactory.newBean( S3ObjectsOnMedia.class );
            response.setBuckets( CollectionFactory.toArray( BucketOnMedia.class, bucketsInResponse ) );
            retval.add( response );
            bucketsInResponse.clear();
        }
        
        return retval;
    }
    
    
    private PoolEnvironmentResource createPoolEnvironmentResource( final AtomicBoolean takeOwnershipCalled )
    {
        return createPoolEnvironmentResource( takeOwnershipCalled, false );
    }
    
    
    private PoolEnvironmentResource createPoolEnvironmentResource( 
            final AtomicBoolean takeOwnershipCalled,
            final boolean fail )
    {
        Validations.verifyNotNull( "Take ownership called", takeOwnershipCalled );
        final Method method = ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        return InterfaceProxyFactory.getProxy(
                PoolEnvironmentResource.class,
                MockInvocationHandler.forMethod(
                        method, 
                        new InvocationHandler()
                        {
                            public Object invoke( final Object proxy, final Method m, final Object[] args )
                                    throws Throwable
                            {
                                if ( takeOwnershipCalled.getAndSet( true ) )
                                {
                                    throw new IllegalStateException( "Take ownership already called." );
                                }
                                if ( fail )
                                {
                                    throw new RpcProxyException(
                                            "Oops.", BeanFactory.newBean( Failure.class ) );
                                }
                                return new RpcResponse<>();
                            }
                        },
                        null ) );
    }
    
    
    private PoolLockSupport< PoolTask > createLockSupport()
    {
        return new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }
    
    
    private BlobStore getBlobStore()
    {
        return InterfaceProxyFactory.getProxy( BlobStore.class, null );
    }
    private static DatabaseSupport dbSupport;

    @BeforeAll
    public static void setUpDB() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void setUp() {
        dbSupport.reset();
    }
}
