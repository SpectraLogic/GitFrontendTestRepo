/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.s3.dataplanner.backend.pool.task.ImportPoolTask;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class PoolBlobStoreImpl_Test 
{
    @Test
    public void testConstructorNullRpcClientNotAllowed()
    {


        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PoolBlobStoreImpl(
                        null,
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                        dbSupport.getServiceManager() );
            }
        } );
    }
    

    @Test
    public void testConstructorNullCacheManagerNotAllowed()
    {


        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PoolBlobStoreImpl(
                        getRpcClient(),
                        null,
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                        dbSupport.getServiceManager() );
            }
        } );
    }
    

    @Test
    public void testConstructorNullJobProgressManagerNotAllowed()
    {


        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PoolBlobStoreImpl(
                        getRpcClient(),
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        null,
                        dbSupport.getServiceManager() );
            }
        } );
    }
    

    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PoolBlobStoreImpl(
                        getRpcClient(),
                        new MockDiskManager( null ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {


        new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
    }
    
    
    @Test
    public void testConstructorReschedulesImportsAsNecessaryWhenPoolWasImportPending()
    {


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID userId = mockDaoDriver.createUser( "user" ).getId();
        final UUID dataPolicyId = mockDaoDriver.createDataPolicy( "dp" ).getId();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "partition" );
        final Pool pool1 = mockDaoDriver.createPool( partition.getId(), PoolState.IMPORT_PENDING );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition.getId() );
        mockDaoDriver.createDataPersistenceRule(dataPolicyId, DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createImportPoolDirective( pool1.getId(), userId, dataPolicyId );
        final Pool pool2 = mockDaoDriver.createPool( null, PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        assertEquals(1,  store.getTasks().size(), "Shoulda scheduled imports for non-foreign pools.");
        assertEquals(PoolState.IMPORT_PENDING, mockDaoDriver.attain( pool1 ).getState(), "Shoulda re-scheduled import.");
        assertEquals(PoolState.FOREIGN, mockDaoDriver.attain( pool2 ).getState(), "Shoulda re-scheduled import.");
    }
    
    
    @Test
    public void testConstructorReschedulesImportsAsNecessaryWhenPoolWasImportInProgress()
    {


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID userId = mockDaoDriver.createUser( "user" ).getId();
        final UUID dataPolicyId = mockDaoDriver.createDataPolicy( "dp" ).getId();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "partition" );
        final Pool pool1 = mockDaoDriver.createPool( partition.getId(), PoolState.IMPORT_IN_PROGRESS );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition.getId() );
        mockDaoDriver.createDataPersistenceRule(dataPolicyId, DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createImportPoolDirective( pool1.getId(), userId, dataPolicyId );
        final Pool pool2 = mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        assertEquals(1,  store.getTasks().size(), "Shoulda scheduled imports for non-foreign pools.");
        assertEquals(PoolState.IMPORT_PENDING, mockDaoDriver.attain( pool1 ).getState(), "Shoulda re-scheduled import.");
        assertEquals(PoolState.FOREIGN, mockDaoDriver.attain( pool2 ).getState(), "Shoulda re-scheduled import.");
    }
    
    
    @Test
    public void testGetLockSupportReturnsNonNull()
    {


        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        assertNotNull(store.getLockSupport(), "Shoulda returned lock support.");
    }
    

    @Test
    public void testImportNullPoolImportsAllEligiblePools()
    {


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID userId = mockDaoDriver.createUser( "user" ).getId();
        final UUID dataPolicyId = mockDaoDriver.createDataPolicy( "dp" ).getId();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "partition" );
        mockDaoDriver.createPool( partition.getId(), PoolState.NORMAL );
        mockDaoDriver.createPool( partition.getId(), PoolState.NORMAL );
        mockDaoDriver.createPool( partition.getId(), PoolState.LOST );
        mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition.getId() );
        mockDaoDriver.createDataPersistenceRule( dataPolicyId, DataPersistenceRuleType.PERMANENT, sd.getId() );
        
        
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        mockDaoDriver.createPool( PoolState.IMPORT_IN_PROGRESS );
        store.importPool( 
                BlobStoreTaskPriority.HIGH, 
                BeanFactory.newBean( ImportPoolDirective.class )
                .setDataPolicyId( dataPolicyId ).setUserId( userId ) );
        assertEquals(3,  store.getTasks().size(), "Shoulda scheduled import for all eligible pools.");
    }
    

    @Test
    public void testImportNonForeignPoolNotAllowed()
    {


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID userId = mockDaoDriver.createUser( "user" ).getId();
        final UUID dataPolicyId = mockDaoDriver.createDataPolicy( "dp" ).getId();
        final Pool pool = mockDaoDriver.createPool( null );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                store.importPool( 
                        BlobStoreTaskPriority.LOW, 
                        BeanFactory.newBean( ImportPoolDirective.class )
                        .setPoolId( pool.getId() ).setUserId( userId ).setDataPolicyId( dataPolicyId ) );
            }
        } );
    }
    

    @Test
    public void testImportPoolDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID userId = mockDaoDriver.createUser( "user" ).getId();
        final UUID userId2 = mockDaoDriver.createUser( "user2" ).getId();
        final UUID dataPolicyId = mockDaoDriver.createDataPolicy( "dp" ).getId();
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "partition" );
        final Pool pool = mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition.getId() );
        mockDaoDriver.createDataPersistenceRule( dataPolicyId, DataPersistenceRuleType.PERMANENT, sd.getId() );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.importPool(
                BlobStoreTaskPriority.LOW, 
                BeanFactory.newBean( ImportPoolDirective.class )
                .setPoolId( pool.getId() ).setUserId( userId ).setDataPolicyId( dataPolicyId ) );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single import pool task for single pool being compacted.");
        assertEquals(BlobStoreTaskPriority.LOW, store.getTasks().iterator().next().getPriority(), "Shoulda set import pool task priority correctly.");

        store.cancelImportPool( pool.getId() );
        store.importPool(
                BlobStoreTaskPriority.LOW, 
                BeanFactory.newBean( ImportPoolDirective.class )
                .setPoolId( pool.getId() ).setUserId( userId ).setDataPolicyId( dataPolicyId ) );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single import pool task for single pool being compacted.");
        assertEquals(BlobStoreTaskPriority.LOW, store.getTasks().iterator().next().getPriority(), "Shoulda set import pool task priority correctly.");
        assertEquals(PoolState.IMPORT_PENDING, mockDaoDriver.attain( pool ).getState(), "Shoulda queued import.");

        store.cancelImportPool( pool.getId() );
        store.importPool(
                BlobStoreTaskPriority.NORMAL, 
                BeanFactory.newBean( ImportPoolDirective.class )
                .setPoolId( pool.getId() ).setUserId( userId2 ).setDataPolicyId( dataPolicyId ) );
        assertEquals(1, store.getTasks().size(), "Shoulda had single import pool task for single pool being compacted.");
        assertEquals(BlobStoreTaskPriority.NORMAL, store.getTasks().iterator().next().getPriority(), "Shoulda set import pool task priority correctly.");
        assertEquals(PoolState.IMPORT_PENDING, mockDaoDriver.attain( pool ).getState(), "Shoulda queued import.");
    }
    
    
    @Test
    public void testCancelImportForPoolThatWasImportPendingDoesSo()
    {
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final PoolBlobStore store = new PoolBlobStoreImpl( 
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ), 
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "partition" );
        Pool pool = mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition.getId() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final ImportPoolDirective directive = mockDaoDriver.createImportPoolDirective(
                pool.getId(),
                user.getId(), 
                dp.getId() );
        store.importPool(
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
        pool = service.attain( pool.getId() );
        assertEquals(PoolState.IMPORT_PENDING, pool.getState(), "Shoulda been import pending.");

        assertTrue(store.cancelImportPool( pool.getId() ), "Shoulda succeeded in cancelling the pool import.");

        pool = service.attain( pool.getId() );
        assertEquals(PoolState.FOREIGN, pool.getState(), "Shoulda succeeded in cancelling the pool import.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(ImportPoolDirective.class).getCount(), "Shoulda succeeded in cancelling the pool import.");
    }
    
    
    @Test
    public void testCancelImportForPoolThatWasImportInProgressDoesNotCancelImport()
    {
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final PoolBlobStore store = new PoolBlobStoreImpl( 
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ), 
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "partition" );
        Pool pool = mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition.getId() );
        mockDaoDriver.createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final ImportPoolDirective directive = mockDaoDriver.createImportPoolDirective(
                pool.getId(),
                user.getId(), 
                dp.getId() );
        store.importPool(
                BlobStoreTaskPriority.values()[ 1 ], 
                directive );
        pool = service.attain( pool.getId() );
        assertEquals(PoolState.IMPORT_PENDING, pool.getState(), "Shoulda been import pending.");
        ( (ImportPoolTask)store.getTasks().iterator().next() ).prepareForExecutionIfPossible();

        assertFalse(store.cancelImportPool( pool.getId() ), "Should notta succeeded in cancelling the pool import since its task has started.");

        pool = service.attain( pool.getId() );
        assertEquals(PoolState.IMPORT_IN_PROGRESS, pool.getState(), "Should notta succeeded in cancelling the pool import since its task has started.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(ImportPoolDirective.class).getCount(), "Should notta succeeded in cancelling the pool import.");
    }
    
    
    @Test
    public void testCancelImportForPoolThatWasNeverImportPendingDoesNotCancelImport()
    {
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final PoolBlobStore store = new PoolBlobStoreImpl( 
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ), 
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );

        assertFalse(store.cancelImportPool( pool.getId() ), "Should notta succeeded in cancelling the pool import since import never queued.");

        pool = service.attain( pool.getId() );
        assertEquals(PoolState.NORMAL, pool.getState(), "Should notta succeeded in cancelling the pool import since import never queued.");
    }
    

    @Test
    public void testCompactNullPoolCompactsAllEligiblePools()
    {


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        mockDaoDriver.createPool( PoolState.LOST );
        mockDaoDriver.createPool( PoolState.FOREIGN );
        final Pool quiescedPool = mockDaoDriver.createPool();
        quiescedPool.setQuiesced(Quiesced.YES);
        dbSupport.getServiceManager().getService( PoolService.class ).update(
                quiescedPool.setQuiesced( Quiesced.YES ), Pool.QUIESCED);
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        mockDaoDriver.createPool( PoolState.IMPORT_IN_PROGRESS );
        store.compactPool( BlobStoreTaskPriority.values()[ 0 ], null );
        assertEquals(2,  store.getTasks().size(), "Shoulda scheduled compaction for all eligible pools.");
    }
    

    @Test
    public void testFormatPoolNotInProperStateNotAllowed()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodTakeOwnership = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        final Method methodFormat =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "formatPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
                {
                    store.formatPool( pool.getId() );
                }
            } );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodTakeOwnership, Integer.valueOf( 0 ) );
        expectedCalls.put( methodFormat, Integer.valueOf( 0 ) );
        btih.verifyMethodInvocations( expectedCalls );
        final Object expected = pool.getState();
        assertEquals(expected, mockDaoDriver.attain( pool ).getState(), "Should notta updated pool state.");
    }
    

    @Test
    public void testFormatPoolWhenCannotAcquireLockOnPoolNotAllowed()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodTakeOwnership = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        final Method methodFormat =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "formatPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        final PoolTask lockHolder = InterfaceProxyFactory.getProxy( PoolTask.class, null );
        store.getLockSupport().acquireReadLock( pool.getId(), lockHolder );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test()
                {
                    store.formatPool( pool.getId() );
                }
            } );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodTakeOwnership, Integer.valueOf( 0 ) );
        expectedCalls.put( methodFormat, Integer.valueOf( 0 ) );
        btih.verifyMethodInvocations( expectedCalls );
        final Object expected = pool.getState();
        assertEquals(expected, mockDaoDriver.attain( pool ).getState(), "Should notta updated pool state.");
    }
    

    @Test
    public void testFormatPoolWhenFailedToFormatThrows()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodTakeOwnership = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        final Method methodFormat =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "formatPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                if ( methodFormat.equals( method ) )
                {
                    throw new RpcProxyException( "oops", BeanFactory.newBean( Failure.class ) );
                }
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
                {
                    store.formatPool( pool.getId() );
                }
            } );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodTakeOwnership, Integer.valueOf( 0 ) );
        expectedCalls.put( methodFormat, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCalls );
        final Object expected = pool.getState();
        assertEquals(expected, mockDaoDriver.attain( pool ).getState(), "Should notta updated pool state.");
    }
    

    @Test
    public void testFormatPoolWhenFailedToTakeOwnershipThrows()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodTakeOwnership = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        final Method methodFormat =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "formatPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                if ( methodTakeOwnership.equals( method ) )
                {
                    throw new RpcProxyException( "oops", BeanFactory.newBean( Failure.class ) );
                }
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
                {
                    store.formatPool( pool.getId() );
                }
            } );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodTakeOwnership, Integer.valueOf( 1 ) );
        expectedCalls.put( methodFormat, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCalls );
        final Object expected = pool.getState();
        assertEquals(expected, mockDaoDriver.attain( pool ).getState(), "Should notta updated pool state.");
    }
    

    @Test
    public void testFormatPoolDoesSo()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodTakeOwnership = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        final Method methodFormat =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "formatPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.formatPool( pool.getId() );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodTakeOwnership, Integer.valueOf( 1 ) );
        expectedCalls.put( methodFormat, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCalls );
        assertEquals(PoolState.NORMAL, mockDaoDriver.attain( pool ).getState(), "Shoulda updated pool state.");
    }
    

    @Test
    public void testFormatPoolNullPoolIdFormatsAllPoolsEligibleForFormatting()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodTakeOwnership = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        final Method methodFormat =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "formatPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createPool( PoolState.FOREIGN );
        mockDaoDriver.createPool( PoolState.FOREIGN );
        mockDaoDriver.createPool( PoolState.NORMAL );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.formatPool( null );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodTakeOwnership, Integer.valueOf( 2 ) );
        expectedCalls.put( methodFormat, Integer.valueOf( 2 ) );
        btih.verifyMethodInvocations( expectedCalls );
    }
    

    @Test
    public void testDestroyPoolUsedByStorageDomainNotAllowed()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodDestroy =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "destroyPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), pool.getPartitionId() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );
        mockDaoDriver.updateBean( 
                pool.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
                {
                    store.destroyPool( pool.getId() );
                }
            } );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodDestroy, Integer.valueOf( 0 ) );
        btih.verifyMethodInvocations( expectedCalls );
        
        mockDaoDriver.attain( pool );
    }
    

    @Test
    public void testDestroyPoolWhenCannotAcquireLockOnPoolNotAllowed()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodDestroy =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "destroyPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        final PoolTask lockHolder = InterfaceProxyFactory.getProxy( PoolTask.class, null );
        store.getLockSupport().acquireReadLock( pool.getId(), lockHolder );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test()
                {
                    store.destroyPool( pool.getId() );
                }
            } );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodDestroy, Integer.valueOf( 0 ) );
        btih.verifyMethodInvocations( expectedCalls );
        
        mockDaoDriver.attain( pool );
    }
    

    @Test
    public void testDestroyPoolWhenFailedToDestroyThrows()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodDestroy =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "destroyPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                if ( methodDestroy.equals( method ) )
                {
                    throw new RpcProxyException( "oops", BeanFactory.newBean( Failure.class ) );
                }
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
                {
                    store.destroyPool( pool.getId() );
                }
            } );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodDestroy, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCalls );
        
        mockDaoDriver.attain( pool );
    }
    

    @Test
    public void testGetPoolFileForSelectsAppropriatePoolToReadFrom() throws IOException {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Path temp = Files.createTempDirectory("pool");
        
        final Pool pool1 = mockDaoDriver.createPool(
                PoolType.NEARLINE, temp.resolve("archon").toString(), null, PoolState.NORMAL, true );
        final Pool pool2 = mockDaoDriver.createPool(
                PoolType.NEARLINE, temp.resolve("archoff").toString(), null, PoolState.NORMAL, false );
        final Pool pool3 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("enton").toString(), null, PoolState.NORMAL, true );
        final Pool pool4 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("entoff").toString(), null, PoolState.NORMAL, false );
        final Pool pool5 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("entlost1").toString(), null, PoolState.LOST, true );
        final Pool pool6 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("entlost2").toString(), null, PoolState.LOST, false );
        mockDaoDriver.putBlobOnPool( pool1.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), blob.getId() );
        final BlobPool blobPool = mockDaoDriver.putBlobOnPool( pool3.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool4.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool5.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool6.getId(), blob.getId() );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        final PoolBlobStore poolBlobStore = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                cacheManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );

        final Path expectedPath = PoolUtils.getPath( pool3, bucket.getName(), object.getId(), blob.getId() );
        Files.createDirectories(expectedPath.getParent());
        RandomAccessFile f = new RandomAccessFile(expectedPath.toString(), "rw");
        f.setLength(blob.getLength());

        final Object actual = poolBlobStore.getPoolFileFor( blob.getId() ).getFilePath();
        assertEquals(expectedPath.toString(), actual, "Shoulda preferred enterprise over archive, and powered on over powered off.");
        final Object expected = blobPool.getId();
        assertEquals(expected, poolBlobStore.getPoolFileFor( blob.getId() ).getBlobPoolId(), "Shoulda chosen correct blob pool id");
        assertEquals(1,  btih.getTotalCallCount(), "Shoulda made call on pool lock support to acquire lock on blob.");
    }
    
    
    @Test
    public void testGetPoolFileWhenBlobNotInCacheButIsOnPoolStorageWhereFirstChoiceUnavailableAllowed() throws IOException {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Path temp = Files.createTempDirectory("pool");
        
        final Pool pool1 = mockDaoDriver.createPool(
                PoolType.NEARLINE, temp.resolve("archon").toString(), null, PoolState.NORMAL, true );
        final Pool pool2 = mockDaoDriver.createPool(
                PoolType.NEARLINE, temp.resolve("archoff").toString(), null, PoolState.NORMAL, false );
        final Pool pool3 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("enton").toString(), null, PoolState.NORMAL, true );
        final Pool pool4 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("entoff").toString(), null, PoolState.NORMAL, false );
        final Pool pool5 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("entlost1").toString(), null, PoolState.LOST, true );
        final Pool pool6 = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("entlost2").toString(), null, PoolState.LOST, false );
        mockDaoDriver.putBlobOnPool( pool1.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool3.getId(), blob.getId() );
        final BlobPool blobPool = mockDaoDriver.putBlobOnPool( pool4.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool5.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool6.getId(), blob.getId() );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        final PoolBlobStore poolBlobStore = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                cacheManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        poolBlobStore.getLockSupport().acquireExclusiveLock(
                pool3.getId(),
                InterfaceProxyFactory.getProxy(PoolTask.class, null) );

        final Path expectedPath = PoolUtils.getPath( pool4, bucket.getName(), object.getId(), blob.getId() );
        Files.createDirectories(expectedPath.getParent());
        RandomAccessFile f = new RandomAccessFile(expectedPath.toString(), "rw");
        f.setLength(blob.getLength());

        final Object actual = poolBlobStore.getPoolFileFor( blob.getId() ).getFilePath();
        assertEquals(expectedPath.toString(), actual, "Shoulda preferred enterprise over archive, and powered on over powered off.");
        final Object expected = blobPool.getId();
        assertEquals(expected, poolBlobStore.getPoolFileFor( blob.getId() ).getBlobPoolId(), "Shoulda chosen correct blob pool id");
    }
    
    
    @Test
    public void testStartBlobReadWhenBlobNotInCacheButIsOnPoolButNotOnlinePoolStorageNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Pool pool1 = mockDaoDriver.createPool(
                PoolType.NEARLINE, "archon", null, PoolState.LOST, true );
        final Pool pool2 = mockDaoDriver.createPool(
                PoolType.NEARLINE, "archoff", null, PoolState.LOST, false );
        final Pool pool3 = mockDaoDriver.createPool(
                PoolType.ONLINE, "enton", null, PoolState.LOST, true );
        final Pool pool4 = mockDaoDriver.createPool(
                PoolType.ONLINE, "entoff", null, PoolState.LOST, false );
        final Pool pool5 = mockDaoDriver.createPool(
                PoolType.ONLINE, "entlost1", null, PoolState.LOST, true );
        final Pool pool6 = mockDaoDriver.createPool(
                PoolType.ONLINE, "entlost2", null, PoolState.LOST, false );
        mockDaoDriver.putBlobOnPool( pool1.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool3.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool4.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool5.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool6.getId(), blob.getId() );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        final PoolBlobStore poolBlobStore = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                cacheManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );

        assertNull(poolBlobStore.getPoolFileFor( blob.getId() ), "Should notta been able to access file when pool not available");
    }
    
    
    @Test
    public void testStartBlobReadWhenBlobNotInCacheButIsOnPoolButRecordWhackedBeforeLockAcquiredNotAllowed()
            throws InterruptedException
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Pool pool1 = mockDaoDriver.createPool(
                PoolType.NEARLINE, "archon", null, PoolState.NORMAL, true );
        final Pool pool2 = mockDaoDriver.createPool(
                PoolType.NEARLINE, "archoff", null, PoolState.NORMAL, false );
        final Pool pool3 = mockDaoDriver.createPool(
                PoolType.ONLINE, "enton", null, PoolState.NORMAL, true );
        final Pool pool4 = mockDaoDriver.createPool(
                PoolType.ONLINE, "entoff", null, PoolState.NORMAL, false );
        final Pool pool5 = mockDaoDriver.createPool(
                PoolType.ONLINE, "entlost1", null, PoolState.LOST, true );
        final Pool pool6 = mockDaoDriver.createPool(
                PoolType.ONLINE, "entlost2", null, PoolState.LOST, false );
        mockDaoDriver.putBlobOnPool( pool1.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool3.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool4.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool5.getId(), blob.getId() );
        mockDaoDriver.putBlobOnPool( pool6.getId(), blob.getId() );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final CountDownLatch whackItLatch = new CountDownLatch( 1 );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        final PoolBlobStore poolBlobStore = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                cacheManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        SystemWorkPool.getInstance().submit( new Runnable()
        {
            public void run()
            {
                synchronized ( poolBlobStore.getLockSupport() )
                {
                    whackItLatch.countDown();
                    TestUtil.sleep( 100 );
                    dbSupport.getDataManager().deleteBeans( BlobPool.class, Require.nothing() );
                }
            }
        } );
        whackItLatch.await();

        assertNull(poolBlobStore.getPoolFileFor( blob.getId() ), "Should notta been able to access file when blob pool was whacked while waiting on lock.");
    }
    
    
    @Test
    public void testDestroyPoolDoesSoWhenNotAssignedToStorageDomain()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodDestroy =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "destroyPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.FOREIGN );
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.destroyPool( pool.getId() );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodDestroy, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCalls );

        assertNull(mockDaoDriver.retrieve( pool ), "Shoulda whacked pool.");
    }
    

    @Test
    public void testDestroyPoolDoesSoWhenAssignedToStorageDomainButNoDataResidesOnPool()
    {
        

        final Method methodPowerOn =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method methodPowerOff =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        final Method methodDestroy =
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "destroyPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), pool.getPartitionId() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        mockDaoDriver.putBlobOnPool( mockDaoDriver.createPool().getId(), blob.getId() );
        mockDaoDriver.updateBean( 
                pool.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.destroyPool( pool.getId() );
        assertEquals(0,  store.getTasks().size(), "Should notta created any tasks for this synchronous operation.");

        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( methodPowerOn, null );
        expectedCalls.put( methodPowerOff, null );
        expectedCalls.put( methodDestroy, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCalls );

        assertNull(mockDaoDriver.retrieve( pool ), "Shoulda whacked pool.");
    }
    

    @Test
    public void testCompactPoolDoesSo()
    {


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.compactPool( BlobStoreTaskPriority.LOW, pool.getId() );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single compact pool task for single pool being compacted.");
        assertEquals(BlobStoreTaskPriority.LOW, store.getTasks().iterator().next().getPriority(), "Shoulda set compact pool task priority correctly.");

        store.compactPool( BlobStoreTaskPriority.NORMAL, pool.getId() );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single compact pool task for single pool being compacted.");
        assertEquals(BlobStoreTaskPriority.NORMAL, store.getTasks().iterator().next().getPriority(), "Shoulda set compact pool task priority correctly.");

        store.compactPool( BlobStoreTaskPriority.LOW, pool.getId() );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single compact pool task for single pool being compacted.");
        assertEquals(BlobStoreTaskPriority.NORMAL, store.getTasks().iterator().next().getPriority(), "Shoulda set compact pool task priority correctly.");
    }
    

    @Test
    public void testVerifyPoolDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.verify( BlobStoreTaskPriority.LOW, pool.getId() );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single verify pool task for single pool being verifyed.");
        assertEquals(BlobStoreTaskPriority.LOW, store.getTasks().iterator().next().getPriority(), "Shoulda set verify pool task priority correctly.");

        store.verify( BlobStoreTaskPriority.NORMAL, pool.getId() );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single verify pool task for single pool being verifyed.");
        assertEquals(BlobStoreTaskPriority.NORMAL, store.getTasks().iterator().next().getPriority(), "Shoulda set verify pool task priority correctly.");

        store.verify( BlobStoreTaskPriority.LOW, pool.getId() );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single verify pool task for single pool being verifyed.");
        assertEquals(BlobStoreTaskPriority.NORMAL, store.getTasks().iterator().next().getPriority(), "Shoulda set verify pool task priority correctly.");
    }


    @Test
    public void testVerifyAllPoolsDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        store.verify( BlobStoreTaskPriority.LOW, null );
        assertEquals(2,  store.getTasks().size(), "Shoulda had single verify pool task for single pool being verifyed.");
        for ( BlobStoreTask task : store.getTasks() )
        {
            assertEquals(BlobStoreTaskPriority.LOW, task.getPriority(), "Shoulda set verify pool task priority correctly.");
        }
    }
    

    @Test
    public void testCancelVerifyPoolDoesSoWhenPossible()
    {


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final PoolBlobStore store = new PoolBlobStoreImpl(
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        store.verify( BlobStoreTaskPriority.LOW, pool.getId() );
        assertEquals(1,  store.getTasks().size(), "Shoulda had single verify pool task for single pool being verifyed.");
        assertEquals(BlobStoreTaskPriority.LOW, store.getTasks().iterator().next().getPriority(), "Shoulda set verify pool task priority correctly.");

        assertTrue(store.cancelVerifyPool( pool.getId() ), "Shoulda reported did cancel.");
        assertEquals(0,  store.getTasks().size(), "Should notta had verify pool task due to cancellation.");

        assertFalse(store.cancelVerifyPool( pool.getId() ), "Shoulda reported did not have task to cancel.");

        store.verify( BlobStoreTaskPriority.LOW, pool.getId() );
        ( (PoolTask)store.getTasks().iterator().next() ).prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
                {
                    store.cancelVerifyPool( pool.getId() );
                }
            } );
    }
    
    
    @Test
    public void testImportPoolWithoutApplicableABMConfigFails()
    {
        
        dbSupport.getServiceManager().getService( PoolService.class );
        final PoolBlobStore store = new PoolBlobStoreImpl( 
                getRpcClient(),
                new MockDiskManager( dbSupport.getServiceManager() ), 
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( null, "tp1" );
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "tp2" );
        final PoolPartition partition3 = mockDaoDriver.createPoolPartition( null, "tp3" );
        final Pool pool = mockDaoDriver.createPool( partition1.getId(), PoolState.FOREIGN );
        final Pool pool2 = mockDaoDriver.createPool( partition2.getId(), PoolState.FOREIGN );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition3.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(),
                partition1.getId() );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );

        final ImportPoolDirective directive = mockDaoDriver.createImportPoolDirective(
                pool.getId(),
                user.getId(), 
                dp.getId() );
        TestUtil.assertThrows(
                "Shoulda thrown exception since we don't have the needed storage domain member.",
                GenericFailure.CONFLICT, 
                new BlastContainer()
                {
                    public void test()
                    {
                        store.importPool( 
                                BlobStoreTaskPriority.values()[ 1 ],
                                directive );
                    }
                } );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(),
                partition1.getId() );
        store.importPool( 
                BlobStoreTaskPriority.values()[ 1 ],
                directive );
                
        final ImportPoolDirective directive2 = mockDaoDriver.createImportPoolDirective(
                pool2.getId(),
                user.getId(), 
                dp.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(),
                partition2.getId() );
        TestUtil.assertThrows(
                "Shoulda thrown exception since we don't have the needed persistence rule",
                GenericFailure.CONFLICT, 
                new BlastContainer()
                {
                    public void test()
                    {
                        store.importPool( 
                                BlobStoreTaskPriority.values()[ 1 ],
                                directive2 );
                    }
                } );
        mockDaoDriver.createDataPersistenceRule(dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        store.importPool( 
                BlobStoreTaskPriority.values()[ 1 ],
                directive2 );
    }


    @Test
    public void testMissingOrIncorrectSizeFileDoesntReturnPath() throws IOException {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Path temp = Files.createTempDirectory("pool");

        final Pool pool = mockDaoDriver.createPool(
                PoolType.ONLINE, temp.resolve("enton").toString(), null, PoolState.NORMAL, true );
        final BlobPool blobPool = mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return new RpcResponse<>();
            }
        } );
        final PoolBlobStore poolBlobStore = new PoolBlobStoreImpl(
                getRpcClient( InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih ) ),
                cacheManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );

        final Path expectedPath = PoolUtils.getPath( pool, bucket.getName(), object.getId(), blob.getId() );
        Files.createDirectories(expectedPath.getParent());

        assertNull(poolBlobStore.getPoolFileFor( blob.getId() ), "Should notta returned file that doesn't exist");


        RandomAccessFile f = new RandomAccessFile(expectedPath.toString(), "rw");
        f.setLength(blob.getLength() + 1);

        assertNull(poolBlobStore.getPoolFileFor( blob.getId() ), "Should notta returned file that is wrong size");

        f.setLength(blob.getLength());

        final Object actual = poolBlobStore.getPoolFileFor( blob.getId() ).getFilePath();
        assertEquals(expectedPath.toString(), actual, "Shoulda returned file.");
        final Object expected = blobPool.getId();
        assertEquals(expected, poolBlobStore.getPoolFileFor( blob.getId() ).getBlobPoolId(), "Shoulda chosen correct blob pool id");
        assertEquals(1,  btih.getTotalCallCount(), "Shoulda made call on pool lock support to acquire lock on blob.");
    }

    
    private RpcClient getRpcClient()
    {
        final PoolEnvironmentResource resource = mock( PoolEnvironmentResource.class);
        when(resource.powerOn(any())).thenReturn(new RpcResponse<>(null));
        return getRpcClient( resource );
    }
    
    
    private RpcClient getRpcClient( final PoolEnvironmentResource per )
    {
        return InterfaceProxyFactory.getProxy( RpcClient.class, new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
            {
                return per;
            }
        } );
    }
    
    
    private PoolLockSupport< PoolTask > createLockSupport()
    {
        return new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }
    
    
    private PoolEnvironmentResource getPoolEnvironmentResource()
    {
        return InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, null );
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
