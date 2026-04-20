/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class PoolManagementResourceImpl_Test 
{
     @Test
    public void testConstructorNullRpcServerNotAllowed()
    {
        
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
                {
                    new PoolManagementResourceImpl( null, poolBlobStore, dbSupport.getServiceManager() );
                }
            } );
    }
    

     @Test
    public void testConstructorNullPoolBlobStoreNotAllowed()
    {
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PoolManagementResourceImpl( rpcServer, null, dbSupport.getServiceManager() );
            }
            } );
    }
    

     @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, null );
            }
            } );
    }
    

     @Test
    public void testConstructorHappyConstruction()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
    }
    

     @Test
    public void testCompactWithNullPriorityDelegatesToPoolBlobStoreWithNonNullPriority()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.compactPool( null, null );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(BlobStoreTaskPriority.BACKGROUND, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
        assertEquals(null, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ), "Shoulda delegated to pool blob store.");
    }
    

     @Test
    public void testCompactWithNonNullPriorityDelegatesToPoolBlobStoreWithSpecifiedPriority()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.compactPool( null, BlobStoreTaskPriority.HIGH );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(BlobStoreTaskPriority.HIGH, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
        assertEquals(null, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ), "Shoulda delegated to pool blob store.");
    }
    

     @Test
    public void testFormatDelegatesToPoolBlobStore()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final UUID poolId = UUID.randomUUID();
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.formatPool( poolId );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(poolId, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
    }
    

     @Test
    public void testDestroyDelegatesToPoolBlobStore()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final UUID poolId = UUID.randomUUID();
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.destroyPool( poolId );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(poolId, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
    }
    

     @Test
    public void testImportAllPoolsDelegatesToPoolBlobStoreWithSpecifiedPriority()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final UUID userId = mockDaoDriver.createUser( "user" ).getId();
        final UUID dataPolicyId = mockDaoDriver.createDataPolicy( "dp" ).getId();
        final UUID storageDomainId = mockDaoDriver.createStorageDomain( "sd1" ).getId();
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.importPool(
                null,
                newImportDirective( userId, dataPolicyId, storageDomainId ) );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(BlobStoreTaskPriority.HIGH, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
        assertEquals(null, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getPoolId(), "Shoulda delegated to pool blob store.");
        assertEquals(userId, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getUserId(), "Shoulda delegated to pool blob store.");
        assertEquals(dataPolicyId, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getDataPolicyId(), "Shoulda delegated to pool blob store.");
        assertEquals(storageDomainId, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getStorageDomainId(), "Shoulda delegated to pool blob store.");
    }
    

     @Test
    public void testImportSinglePoolDelegatesToPoolBlobStoreWithSpecifiedPriority()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );

        final UUID poolId = mockDaoDriver.createPool().getId();
        final UUID userId = mockDaoDriver.createUser( "user" ).getId();
        final UUID dataPolicyId = mockDaoDriver.createDataPolicy( "dp" ).getId();
        final UUID storageDomainId = mockDaoDriver.createStorageDomain( "sd1" ).getId();
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.importPool(
                poolId, 
                newImportDirective( userId, dataPolicyId, storageDomainId ) );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(BlobStoreTaskPriority.HIGH, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
        assertEquals(poolId, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getPoolId(), "Shoulda delegated to pool blob store.");
        assertEquals(userId, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getUserId(), "Shoulda delegated to pool blob store.");
        assertEquals(dataPolicyId, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getDataPolicyId(), "Shoulda delegated to pool blob store.");
        assertEquals(storageDomainId, ( (ImportPoolDirective)blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getStorageDomainId(), "Shoulda delegated to pool blob store.");
    }
    
    
     @Test
    public void testCancelImportPoolNonNullPoolIdCancelsImportForSpecifiedPool()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        mockDaoDriver.createPool( PoolState.FOREIGN );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, btih );
        final PoolManagementResource resource = new PoolManagementResourceImpl( 
                rpcServer,
                poolBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelImportPool( pool1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( PoolBlobStore.class, "cancelImportPool" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled import for single pool.");
        final Object expected = pool1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled import for single pool.");
    }
    
    
     @Test
    public void testCancelImportPoolNullPoolIdCancelsImportForEveryPool()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool( PoolState.IMPORT_PENDING );
        final Pool pool2 = mockDaoDriver.createPool( PoolState.IMPORT_PENDING );
        mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        final Set< UUID > poolIds = CollectionFactory.toSet( pool1.getId(), pool2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, btih );
        final PoolManagementResource resource = new PoolManagementResourceImpl( 
                rpcServer,
                poolBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelImportPool( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( PoolBlobStore.class, "cancelImportPool" ) );
        assertEquals(2,  invocations.size(), "Shoulda canceled format for all pools.");
        poolIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        poolIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  poolIds.size(), "Shoulda canceled format for all pools.");
    }
    

     @Test
    public void testVerifyWithNullPriorityDelegatesToPoolBlobStoreWithNonNullPriority()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.verifyPool( null, null );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(BlobStoreTaskPriority.BACKGROUND, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
        assertEquals(null, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ), "Shoulda delegated to pool blob store.");
    }
    

     @Test
    public void testVerifyWithNonNullPriorityDelegatesToPoolBlobStoreWithSpecifiedPriority()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.verifyPool( null, BlobStoreTaskPriority.HIGH );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
        assertEquals(BlobStoreTaskPriority.HIGH, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda delegated to pool blob store.");
        assertEquals(null, blobStoreBtih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ), "Shoulda delegated to pool blob store.");
    }
    
    
     @Test
    public void testCancelVerifyPoolNonNullPoolIdCancelsVerifyForSpecifiedPool()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        mockDaoDriver.createPool( PoolState.FOREIGN );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, btih );
        final PoolManagementResource resource = new PoolManagementResourceImpl( 
                rpcServer,
                poolBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelVerifyPool( pool1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( PoolBlobStore.class, "cancelVerifyPool" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled verify for single pool.");
        final Object expected = pool1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled verify for single pool.");
    }
    
    
     @Test
    public void testCancelVerifyPoolNullPoolIdCancelsVerifyForEveryPool()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool2 = mockDaoDriver.createPool( PoolState.IMPORT_PENDING );
        final Set< UUID > poolIds = CollectionFactory.toSet( pool1.getId(), pool2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, btih );
        final PoolManagementResource resource = new PoolManagementResourceImpl( 
                rpcServer,
                poolBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelVerifyPool( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( PoolBlobStore.class, "cancelVerifyPool" ) );
        assertEquals(2,  invocations.size(), "Shoulda canceled verify for all pools.");
        poolIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        poolIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  poolIds.size(), "Shoulda canceled verify for all pools.");
    }
    
    
     @Test
    public void testCancelVerifyPoolNullPoolIdCancelsVerifyForEveryPoolEvenWhenFailureOccursDuringVerify()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool2 = mockDaoDriver.createPool( PoolState.IMPORT_PENDING );
        final Set< UUID > poolIds = CollectionFactory.toSet( pool1.getId(), pool2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            public Object invoke( final Object arg0, final Method arg1, final Object[] arg2 ) throws Throwable
            {
                throw new RuntimeException( "Oops." );
            }
        } );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, btih );
        final PoolManagementResource resource = new PoolManagementResourceImpl( 
                rpcServer,
                poolBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelVerifyPool( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( PoolBlobStore.class, "cancelVerifyPool" ) );
        assertEquals(2,  invocations.size(), "Shoulda canceled verify for all pools.");
        poolIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        poolIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  poolIds.size(), "Shoulda canceled verify for all pools.");
    }
    

     @Test
    public void testRefreshDelegatesToPoolBlobStore()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler blobStoreBtih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, blobStoreBtih );
        
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        resource.forcePoolEnvironmentRefresh();
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda delegated to pool blob store.");
    }
    
    
     @Test
    public void testDeletePermanentlyOfflinePoolNotInLostStateNotAllowed()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                MockInvocationHandler.forReturnType(
                        Object.class, new ConstantResponseInvocationHandler( new Object() ), null ) );
        
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o11 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b11 = mockDaoDriver.getBlobFor( o11.getId() );
        final S3Object o12 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b12 = mockDaoDriver.getBlobFor( o12.getId() );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final Blob b21 = mockDaoDriver.getBlobFor( o21.getId() );
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b22 = mockDaoDriver.getBlobFor( o22.getId() );
        final Pool pool1 = mockDaoDriver.createPool( PoolState.FOREIGN );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b11.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b21.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b22.getId() );
        final Pool pool2 = mockDaoDriver.createPool( PoolState.FOREIGN );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b12.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b21.getId() );
        
        final BeansRetriever< BlobPool > retriever = 
                dbSupport.getServiceManager().getRetriever( BlobPool.class );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    resource.deletePermanentlyLostPool( pool1.getId() );
                }
            } );
        assertEquals(5,  retriever.getCount(), "Should notta whacked blobs persisted on pool1.");
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    resource.deletePermanentlyLostPool( pool2.getId() );
                }
            } );
        assertEquals(5,  retriever.getCount(), "Should notta whacked blobs persisted on pool2.");
    }
    
    
     @Test
    public void testDeletePermanentlyOfflinePoolDoesSo()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                MockInvocationHandler.forReturnType(
                        Object.class, new ConstantResponseInvocationHandler( new Object() ), null ) );
        
        final PoolManagementResource resource =
                new PoolManagementResourceImpl( rpcServer, poolBlobStore, dbSupport.getServiceManager() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o11 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b11 = mockDaoDriver.getBlobFor( o11.getId() );
        final S3Object o12 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final Blob b12 = mockDaoDriver.getBlobFor( o12.getId() );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final Blob b21 = mockDaoDriver.getBlobFor( o21.getId() );
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b22 = mockDaoDriver.getBlobFor( o22.getId() );
        final Pool pool1 = mockDaoDriver.createPool( PoolState.LOST );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b11.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b21.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b22.getId() );
        final Pool pool2 = mockDaoDriver.createPool( PoolState.LOST );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b12.getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), b21.getId() );
        
        final BeansRetriever< BlobPool > retriever = 
                dbSupport.getServiceManager().getRetriever( BlobPool.class );
        resource.deletePermanentlyLostPool( pool1.getId() );
        assertEquals(2,  retriever.getCount(), "Shoulda whacked blobs persisted on pool1.");
        resource.deletePermanentlyLostPool( pool2.getId() );
        assertEquals(0,  retriever.getCount(), "Shoulda whacked blobs persisted on pool2.");
    }
    
    
    private ImportPersistenceTargetDirectiveRequest newImportDirective(
            final UUID userId,
            final UUID dataPolicyId,
            final UUID storageDomainId )
    {
        return BeanFactory.newBean( ImportPersistenceTargetDirectiveRequest.class )
                .setUserId( userId ).setDataPolicyId( dataPolicyId ).setStorageDomainId( storageDomainId )
                .setPriority( BlobStoreTaskPriority.HIGH );
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }
}
