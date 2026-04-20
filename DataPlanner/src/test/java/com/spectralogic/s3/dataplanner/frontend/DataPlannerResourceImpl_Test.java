/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.frontend;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.orm.JobRM;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.DataMigrationService;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobChunkToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
import com.spectralogic.s3.common.rpc.dataplanner.CancelJobFailedException;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.*;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.task.MockTapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.task.MockTapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.tape.task.NoOpTapeTask;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.task.MockDs3ConnectionFactory;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.JobCreatedListener;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class DataPlannerResourceImpl_Test
{
    @Test
    public void testConstructorNullDeadJobMonitorNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        null,
                        rpcServer,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        progressManager,
                        tapeBlobStore,
                        poolBlobStore,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    
    
    @Test
    public void testConstructorNullRpcServerNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        null,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        progressManager,
                        tapeBlobStore,
                        poolBlobStore,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        rpcServer,
                        null,
                        cacheManager,
                        new MockJobCreator(),
                        progressManager,
                        tapeBlobStore,
                        poolBlobStore,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullCacheManagerNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        rpcServer,
                        dbSupport.getServiceManager(),
                        null,
                        new MockJobCreator(),
                        progressManager,
                        tapeBlobStore,
                        poolBlobStore,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullJobCreatorNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        rpcServer,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        null,
                        progressManager,
                        tapeBlobStore,
                        poolBlobStore,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullJobProgressManagerNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        rpcServer,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        null,
                        tapeBlobStore,
                        poolBlobStore,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullTapeBlobStoreNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        rpcServer,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        progressManager,
                        null,
                        poolBlobStore,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullPoolBlobStoreNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        rpcServer,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        progressManager,
                        tapeBlobStore,
                        null,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullDs3ConnectionFactoryNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPlannerResourceImpl( 
                        deadJobMonitor,
                        rpcServer,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        progressManager,
                        tapeBlobStore,
                        poolBlobStore,
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        new DataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                progressManager,
                tapeBlobStore,
                poolBlobStore,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
    }
    
    
    @Test
    public void testAddTargetBlobStoreNullStoreNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.updateAllBeans(
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                        .setMaxNumberOfConcurrentJobs(100),
                DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS );
        
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResourceImpl resource = new DataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.addTargetBlobStore( null );
            }
        } );
    }
    
    
    @Test
    public void testForceTargetEnvironmentRefreshDelegatesToAddedTargetBlobStores()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.updateAllBeans(
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                        .setMaxNumberOfConcurrentJobs(100),
                DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResourceImpl resource = new DataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
        
        final BasicTestsInvocationHandler btih1 = new BasicTestsInvocationHandler( null );
        final BasicTestsInvocationHandler btih2 = new BasicTestsInvocationHandler( null );
        final BasicTestsInvocationHandler btih3 = new BasicTestsInvocationHandler( null );
        resource.addTargetBlobStore( InterfaceProxyFactory.getProxy( TargetBlobStore.class, btih1 ) );
        resource.addTargetBlobStore( InterfaceProxyFactory.getProxy( TargetBlobStore.class, btih2 ) );
        resource.forceTargetEnvironmentRefresh();
        resource.addTargetBlobStore( InterfaceProxyFactory.getProxy( TargetBlobStore.class, btih3 ) );
        
        final Method method = ReflectUtil.getMethod( BlobStore.class, "refreshEnvironmentNow" );
        assertEquals(1, btih1.getMethodCallCount(method), "Shoulda delegated refresh to target blob stores.");
        assertEquals(1, btih2.getMethodCallCount(method), "Shoulda delegated refresh to target blob stores.");
        assertEquals(0, btih3.getMethodCallCount(method), "Shoulda delegated refresh to target blob stores.");

        resource.forceTargetEnvironmentRefresh();
        assertEquals(2, btih1.getMethodCallCount(method), "Shoulda delegated refresh to target blob stores.");
        assertEquals(2, btih2.getMethodCallCount(method), "Shoulda delegated refresh to target blob stores.");
        assertEquals(1, btih3.getMethodCallCount(method), "Shoulda delegated refresh to target blob stores.");
    }
    

    @Test
    public void testCreateTooManyConcurrentJobsNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final User user = BeanFactory.newBean( User.class )
                .setName( "jason" ).setAuthId( "a" ).setSecretKey( "b" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );

        mockDaoDriver.updateAllBeans(
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                        .setMaxNumberOfConcurrentJobs(100),
                DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS );
        
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = new DataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
        
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        // Create 99 internal jobs
        for ( int i = 0; i < 99; ++i )
        {
            mockDaoDriver.createJobInternal( transaction, bucket.getId(), user.getId(), JobRequestType.PUT );
        }
        transaction.commitTransaction();

        // Create the last job within the limit
        final S3ObjectToCreate o1 = BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "objectnotoomany" ).setSizeInBytes( 10 );
        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( user.getId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { o1 } ) );
        TestUtil.assertThrows( null, DataPlannerException.class, new BlastContainer()
        {
            public void test()
            {
                // Verify limit + 1 throws error
                final S3ObjectToCreate o2 = BeanFactory.newBean( S3ObjectToCreate.class )
                        .setName( "onetoomany" ).setSizeInBytes( 10 );
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( user.getId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( new S3ObjectToCreate [] { o2 } ) );
            }
        } );
    }
    

    @Test
    public void testCreateGetJobNullObjectIdsNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final User user = BeanFactory.newBean( User.class )
                .setName( "jason" ).setAuthId( "a" ).setSecretKey( "b" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );
        
        mockDaoDriver.createObject( bucket.getId(), "a", 11 );
        mockDaoDriver.createObject( bucket.getId(), "b", 11 );

        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( user.getId() )
                        .setBlobIds( null ) );
            }
        } );
    }
    

    @Test
    public void testCreateGetJobEmptyObjectIdsNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final User user = BeanFactory.newBean( User.class )
                .setName( "jason" ).setAuthId( "a" ).setSecretKey( "b" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );

        mockDaoDriver.createObject( bucket.getId(), "a", 11 );
        mockDaoDriver.createObject( bucket.getId(), "b", 11 );

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                progressManager,
                tapeBlobStore, 
                poolBlobStore );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( user.getId() )
                        .setBlobIds( (UUID[])Array.newInstance( UUID.class, 0 ) ) );
            }
        } );
    }
    
    
    @Test
    public void testCreateGetJobWithOneDataObjectThatIsntToBeFoundAnywhereNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob = mockDaoDriver.getBlobFor( data.getId() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID [] { blob.getId() } ) );
            }
        } );
    }
    
    
    @Test
    public void testCreateGetJobWithOneDataObjectSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final UUID customJobId = UUID.randomUUID();
        final S3Object data = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, data.getId() );
        mockDaoDriver.putBlobOnTape( null, blob.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setName( "proliant" )
                        .setReplicatedJobId( customJobId )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entry.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(customJobId, mockDaoDriver.attainOneAndOnly( Job.class ).getId(), "Shoulda respected custom job id.");
        assertEquals("proliant", mockDaoDriver.attainOneAndOnly( Job.class ).getName(), "Shoulda respected job name.");
    }
    
    
    @Test
    public void testCreateGetJobWithManyDataObjectsSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final JobEntry chunkToDelete =
                mockDaoDriver.createJobWithEntry( JobRequestType.GET, blob1 );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        dbSupport.getDataManager().deleteBean( JobEntry.class, chunkToDelete.getId() );
        dbSupport.getDataManager().deleteBean( Job.class, chunkToDelete.getJobId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.COMPLETED)), "Shoulda marked blob already in cache as completed");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(20, mockDaoDriver.attainOneAndOnly(Job.class).getOriginalSizeInBytes(), "Shoulda initialized job progress stats correctly.");
        assertEquals(10, mockDaoDriver.attainOneAndOnly(Job.class).getCachedSizeInBytes(), "Shoulda initialized job progress stats correctly.");
        assertEquals(0, mockDaoDriver.attainOneAndOnly(Job.class).getCompletedSizeInBytes(), "Shoulda initialized job progress stats correctly.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWithManyDataObjectsSetsUpJobWhenNoObjectsInCache()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWithManyDataObjectsSetsUpJobWhenSomeObjectsInCache()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.COMPLETED)), "Shoulda marked blob already in cache as completed");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWithManyDataObjectsSetsUpJobWhenAllObjectsInCache()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1, blob2 ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 10 );
        cacheManager.blobLoadedToCache( blob2.getId() );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.COMPLETED)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWithFoldersAndDataObjectsSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final S3Object folder3 = mockDaoDriver.createObject( bucket.getId(), "some/folder3/", -1 );
        final Blob blob3 = mockDaoDriver.createBlobs( folder3.getId(), 1, 0 ).get( 0 );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId(), blob3.getId() } ) )
                        .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(3, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.COMPLETED)), "Shoulda chunked up the job with the correct initial blob store state.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWhenPreallocationImpossibleNoOrderingGuaranteeAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject(
                bucket.getId(), "some/object", 300 * 1024 * 1024 );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject(
                bucket.getId(), "some/object2", 300 * 1024 * 1024 );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertFalse(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Should notta pre-allocated entire job.");
        assertFalse(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Should notta pre-allocated entire job.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWhenPreallocationPossibleOrderingGuaranteeAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject(
                bucket.getId(), "some/object", 200 * 1024 * 1024 );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject(
                bucket.getId(), "some/object2", 200 * 1024 * 1024 );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final S3Object data3 = mockDaoDriver.createObject(
                bucket.getId(), "some/object3", 300 * 1024 * 1024 );
        final Blob blob3 = mockDaoDriver.getBlobFor( data3.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob3.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setChunkOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda pre-allocated entire job.");
        assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda pre-allocated entire job.");

        mockDaoDriver.delete( Blob.class, blob1 );
        mockDaoDriver.delete( Blob.class, blob2 );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setChunkOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER )
                        .setBlobIds( new UUID[] { blob3.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertTrue(cacheManager.isCacheSpaceAllocated( blob3.getId() ), "Shoulda pre-allocated entire job.");

        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWhenPreallocationImpossibleAndOrderingGuaranteeNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject(
                bucket.getId(), "some/object", 300 * 1024 * 1024 );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject(
                bucket.getId(), "some/object2", 300 * 1024 * 1024 );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, new BlastContainer()
        {
            public void test()
            {
                resource.createGetJob( 
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setChunkOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER )
                        .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking();
            }
        } );
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta created job.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateAggregatingGetJobWhenPreallocationImpossibleAndOrderingGuaranteeNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        final S3Object data0 = mockDaoDriver.createObject(
                bucket.getId(), "some/object0", 3 * 1024 * 1024 );
        final Blob blob0 = mockDaoDriver.getBlobFor( data0.getId() );
        final S3Object data1 = mockDaoDriver.createObject(
                bucket.getId(), "some/object", 300 * 1024 * 1024 );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject(
                bucket.getId(), "some/object2", 300 * 1024 * 1024 );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        mockDaoDriver.putBlobOnTape( null, blob0.getId() );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );

        resource.createGetJob( 
                BeanFactory.newBean( CreateGetJobParams.class )
                .setUserId( bucket.getUserId() )
                .setAggregating( true )
                .setChunkOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER )
                .setBlobIds( new UUID [] { blob0.getId() } ) ).getWithoutBlocking();
        
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, new BlastContainer()
        {
            public void test()
            {
                resource.createGetJob( 
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setChunkOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER )
                        .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) );
            }
        } );
        
        resource.cleanUpCompletedJobsAndJobChunks();
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta appended to initial job.");
        assertEquals(3 * 1024 * 1024, mockDaoDriver.attainOneAndOnly(Job.class).getOriginalSizeInBytes(), "Should notta appended to initial job.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWithDataOnTargetToServiceFromFailsIfCannotGetThemFromTarget()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.updateBean( 
                target.setDefaultReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL ), 
                ReplicationTarget.DEFAULT_READ_PREFERENCE );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );

        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.RETIRED, target.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        
        final DataPlannerResource resource = new DataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore,
                new MockDs3ConnectionFactory() );
        
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.createGetJob( 
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) );
            }
        } );
        assertEquals(0, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Should notta created the job.");
        cacheFilesystemDriver.shutdown();
    }
    

    @Test
    public void testCreateAggregatingGetJobWithBlobInJobAndInExistingAggregatingGetJobDoesNotAggregate()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );

        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final S3Object data3 = mockDaoDriver.createObject( bucket.getId(), "some/object3" );
        final Blob blob3 = mockDaoDriver.getBlobFor( data3.getId() );
        final S3Object data4 = mockDaoDriver.createObject( bucket.getId(), "some/object4" );
        final Blob blob4 = mockDaoDriver.getBlobFor( data4.getId() );
        
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob3.getId() );
        mockDaoDriver.putBlobOnTape( null, blob4.getId() );
        
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        int numJobEntries = 0;
        int numJobs = 0;
        
        numJobEntries += 2;
        numJobs += 1;
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Should notta returned null.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should have cache entry only on the one chunk fully in cache.");
        assertEquals(numJobs, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Shoulda created the first aggregated job since it is the only job.");

        numJobEntries += 2;
        numJobs += 1;
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setBlobIds( new UUID[] { blob2.getId(), blob3.getId() } ) ).getWithoutBlocking(), "Should notta returned null.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should have cache entry only on the one chunk fully in cache.");
        assertEquals(numJobs, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Should notta aggregated job due to overlapping blob for blob2.");

        numJobEntries += 1;
        numJobs += 0;
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setBlobIds( new UUID[] { blob4.getId() } ) ).getWithoutBlocking(), "Should notta returned null.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should have cache entry only on the one chunk fully in cache.");
        assertEquals(numJobs, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Shoulda aggregated job due to no overlapping blob for blob4.");

        numJobEntries += 1;
        numJobs += 0;
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setBlobIds( new UUID[] { blob3.getId() } ) ).getWithoutBlocking(), "Should notta returned null.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should have cache entry only on the one chunk fully in cache.");
        assertEquals(numJobs, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Shoulda aggregated because previous job containing blobs 1 & 2 did not contain blobs 3.");

        numJobEntries += 2;
        numJobs += 1;
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setBlobIds( new UUID[] { blob3.getId(), blob4.getId() } ) ).getWithoutBlocking(), "Should notta returned null.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(numJobEntries, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should have cache entry only on the one chunk fully in cache.");
        assertEquals(numJobs, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Should notta aggregated because previous jobs contain blobs 3 and/or 4.");

        cacheFilesystemDriver.shutdown();
    }

    
    @Test
    public void testCreateAggregatingGetJobWithDataOnTargetToServiceFromDefersCreatingGetJobRemotelyOnTarget()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.updateBean( 
                target.setDefaultReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL ), 
                ReplicationTarget.DEFAULT_READ_PREFERENCE );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );

        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final S3Object data3 = mockDaoDriver.createObject( bucket.getId(), "some/object3" );
        final Blob blob3 = mockDaoDriver.getBlobFor( data3.getId() );
        final S3Object data4 = mockDaoDriver.createObject( bucket.getId(), "some/object4" );
        final Blob blob4 = mockDaoDriver.getBlobFor( data4.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.RETIRED, target.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), blob3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), blob4.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        
        final MockDs3ConnectionFactory ds3ConnectionFactory =
                new MockDs3ConnectionFactory();
        final BlobPersistenceContainer blobPersistenceContainer = 
                BeanFactory.newBean( BlobPersistenceContainer.class );
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setAvailableOnTapeNow( true );
        final BlobPersistence bp3 = BeanFactory.newBean( BlobPersistence.class );
        bp3.setId( blob3.getId() );
        bp3.setAvailableOnPoolNow( true );
        final BlobPersistence bp4 = BeanFactory.newBean( BlobPersistence.class );
        bp4.setId( blob4.getId() );
        bp4.setAvailableOnPoolNow( true );
        blobPersistenceContainer.setBlobs( new BlobPersistence [] { bp1, bp3, bp4 } );
        ds3ConnectionFactory.setGetBlobPersistenceResponse( blobPersistenceContainer );
        
        final DataPlannerResource resource = new DataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, ds3ConnectionFactory, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore,
                ds3ConnectionFactory );

        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Should notta returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_DS3_TARGET_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(2, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should have cache entry only on the one chunk fully in cache.");
        assertEquals(0, ds3ConnectionFactory.getBtih().getMethodCallCount(
                ReflectUtil.getMethod(Ds3Connection.class, "createGetJob")), "Should notta created remote GET job.");

        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setAggregating( true )
                        .setBlobIds( new UUID[] { blob3.getId(), blob4.getId() } ) ).getWithoutBlocking(), "Should notta returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(3, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_DS3_TARGET_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(4, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should notta populated any node ID's since nothing is in cache.");
        assertEquals(1, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Shoulda aggregated job.");
        assertEquals(0, ds3ConnectionFactory.getBtih().getMethodCallCount(
                ReflectUtil.getMethod(Ds3Connection.class, "createGetJob")), "Should notta created remote GET job.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWithDataOnTargetToServiceFromCreatesGetJobRemotelyOnTarget()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.updateBean( 
                target.setDefaultReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL ), 
                ReplicationTarget.DEFAULT_READ_PREFERENCE );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );

        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.RETIRED, target.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        
        final MockDs3ConnectionFactory ds3ConnectionFactory =
                new MockDs3ConnectionFactory();
        final BlobPersistenceContainer blobPersistenceContainer = 
                BeanFactory.newBean( BlobPersistenceContainer.class );
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setAvailableOnTapeNow( true );
        blobPersistenceContainer.setBlobs( new BlobPersistence [] { bp1 } );
        ds3ConnectionFactory.setGetBlobPersistenceResponse( blobPersistenceContainer );
        final JobChunkToReplicate remoteEntry = BeanFactory.newBean( JobChunkToReplicate.class );
        remoteEntry.setBlobId( blob1.getId() );
        remoteEntry.setId( UUID.randomUUID() );
        remoteEntry.setChunkNumber( 222 );
        remoteEntry.setId( remoteEntry.getId() );
        final DetailedJobToReplicate remoteJob = BeanFactory.newBean( DetailedJobToReplicate.class );
        remoteJob.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        remoteJob.getJob().setChunks( new JobChunkToReplicate [] { remoteEntry } );
        ds3ConnectionFactory.setCreateGetJobResponse( remoteJob.getJob() );

        final DataPlannerResource resource = new DataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, ds3ConnectionFactory, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore,
                ds3ConnectionFactory );

        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_DS3_TARGET_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(2, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should have cache entry only on the one chunk fully in cache.");
        ;
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testCreateGetJobWithDataOnTargetToServiceFromFailsWhenCustomJobId()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.updateBean(
                target.setDefaultReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL ),
                ReplicationTarget.DEFAULT_READ_PREFERENCE );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );

        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.RETIRED, target.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );

        final MockDs3ConnectionFactory ds3ConnectionFactory =
                new MockDs3ConnectionFactory();
        final BlobPersistenceContainer blobPersistenceContainer =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( blob1.getId() );
        bp1.setAvailableOnTapeNow( true );
        blobPersistenceContainer.setBlobs( new BlobPersistence [] { bp1 } );
        ds3ConnectionFactory.setGetBlobPersistenceResponse( blobPersistenceContainer );
        final JobChunkToReplicate remoteEntry = BeanFactory.newBean( JobChunkToReplicate.class );
        remoteEntry.setBlobId( blob1.getId() );
        remoteEntry.setId( UUID.randomUUID() );
        remoteEntry.setChunkNumber( 222 );
        remoteEntry.setId( remoteEntry.getId() );
        final DetailedJobToReplicate remoteJob = BeanFactory.newBean( DetailedJobToReplicate.class );
        remoteJob.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        remoteJob.getJob().setChunks( new JobChunkToReplicate [] { remoteEntry } );
        ds3ConnectionFactory.setCreateGetJobResponse( remoteJob.getJob() );
        
        final DataPlannerResource resource = new DataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, ds3ConnectionFactory, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore,
                ds3ConnectionFactory );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.createGetJob( 
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setReplicatedJobId( UUID.randomUUID() )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking();
            }
        } );
        assertEquals(0, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Should notta created the job.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateGetJobWithManyDataObjectsIncludingZeroLengthOnesSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object", 0 );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.COMPLETED)), "Shoulda marked zero length blob complete.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(10, mockDaoDriver.attainOneAndOnly(Job.class).getOriginalSizeInBytes(), "Shoulda initialized job progress stats correctly.");
        assertEquals(0, mockDaoDriver.attainOneAndOnly(Job.class).getCachedSizeInBytes(), "Shoulda initialized job progress stats correctly.");
        assertEquals(0, mockDaoDriver.attainOneAndOnly(Job.class).getCompletedSizeInBytes(), "Shoulda initialized job progress stats correctly.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateAggregatingPutJobWhenPreallocationImpossibleNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        final int mb = 1024 * 1024;
        final S3ObjectToCreate data1 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 200 * mb );
        final S3ObjectToCreate data2 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 200 * mb );
        final S3ObjectToCreate data3 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 200 * mb );
        
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, new BlastContainer()
        {
            public void test()
            {
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                        .setAggregating( true )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( new S3ObjectToCreate [] { data1, data2, data3 } ) );
            }
        } );

        final UUID jobId = resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                .setAggregating( true )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { data1, data2 } ) ).getWithoutBlocking();
        assertEquals(2, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda only had data1 and data2 as part of PUT job.");

        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, new BlastContainer()
        {
            public void test()
            {
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                        .setAggregating( true )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( new S3ObjectToCreate [] { data3 } ) );
            }
        } );
        assertEquals(2, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda only had data1 and data2 as part of PUT job.");

        resource.cancelJob( null, jobId, false );
        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                .setAggregating( true )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { data3 } ) ).getWithoutBlocking();
        
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreatePutJobWithPreallocationRequestedDoesAllocate()
    {
        dbSupport.getServiceManager()
                 .getService( BucketService.class )
                 .initializeLogicalSizeCache();
        
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b2" );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl(
                new CacheManagerImpl( dbSupport.getServiceManager(), new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor = InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource =
                newDataPlannerResourceImpl( deadJobMonitor, rpcServer, dbSupport.getServiceManager(), cacheManager,
                        newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                        new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                        tapeBlobStore, poolBlobStore );
        
        final S3ObjectToCreate o1 = BeanFactory.newBean( S3ObjectToCreate.class )
                                               .setName( "objectnotoomany" )
                                               .setSizeInBytes( 10 );
        
        final UUID jobId = resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                                                             .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                                                             .setUserId( bucket.getUserId() )
                                                             .setBucketId( bucket.getId() )
                                                             .setPreAllocateJobSpace( true )
                                                             .setObjectsToCreate( new S3ObjectToCreate[]{ o1 } ) )
                                   .getWithoutBlocking();
        
        final JobRM jobRM = new JobRM( jobId, dbSupport.getServiceManager() );
        cacheManager.isCacheSpaceAllocated( jobRM.getJobEntries()
                                                 .getFirst()
                                                 .getBlobId() );
        
        resource.cancelJob( null, jobId, false );
        
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateAggregatingPutJobHonorsMinimizeSpanningAcrossMediaParam()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        final int mb = 1024 * 1024;
        final S3ObjectToCreate data1 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data3 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data4 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 2 * mb );

        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                .setAggregating( true )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { data1 } ) );
        assertFalse(mockDaoDriver.attainOneAndOnly( Job.class ).isMinimizeSpanningAcrossMedia(), "Shoulda defaulted to not minimize spanning.");

        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                .setAggregating( true )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { data2 } ) );
        assertFalse(mockDaoDriver.attainOneAndOnly( Job.class ).isMinimizeSpanningAcrossMedia(), "Shoulda defaulted to not minimize spanning.");

        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                .setAggregating( true )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setMinimizeSpanningAcrossMedia( true )
                .setObjectsToCreate( new S3ObjectToCreate [] { data3 } ) );
        assertTrue(mockDaoDriver.attainOneAndOnly( Job.class ).isMinimizeSpanningAcrossMedia(), "Shoulda honored request to minimize spanning.");

        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setBlobbingPolicy( BlobbingPolicy.DISABLED )
                .setAggregating( true )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { data4 } ) );
        assertTrue(mockDaoDriver.attainOneAndOnly( Job.class ).isMinimizeSpanningAcrossMedia(), "Shoulda honored request to minimize spanning.");

        cacheFilesystemDriver.shutdown();
    }
    

    @Test
    public void testCreateVerifyJobNullObjectIdsNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final User user = BeanFactory.newBean( User.class )
                .setName( "jason" ).setAuthId( "a" ).setSecretKey( "b" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );
        
        mockDaoDriver.createObject( bucket.getId(), "a", 11 );
        mockDaoDriver.createObject( bucket.getId(), "b", 11 );

        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.createVerifyJob( 
                        BeanFactory.newBean( CreateVerifyJobParams.class )
                        .setUserId( user.getId() )
                        .setBlobIds( null ) );
            }
        } );
    }
    

    @Test
    public void testCreateVerifyJobEmptyObjectIdsNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final User user = BeanFactory.newBean( User.class )
                .setName( "jason" ).setAuthId( "a" ).setSecretKey( "b" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );

        mockDaoDriver.createObject( bucket.getId(), "a", 11 );
        mockDaoDriver.createObject( bucket.getId(), "b", 11 );

        final JobProgressManager progressManager =
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                progressManager,
                tapeBlobStore,
                poolBlobStore );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.createVerifyJob(
                        BeanFactory.newBean( CreateVerifyJobParams.class )
                        .setUserId( user.getId() )
                        .setBlobIds( (UUID[])Array.newInstance( UUID.class, 0 ) ) );
            }
        } );
    }
    
    
    @Test
    public void testCreateVerifyJobWithOneDataObjectThatIsntToBeFoundAnywhereNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob = mockDaoDriver.getBlobFor( data.getId() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.createVerifyJob(
                        BeanFactory.newBean( CreateVerifyJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID [] { blob.getId() } ) );
            }
        } );
    }
    
    
    @Test
    public void testCreateVerifyJobWithOneDataObjectSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, data.getId() );
        mockDaoDriver.putBlobOnTape( null, blob.getId() );
        assertNotNull(resource.createVerifyJob(
                        BeanFactory.newBean( CreateVerifyJobParams.class )
                        .setName( "proliant" )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entry.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals("proliant", mockDaoDriver.attainOneAndOnly( Job.class ).getName(), "Shoulda respected job name.");
    }

    @Tag("dataplanner-integration")
    @Test
    public void testCreateVerifyJobWithManyDataObjectsNotAllOnTapeNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.VERIFY, CollectionFactory.toSet( blob1 ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        TestUtil.assertThrows( 
                null,
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    @Override
                    public void test()
                    {
                        resource.createVerifyJob( 
                                BeanFactory.newBean( CreateVerifyJobParams.class )
                                .setUserId( bucket.getUserId() )
                                .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) );
                    }
                } );
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta created the job entries.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreateVerifyJobWithManyDataObjectsAllOnTapeSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        assertNotNull(resource.createVerifyJob(
                        BeanFactory.newBean( CreateVerifyJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
    }
    
    
    @Test
    public void testCreateVerifyJobWithManyDataAndFolderObjectsAllOnTapeSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final S3Object folder3 = mockDaoDriver.createObject( bucket.getId(), "some/folder3/", -1 );
        final Blob blob3 = mockDaoDriver.createBlobs( folder3.getId(), 1, 0 ).get( 0 );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob3.getId() );
        assertNotNull(resource.createVerifyJob(
                        BeanFactory.newBean( CreateVerifyJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId(), blob3.getId() } ) )
                        .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(3, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(3, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
    }
    

    @Test
    public void testCreatePutJobNullObjectsToCreateNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final User user = BeanFactory.newBean( User.class )
                .setName( "jason" ).setAuthId( "a" ).setSecretKey( "b" );
        dbSupport.getDataManager().createBean( user );
        
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );

        mockDaoDriver.createObject( bucket.getId(), "a", 11 );
        mockDaoDriver.createObject( bucket.getId(), "b", 11 );

        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( user.getId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( null ) );
            }
        } );
    }
    

    @Test
    public void testCreatePutJobEmptyObjectsToCreateNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        
        final User user = BeanFactory.newBean( User.class )
                .setName( "jason" ).setAuthId( "a" ).setSecretKey( "b" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );

        mockDaoDriver.createObject( bucket.getId(), "a", 11 );
        mockDaoDriver.createObject( bucket.getId(), "b", 11 );

        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( user.getId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( (S3ObjectToCreate[])Array.newInstance(
                                S3ObjectToCreate.class, 0 ) ) );
            }
        } );
    }
    
    
    @Test
    public void testCreatePutJobWithDuplicateObjectsToPutNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final BasicTestsInvocationHandler jobCreatedListenerBtih = new BasicTestsInvocationHandler( null );
        final JobCreatedListener jobCreatedListener = 
                InterfaceProxyFactory.getProxy( JobCreatedListener.class, jobCreatedListenerBtih );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final JobCreator jobCreator = newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) );
        jobCreator.addJobCreatedListener( jobCreatedListener );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                jobCreator,
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "folder/data.txt" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "folder/data2.txt" ).setSizeInBytes( 122 );
        final S3ObjectToCreate data3 = BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "folder/data.txt" ).setSizeInBytes( 12 );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( 
                                new S3ObjectToCreate [] { data1, data2, data3 } ) ).getWithoutBlocking();
            }
        } );
    }
    
    
    @Test
    public void testCreatePutJobWithOneFolderReturnsNonNullJobId()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate folder = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder/" );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { folder } ) ).getWithoutBlocking(), "Shoulda created job.");
        assertEquals(1, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the folder.");
        assertEquals(1, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Shoulda created a job.");
    }
    
    
    @Test
    public void testCreatePutJobWithManyFoldersReturnsNonNullJobId()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final Method methodJobCreated = ReflectUtil.getMethod( JobCreatedListener.class, "jobCreated" );
        final BasicTestsInvocationHandler jobCreatedListenerBtih = new BasicTestsInvocationHandler( null );
        final JobCreatedListener jobCreatedListener = 
                InterfaceProxyFactory.getProxy( JobCreatedListener.class, jobCreatedListenerBtih );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final JobCreator jobCreator = newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) );
        jobCreator.addJobCreatedListener( jobCreatedListener );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                jobCreator,
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate folder1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder1/" );
        final S3ObjectToCreate folder2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder2/" );
        assertEquals(0, jobCreatedListenerBtih.getMethodCallCount(methodJobCreated), "Should notta been a job created notification sent.");
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { folder1, folder2 } ) ).getWithoutBlocking(), "Shoulda created job.");
        assertEquals(1, jobCreatedListenerBtih.getMethodCallCount(methodJobCreated), "Shoulda been a job created notification sent.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the folders.");
        assertEquals(2, dbSupport.getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.CREATION_DATE, null)), "Shoulda created the folders.");
        assertEquals(2, dbSupport.getDataManager().getCount(
                Blob.class,
                Require.nothing()), "Shoulda created the folders.");
        assertEquals(1, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Shoulda created job.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created job.");
    }
    
    
    @Test
    public void testCreatePutJobWithOneDataObjectSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final Method methodJobCreated = ReflectUtil.getMethod( JobCreatedListener.class, "jobCreated" );
        final BasicTestsInvocationHandler jobCreatedListenerBtih = new BasicTestsInvocationHandler( null );
        final JobCreatedListener jobCreatedListener = 
                InterfaceProxyFactory.getProxy( JobCreatedListener.class, jobCreatedListenerBtih );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final JobCreator jobCreator = newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) );
        jobCreator.addJobCreatedListener( jobCreatedListener );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                jobCreator,
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );
        assertEquals(0, jobCreatedListenerBtih.getMethodCallCount(methodJobCreated), "Should notta been a job created notification sent yet.");
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setName( "proliant" )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(1, jobCreatedListenerBtih.getMethodCallCount(methodJobCreated), "Shoulda sent a job created notification.");
        assertEquals(1, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the object.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entry.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_TAPE_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals("proliant", mockDaoDriver.attainOneAndOnly( Job.class ).getName(), "Shoulda respected job name.");
    }
    
    
    @Test
    public void testCreatePutJobMarksDataPolicyAsIncompatibleWithFullLtfsCompWhenInvalidFileName()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final Method methodJobCreated = ReflectUtil.getMethod( JobCreatedListener.class, "jobCreated" );
        final BasicTestsInvocationHandler jobCreatedListenerBtih = new BasicTestsInvocationHandler( null );
        final JobCreatedListener jobCreatedListener = 
                InterfaceProxyFactory.getProxy( JobCreatedListener.class, jobCreatedListenerBtih );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final JobCreator jobCreator = newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) );
        jobCreator.addJobCreatedListener( jobCreatedListener );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                jobCreator,
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
    
        final S3ObjectToCreate data = BeanFactory.newBean( S3ObjectToCreate.class )
                                                 .setName(
                                                         "file/" + String.join( "", Collections.nCopies( 256, "a" ) ) +
                                                                 "/01" )
                                                 .setSizeInBytes( 12 );
        assertEquals(0, jobCreatedListenerBtih.getMethodCallCount(methodJobCreated), "Should notta been a job created notification sent yet.");
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setName( "proliant" )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(1, jobCreatedListenerBtih.getMethodCallCount(methodJobCreated), "Shoulda sent a job created notification.");
        assertEquals(1, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the object.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entry.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_TAPE_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals("proliant", mockDaoDriver.attainOneAndOnly( Job.class ).getName(), "Shoulda respected job name.");
        final DataPolicyService dataPolicyService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        assertFalse(dataPolicyService.areStorageDomainsWithObjectNamingAllowed(
                        mockDaoDriver.attainOneAndOnly( DataPolicy.class ) ), "Shoulda marked data policy as not allowing FULL ltfs compatibility.");
    }
    
    
    @Test
    public void testCreatePutJobWithManyDataObjectsWhenZeroLengthObjectsDoesNotAllocateAnything()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 1 blob, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 0 );
        final S3ObjectToCreate data2 = // 1 blob, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 0 );
        final S3ObjectToCreate data3 = // 1 blob, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 0 );
        final S3ObjectToCreate data4 = // 1 blob, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 0 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(4, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should notta allocated any of the job chunks.");
    }
    
    
    @Test
    public void testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarily1()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 1 blob, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 1 blob, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 1 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 2 blobs, chunks 3 and 4
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 90L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(5, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(5, dbSupport.getDataManager().getCount(
                DetailedJobEntry.class,
                s_notInCache), "Should notta allocated any of the job chunks.");
    }
    
    
    @Test
    public void testCreatePutJobWithManyHugeDataObjectsFailsForTooManyBlobs()
    {
        dbSupport.getServiceManager()
                 .getService( BucketService.class )
                 .initializeLogicalSizeCache();
        
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager =
                new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(), new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor = InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource =
                newDataPlannerResourceImpl( deadJobMonitor, rpcServer, dbSupport.getServiceManager(), cacheManager,
                        newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                        new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                        tapeBlobStore, poolBlobStore );
    
        final int numObjects = 100;
        S3ObjectToCreate toc[] = new S3ObjectToCreate[ numObjects ];
        for ( int i = 0; i < numObjects; i++ )
        {
            toc[ i ] = BeanFactory.newBean( S3ObjectToCreate.class )
                                  .setName( "some/data" + i )
                                  .setSizeInBytes( 6000L * 1024 * 1024 );
        }
        
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, () -> resource.createPutJob(
                BeanFactory.newBean( CreatePutJobParams.class )
                           .setUserId( bucket.getUserId() )
                           .setBucketId( bucket.getId() )
                           .setObjectsToCreate( toc ) )
                                                                               .getWithoutBlocking() );
    }


    @Test
    public void testCreatePutJobWithManyHugeDataObjectsFailsForTooManyBlobsWhenSomeAreSizeZero()
    {
        dbSupport.getServiceManager()
                .getService( BucketService.class )
                .initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager =
                new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(), new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor = InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource =
                newDataPlannerResourceImpl( deadJobMonitor, rpcServer, dbSupport.getServiceManager(), cacheManager,
                        newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                        new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                        tapeBlobStore, poolBlobStore );

        final int numObjects = 100;
        S3ObjectToCreate toc[] = new S3ObjectToCreate[ numObjects + 1];
        for ( int i = 0; i < numObjects; i++ )
        {
            toc[ i ] = BeanFactory.newBean( S3ObjectToCreate.class )
                    .setName( "some/data" + i )
                    .setSizeInBytes( 5000L * 1024 * 1024 );
        }
        toc[numObjects] = BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "some/data" + numObjects )
                .setSizeInBytes( 0 ); // this one is zero length


        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, () -> resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                                .setUserId( bucket.getUserId() )
                                .setBucketId( bucket.getId() )
                                .setObjectsToCreate( toc ) )
                .getWithoutBlocking() );
    }
    
    
    @Test
    public void testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarily2()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 10, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 3 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunks 1 and 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 2 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 9 blobs, chunks 2 and 3
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 90L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(18, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    @Test
    public void
  testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarilyWhnSmllMUSFromDataPlcy()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        dbSupport.getServiceManager().getService( DataPolicyService.class ).update(
                dataPolicy.setDefaultBlobSize( Long.valueOf( 10 * 1024 * 1024 ) ), 
                DataPolicy.DEFAULT_BLOB_SIZE );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 3 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 9 blobs, chunks 2 and 3
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 90L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(18, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }


    @Test
    public void 
  testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarilyWhenSmallMaxUploadSize()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 3 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunks 1 and 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 2 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 9 blobs, chunks 2 and 3
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 90L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( 10 * 1024 * 1024 ) )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(18, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(3, dbSupport.getDataManager().getCount(
                JobEntry.class,
                Require.exists(
                        BlobObservable.BLOB_ID,
                        Require.exists(
                                Blob.OBJECT_ID,
                                Require.beanPropertyEquals(
                                        NameObservable.NAME, "some/data1")))), "Shoulda created the job chunks.");
        assertEquals(4, dbSupport.getDataManager().getCount(
                JobEntry.class,
                Require.exists(
                        BlobObservable.BLOB_ID,
                        Require.exists(
                                Blob.OBJECT_ID,
                                Require.beanPropertyEquals(
                                        NameObservable.NAME, "some/data2")))), "Shoulda created the job chunks.");
        assertEquals(2, dbSupport.getDataManager().getCount(
                JobEntry.class,
                Require.exists(
                        BlobObservable.BLOB_ID,
                        Require.exists(
                                Blob.OBJECT_ID,
                                Require.beanPropertyEquals(
                                        NameObservable.NAME, "some/data3")))), "Shoulda created the job chunks.");
        assertEquals(9, dbSupport.getDataManager().getCount(
                JobEntry.class,
                Require.exists(
                        BlobObservable.BLOB_ID,
                        Require.exists(
                                Blob.OBJECT_ID,
                                Require.beanPropertyEquals(
                                        NameObservable.NAME, "some/data4")))), "Shoulda created the job chunks.");
    }
    
    
    @Test
    public void
  testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarilyWhenMedMaxUploadSize()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 3 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 2 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 9 blobs, chunks 3 and 4
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 90L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( 80 * 1024 * 1024 ) )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(5, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void
  testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarilyWhenLargeMaxUploadSize()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 3 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 2 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 9 blobs, chunks 3 and 4
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 90L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( 90 * 1024 * 1024 ) )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void
  testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarilyWhenMaxMaxUploadSize1()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 3 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 2 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 9 blobs, chunks 3 and 4
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 90L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( Long.MAX_VALUE ) )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2, data3, data4 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void 
  testCreatePutJobWithManyDataObjectsDoesNotBreakObjectsUpAcrossJobChunksUnnecessarilyWhenMaxMaxUploadSize2()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 10, 10, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 3 * 1000L * 1024 * 1024 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 4 * 1000L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( Long.MAX_VALUE ) )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(14, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void 
    testCreatePutJobWhenMaxChunkSizeLessThanPreferredBlobSizeResultsInBlobSizeEqualToMaxChunkSize()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( 
                        cacheManager, dbSupport.getServiceManager(), 1024, 1024, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 =
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( (long)
                        ( cacheFilesystemDriver.getFilesystem().getMaxCapacityInBytes().longValue() 
                             * 1.01 ) );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( Long.MAX_VALUE ) )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertTrue(1 < dbSupport.getDataManager().getCount( JobEntry.class, Require.nothing() ), "Shoulda gone off max chunk size and not preferred blob size.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testCreatePutJobWithManyDataObjectsSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( Long.MAX_VALUE ) )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testAggregatingPutJobIdsReturnedToClientAreRetainedByServer()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        internalTestAggregatingJobIdsReturnedToClientAreRetainedByServer( JobRequestType.PUT );
    }
    
    
    @Test
    public void testAggregatingGetJobIdsReturnedToClientAreRetainedByServer()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        internalTestAggregatingJobIdsReturnedToClientAreRetainedByServer( JobRequestType.GET );     
    }
    
    
    @Test
    public void testAggregatingVerifyJobIdsReturnedToClientAreRetainedByServer()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        internalTestAggregatingJobIdsReturnedToClientAreRetainedByServer( JobRequestType.VERIFY );
    }
    
    
    private void internalTestAggregatingJobIdsReturnedToClientAreRetainedByServer( final JobRequestType type )
            throws InterruptedException, ExecutionException, TimeoutException
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null ),
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore);
        final Set<Future<?>> workPoolFutures = new HashSet<>();
        final Set<UUID> returnedJobIds = Collections.synchronizedSet( new HashSet<UUID>() );
        final Runnable createJobTask = () -> {
            final RpcFuture< UUID > future;
            if ( type == JobRequestType.PUT )
            {
                final S3ObjectToCreate obj = BeanFactory.newBean( S3ObjectToCreate.class )
                        .setName( UUID.randomUUID().toString() ).setSizeInBytes( 12 );
                future = resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                                .setUserId( bucket.getUserId() )
                                .setBucketId( bucket.getId() )
                                .setMaxUploadSizeInBytes( Long.valueOf( Long.MAX_VALUE ) )
                                .setObjectsToCreate(
                                        new S3ObjectToCreate [] { obj } )
                                .setAggregating( true ) );
            }
            else
            {
                final S3Object obj = mockDaoDriver.createObject(
                        bucket.getId(), UUID.randomUUID().toString(), 12 );
                final Blob blob = mockDaoDriver.getBlobFor( obj.getId() );
                mockDaoDriver.putBlobOnTape( null, blob.getId() );
                if ( type == JobRequestType.GET )
                {
                    future = resource.createGetJob(
                            BeanFactory.newBean( CreateGetJobParams.class )
                                    .setUserId( bucket.getUserId() )
                                    .setBlobIds( new UUID[] { blob.getId() } )
                                    .setAggregating( true ) );
                }
                else if ( type == JobRequestType.VERIFY )
                {
                    future = resource.createVerifyJob(
                            BeanFactory.newBean( CreateVerifyJobParams.class )
                                    .setUserId( bucket.getUserId() )
                                    .setBlobIds( new UUID[] { blob.getId() } )
                                    .setAggregating( true ) );
                }
                else
                {
                    throw new UnsupportedOperationException(
                            "No code to support job type " + type.name() );
                }
            }
            returnedJobIds.add( future.get( 60, TimeUnit.SECONDS ) );
        };

        final int numThreads = 10;
        final WorkPool wp = WorkPoolFactory.createWorkPool( numThreads, getClass().getSimpleName() );
        try
        {
            for ( int i = 0; i < numThreads; i++ )
            {
                workPoolFutures.add( wp.submit( createJobTask ) );
            }
            for ( Future< ? > future : workPoolFutures )
            {
                future.get( 300, TimeUnit.SECONDS );
            }
            final List<Job> jobsOnServer =
                    dbSupport.getServiceManager().getRetriever( Job.class ).retrieveAll().toList();
            final List<UUID> jobIdsOnServer = new ArrayList<>();
            for ( Job job : jobsOnServer )
            {
                jobIdsOnServer.add( job.getId() );
            }
            for ( UUID id: returnedJobIds )
            {
                assertTrue(jobIdsOnServer.contains( id ), "Shoulda retained all job IDs that we sent back to the client");
            }
        }
        finally
        {
            wp.shutdownNow();
        }
    }
        
    
    @Test
    public void testCreatePutJobWithManyDataObjectsWhereSomeExistButIgnoreConflictsSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        mockDaoDriver.createObject( bucket.getId(), "some/data2", 12 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setIgnoreNamingConflicts( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Should notta returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects not already created.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries for objects not already created.");
    }
    
    
    @Test
    public void testCreatePutJobWithManyDataObjectsWhereAllExistButIgnoreConflictsThrowsNoWorkToPerform()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        mockDaoDriver.createObject( bucket.getId(), "some/data1", 12 );
        mockDaoDriver.createObject( bucket.getId(), "some/data2", 12 );
        
        TestUtil.assertThrows( null, GenericFailure.GONE, new BlastContainer()
        {
            public void test()
            {
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setIgnoreNamingConflicts( true )
                        .setObjectsToCreate( 
                                new S3ObjectToCreate [] { data1, data2 } ) );
            }
        } );
    }
    
    
    @Test
    public void testCreatePutJobWithVeryLargeObjectBreaksUpObjectIntoBlobs()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 3 * mb );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(5, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void
    testCreatePutJobWithVeryLargeObjectWhenDataPolicyDisallowsBlobbingDoesNotBreakUpObjectIntoBlobs()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean(
                dataPolicy.setBlobbingEnabled( false ),
                DataPolicy.BLOBBING_ENABLED );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 3 * mb );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void
    testCreatePutJobWithVeryLargeObjectWhenDataPolicyDisallowsBlobbingNecessaryNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean(
                dataPolicy.setBlobbingEnabled( false ),
                DataPolicy.BLOBBING_ENABLED );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 501 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 3 * mb );

        TestUtil.assertThrows(
                null,
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test()
                    {
                        resource.createPutJob( 
                                BeanFactory.newBean( CreatePutJobParams.class )
                                .setUserId( bucket.getUserId() )
                                .setBucketId( bucket.getId() )
                                .setObjectsToCreate( 
                                        new S3ObjectToCreate [] { data1, data2 } ) );
                    }
                } );
    }
    
    
    @Test
    public void testCreatePutJobWithManyDataAndFolderObjectsSetsUpJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        final S3ObjectToCreate data0 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/nodata" ).setSizeInBytes( 0 );
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        final S3ObjectToCreate folder = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder/" );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data0, data1, data2, folder } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects and folder.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobWithMaxUploadSizeTooSmallNotAllowed()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final BasicTestsInvocationHandler jobCreatedListenerBtih = new BasicTestsInvocationHandler( null );
        final JobCreatedListener jobCreatedListener = 
                InterfaceProxyFactory.getProxy( JobCreatedListener.class, jobCreatedListenerBtih );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final JobCreator jobCreator = newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) );
        jobCreator.addJobCreatedListener( jobCreatedListener );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                jobCreator,
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMaxUploadSizeInBytes( Long.valueOf( 1024 * 1024 ) )
                        .setObjectsToCreate( 
                                new S3ObjectToCreate [] { data } ) );
            }
        } );
    }
    
    
    @Test
    public void testCreatePutJobAggregatingAlwaysAppendsToOriginalAggregatingJob()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 10L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 6 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 8 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMinimizeSpanningAcrossMedia( true )
                        .setAggregating( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        final Job job = dbSupport.getServiceManager().getRetriever( Job.class ).attain( Require.nothing() );
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");

        Object a1 = job.getId();
        assertEquals(a1, resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMinimizeSpanningAcrossMedia( true )
                        .setAggregating( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data3 } ) )
                                .getWithoutBlocking(), "Shoulda appended to existing job.");
        assertEquals(3, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(3, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");

        Object a = job.getId();
        assertEquals(a, resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMinimizeSpanningAcrossMedia( true )
                        .setAggregating( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data4 } ) )
                                .getWithoutBlocking(), "Shoulda appended to existing job.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertTrue(job.isMinimizeSpanningAcrossMedia(), "Shoulda created job with correct configuration.");
    }
    
    
    @Test
    public void testCreatePutJobAggregatingReshapesAsLongAsOriginalJobRemainsAggregating()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 60, 60, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 10L * 1024 * 1024 );
        final S3ObjectToCreate data2 = // 4 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 20L * 1024 * 1024 );
        final S3ObjectToCreate data3 = // 6 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data3" )
                .setSizeInBytes( 30L * 1024 * 1024 );
        final S3ObjectToCreate data4 = // 8 blobs, chunk 2
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data4" )
                .setSizeInBytes( 40L * 1024 * 1024 );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMinimizeSpanningAcrossMedia( true )
                        .setAggregating( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        final Job job = dbSupport.getServiceManager().getRetriever( Job.class ).attain( Require.nothing() );
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(0, mockDaoDriver.attain(job).getCachedSizeInBytes(), "Shoulda computed correct job statistics.");
        assertEquals(0, mockDaoDriver.attain(job).getCompletedSizeInBytes(), "Shoulda computed correct job statistics.");
        Object a3 = data1.getSizeInBytes() + data2.getSizeInBytes();
        assertEquals(a3, mockDaoDriver.attain( job ).getOriginalSizeInBytes(), "Shoulda computed correct job statistics.");

        Object a2 = job.getId();
        assertEquals(a2, resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setAggregating( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data3 } ) )
                                .getWithoutBlocking(), "Shoulda appended to existing job.");
        assertEquals(3, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(3, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(0, mockDaoDriver.attain(job).getCachedSizeInBytes(), "Shoulda computed correct job statistics.");
        assertEquals(0, mockDaoDriver.attain(job).getCompletedSizeInBytes(), "Shoulda computed correct job statistics.");
        Object a1 = data1.getSizeInBytes() + data2.getSizeInBytes() + data3.getSizeInBytes();
        assertEquals(a1, mockDaoDriver.attain( job ).getOriginalSizeInBytes(), "Shoulda computed correct job statistics.");

        mockDaoDriver.updateBean( job.setAggregating( false ), Job.AGGREGATING );
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setAggregating( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data4 } ) )
                                .getWithoutBlocking(), "Should notta appended to existing job.");
        assertEquals(2, dbSupport.getDataManager().getCount(Job.class, Require.nothing()), "Shoulda created a second job.");
        assertEquals(4, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(0, mockDaoDriver.attain(job).getCachedSizeInBytes(), "Shoulda computed correct job statistics.");
        assertEquals(0, mockDaoDriver.attain(job).getCompletedSizeInBytes(), "Shoulda computed correct job statistics.");
        Object a = data1.getSizeInBytes() + data2.getSizeInBytes() + data3.getSizeInBytes();
        assertEquals(a, mockDaoDriver.attain( job ).getOriginalSizeInBytes(), "Shoulda computed correct job statistics.");
        assertTrue(job.isMinimizeSpanningAcrossMedia(), "Shoulda created job with correct configuration.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenNoConstraints1()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 3 * mb ); // 3 blobs, chunks 1 and 2
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(5, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenNoConstraints2()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 6 * mb ); // 6 blobs, chunks 1 and 2
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(8, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenTapeConstraintsPerformanceStorageDomains1()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 25 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 25 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 15 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd1" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd2 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd2" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd3 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd3" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 3 * mb ); // 3 blobs, chunks 2 and 3
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(5, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenTapeConstraintsPerformanceStorageDomains2()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 25 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 25 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 15 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd1" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd2 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd2" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd3 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd3" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 4 * mb ); // 4 blobs, chunks 2 and 3
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(6, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenTapeConstraintsPerformanceStorageDomains3()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( Long.valueOf( 25 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 25 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 25 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 15 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd1" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd2 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd2" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd3 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd3" )
                .setWriteOptimization( WriteOptimization.PERFORMANCE ),
                StorageDomain.WRITE_OPTIMIZATION );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 4 * mb ); // 4 blobs, chunks 2 and 3
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(6, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenTapeConstraintsCapacityStorageDomains0()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 75 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd1" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd2 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd2" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd3 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd3" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 3 * mb ); // 3 blobs, chunks 2 and 3
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(5, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenTapeConstraintsCapacityStorageDomains1()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 75 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd1" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd2 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd2" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd3 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd3" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 3 * mb ); // 3 blobs, chunks 2 and 3
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(5, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobDynamicallyDeterminesChunkSizeWhenTapeConstraintsCapacityStorageDomains2()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 75 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd1" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd2 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd2" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd3 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd3" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 4 * mb ); // 4 blobs, chunks 2 and 3
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(6, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreatePutJobRespectsMinimizeSpanningAcrossMediaIfSpecified()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 75 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd1" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd2 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd2" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        final StorageDomain sd3 = mockDaoDriver.updateBean( 
                mockDaoDriver.createStorageDomain( "sd3" )
                .setWriteOptimization( WriteOptimization.CAPACITY ),
                StorageDomain.WRITE_OPTIMIZATION );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final long mb = 1024 * 1024;
        final S3ObjectToCreate data1 = // 2 blobs, chunk 1
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 2 * mb );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 4 * mb ); // 4 blobs, chunks 2 and 3
        assertNotNull(resource.createPutJob(
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setMinimizeSpanningAcrossMedia( true )
                        .setObjectsToCreate(
                                new S3ObjectToCreate[] { data1, data2 } ) )
                                .getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the objects.");
        assertEquals(6, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
    }
    
    
    @Test
    public void testCreateNonPutJobDynamicallyDeterminesChunkSizeJustReturnsPreferredBlobSize()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockCacheFilesystemDriver cacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO5 )
                .setTotalRawCapacity( null ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.LTO6 )
                .setTotalRawCapacity( Long.valueOf( 50 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape( null, null, TapeType.TS_JC )
                .setTotalRawCapacity( Long.valueOf( 75 * 1024 * 1024 ) ),
                Tape.TOTAL_RAW_CAPACITY );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd3.getId(), tp1.getId(), TapeType.TS_JC );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.RETIRED, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport.getServiceManager(), 1, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = mockDaoDriver.getBlobFor( data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( data2.getId() );
        final Set<JobEntry> chunksToDelete =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob1 ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(chunksToDelete).keySet()) );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        assertNotNull(resource.createGetJob(
                        BeanFactory.newBean( CreateGetJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBlobIds( new UUID[] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking(), "Shoulda returned null.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.COMPLETED)), "Shoulda marked entry already in cache as complete");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class,
                Require.not(Require.beanPropertyEquals(
                        ReadFromObservable.READ_FROM_TAPE_ID, null))), "Shoulda chunked up the job with the correct read from state.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testReplicatePutJobDoesSo()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy policy = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null, policy.getId(),"b1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry entry = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final DetailedJobToReplicate jobToReplicate = new JobReplicationSupport(
                dbSupport.getServiceManager(), entry.getJobId() ).getJobToReplicate();
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Job.class );

        final Method methodJobCreated = ReflectUtil.getMethod( JobCreatedListener.class, "jobCreated" );
        final BasicTestsInvocationHandler jobCreatedListenerBtih = new BasicTestsInvocationHandler( null );
        final JobCreatedListener jobCreatedListener = 
                InterfaceProxyFactory.getProxy( JobCreatedListener.class, jobCreatedListenerBtih );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final JobCreator jobCreator = newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) );
        jobCreator.addJobCreatedListener( jobCreatedListener );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                jobCreator,
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        Object a = jobToReplicate.getJob().getId();
        assertEquals(a, resource.replicatePutJob( jobToReplicate ).getWithoutBlocking(), "Shoulda returned new job id.");

        assertEquals(1, jobCreatedListenerBtih.getMethodCallCount(methodJobCreated), "Shoulda sent a job created notification.");
        assertEquals(1, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda created the object.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entry.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "Shoulda chunked up the job with the correct initial blob store state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_POOL_ID, null)), "Shoulda chunked up the job with the correct read from state.");
        assertEquals(1, dbSupport.getDataManager().getCount(JobEntry.class, Require.beanPropertyEquals(
                ReadFromObservable.READ_FROM_TAPE_ID, null)), "Shoulda chunked up the job with the correct read from state.");
    }
    
    
    @Test
    public void testCancelJobWithNullJobIdNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                resource.cancelJob( null, null, false );
            }
        } );
    }
    
    
    @Test
    public void testCancelIOMPutJobCancelsCounterpartJob()
    {
        
            dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
                new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        final DataMigration migration = BeanFactory.newBean( DataMigration.class );
        final Job getJob = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.GET )
                .setUserId( mockDaoDriver.attainOneAndOnly( User.class ).getId() )
                .setIomType(IomType.STANDARD_IOM);
        dbSupport.getServiceManager().getService( JobService.class ).create( getJob );
        final Job putJob = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.PUT )
                .setUserId( mockDaoDriver.attainOneAndOnly( Job.class ).getUserId() )
                .setIomType(IomType.STANDARD_IOM);
        dbSupport.getServiceManager().getService( JobService.class ).create( putJob );
        dbSupport.getServiceManager().getService( DataMigrationService.class )
            .create( migration
                    .setGetJobId( getJob.getId() )
                    .setPutJobId(putJob.getId() ) );
        assertEquals(2, dbSupport.getServiceManager().getRetriever(Job.class).getCount(), "Shoulda been 2 jobs prior to cancel");

        assertEquals(1, dbSupport.getServiceManager().getRetriever(DataMigration.class).getCount(), "Shoulda been 1 data migration prior to cancel");

        resource.cancelJobInternal( putJob.getId(), false );

        assertEquals(0, dbSupport.getServiceManager().getRetriever(Job.class).getCount(), "Shoulda been cancelled both jobs");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(DataMigration.class).getCount(), "Shoulda deleted data migration");
    }
    
    
    @Test
    public void testCancelIOMGetJobCancelsCounterpartJob()
    {
        
            dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
                new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        final DataMigration migration = BeanFactory.newBean( DataMigration.class );
        final Job getJob = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.GET )
                .setUserId( mockDaoDriver.attainOneAndOnly( User.class ).getId() )
                .setIomType(IomType.STANDARD_IOM);
        dbSupport.getServiceManager().getService( JobService.class ).create( getJob );
        final Job putJob = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.PUT )
                .setUserId( mockDaoDriver.attainOneAndOnly( Job.class ).getUserId() )
                .setIomType(IomType.STANDARD_IOM);
        dbSupport.getServiceManager().getService( JobService.class ).create( putJob );
        dbSupport.getServiceManager().getService( DataMigrationService.class )
            .create( migration
                    .setGetJobId( getJob.getId() )
                    .setPutJobId(putJob.getId() ) );
        assertEquals(2, dbSupport.getServiceManager().getRetriever(Job.class).getCount(), "Shoulda been 2 jobs prior to cancel");

        assertEquals(1, dbSupport.getServiceManager().getRetriever(DataMigration.class).getCount(), "Shoulda been 1 data migration prior to cancel");

        resource.cancelJobInternal( getJob.getId(), false );

        assertEquals(0, dbSupport.getServiceManager().getRetriever(Job.class).getCount(), "Shoulda been cancelled both jobs");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(DataMigration.class).getCount(), "Shoulda deleted data migration");
    }
    
    
    @Test
    public void testCancelJobDoesNotAutoEjectTapeWhenAutoEjectionNotApplicable()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        mockDaoDriver.updateBean( 
                storageDomain.setAutoEjectUponJobCancellation( false ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( 
                tape.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ), 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final UUID jobId = resource.createGetJob( 
                BeanFactory.newBean( CreateGetJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking();
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        final Set< UUID > objectIds = resource.cancelJobInternal( jobId, false );
        assertEquals(0, objectIds.size(), "Should notta deleted the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda notta deleted the objects.");
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda deleted the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(
                CanceledJob.class,
                Require.beanPropertyEquals(Identifiable.ID, jobId)), "Shoulda created canceled job entry.");
        assertEquals(0, btih.getMethodCallCount(ReflectUtil.getMethod(TapeEjector.class, "ejectTape")), "Should notta ejected tape.");
    }
    
    
    @Test
    public void testCancelJobAutoEjectsTapesWhenApplicable()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        mockDaoDriver.updateBean( 
                storageDomain.setAutoEjectUponJobCancellation( true ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( 
                tape.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ), 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final UUID jobId = resource.createGetJob( 
                BeanFactory.newBean( CreateGetJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking();
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        final Set< UUID > objectIds = resource.cancelJobInternal( jobId, false );
        assertEquals(0, objectIds.size(), "Should notta deleted the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda notta deleted the objects.");
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda deleted the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(
                CanceledJob.class,
                Require.beanPropertyEquals(Identifiable.ID, jobId)), "Shoulda created canceled job entry.");
        assertEquals(1, btih.getMethodCallCount(ReflectUtil.getMethod(TapeEjector.class, "ejectTape")), "Shoulda ejected tape.");
    }
    
    
    @Test
    public void testCancelJobCancelsGetJob()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        final UUID jobId = resource.createGetJob( 
                BeanFactory.newBean( CreateGetJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) ).getWithoutBlocking();
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        final Set< UUID > objectIds = resource.cancelJobInternal( jobId, false );
        assertEquals(1, dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> (data.getArgs().get(0) instanceof JobNotificationEvent)), "Shoulda generated a job completion notification.");
        assertEquals(0, objectIds.size(), "Should notta deleted the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda notta deleted the objects.");
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda deleted the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(
                CanceledJob.class,
                Require.beanPropertyEquals(Identifiable.ID, jobId)), "Shoulda created canceled job entry.");
    }
    
    
    @Test
    public void testCancelJobCancelsVerifyJob()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3Object data1 = mockDaoDriver.createObject( bucket.getId(), "some/object" );
        final Blob blob1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data1.getId() );
        final S3Object data2 = mockDaoDriver.createObject( bucket.getId(), "some/object2" );
        final Blob blob2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, data2.getId() );
        mockDaoDriver.putBlobOnTape( null, blob1.getId() );
        mockDaoDriver.putBlobOnTape( null, blob2.getId() );
        final UUID jobId = resource.createVerifyJob( 
                BeanFactory.newBean( CreateVerifyJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBlobIds( new UUID [] { blob1.getId(), blob2.getId() } ) )
                .getWithoutBlocking();
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");

        final Set< UUID > objectIds = resource.cancelJobInternal( jobId, false );
        assertEquals(1, dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> (data.getArgs().get(0) instanceof JobNotificationEvent)), "Shoulda generated a job completion notification.");
        assertEquals(0, objectIds.size(), "Should notta deleted the objects.");
        assertEquals(2, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda notta deleted the objects.");
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda deleted the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(
                CanceledJob.class,
                Require.beanPropertyEquals(Identifiable.ID, jobId)), "Shoulda created canceled job entry.");
    }
    
    
    @Test
    public void testCancelJobCancelsPutJobNotStarted()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        final S3ObjectToCreate folder = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder/" );
        final S3ObjectToCreate folder2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder2/" );
        final UUID jobId = resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( 
                        new S3ObjectToCreate [] { data1, data2, folder, folder2 } ) )
                        .getWithoutBlocking();
        assertEquals(4, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");
        final Set< UUID > objectIds = resource.cancelJobInternal( jobId, false );
        assertEquals(4, objectIds.size(), "Shoulda deleted the objects.");
        assertEquals(0, dbSupport.getDataManager().getCount(S3Object.class, Require.nothing()), "Shoulda deleted the objects.");
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda deleted the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(
                CanceledJob.class,
                Require.beanPropertyEquals(Identifiable.ID, jobId)), "Shoulda created canceled job entry.");
    }
    
    
    @Test
    public void testCancelJobWithoutForceFlagDoesNotCancelPutJobPartiallyCompleted()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        final S3ObjectToCreate data0 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data0" )
                .setSizeInBytes( 12 );
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 2 * 1024 * 1024 );
        final S3ObjectToCreate folder = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder/" );
        final S3ObjectToCreate folder2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder2/" );
        final UUID jobId = resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( 
                        new S3ObjectToCreate [] { data0, data1, data2, folder, folder2 } ) )
                        .getWithoutBlocking();
        assertEquals(6, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");

        final Tape tape = mockDaoDriver.createTape();
        final S3Object object0 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/data0" );
        mockDaoDriver.getBlobFor( object0.getId() );
        final S3Object object1 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/data1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( object1.getId() );
        final S3Object object2 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/data2" );
        final S3Object f2 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/folder2/" );
        final List< Blob > blobs = dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll(
                Blob.OBJECT_ID, object2.getId() ).toList();
        assertEquals(2, blobs.size(), "Shoulda been 2 blobs for object2.");
        mockDaoDriver.simulateObjectUploadCompletion( object2.getId() );
        mockDaoDriver.simulateObjectUploadCompletion( f2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs.get( 0 ).getId() );
        dbSupport.getDataManager().deleteBeans( 
                JobEntry.class,
                Require.beanPropertyEqualsOneOf( 
                        BlobObservable.BLOB_ID, blob1.getId(), blobs.get( 0 ).getId() ) );

        assertEquals(12 + 12 + 2 * 1024 * 1024, dbSupport.getServiceManager().getRetriever(Job.class).attain(
                Require.nothing()).getOriginalSizeInBytes(), "Should notta reduced original job size yet.");
        CancelJobFailedException ex =
                (CancelJobFailedException)
                TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED_OK, new BlastContainer()
                {
                    public void test()
                    {
                        resource.cancelJob( null, jobId, false );
                    }
                } );
        assertEquals(2, ex.getDeletedObjectIds().size(), "Shoulda deleted data0 and folder.");
        assertEquals(12 + 2 * 1024 * 1024, dbSupport.getServiceManager().getRetriever(Job.class).attain(
                Require.nothing()).getOriginalSizeInBytes(), "Shoulda reduced original job size.");
        Object a = CollectionFactory.toSet( "some/data1", "some/data2", "some/folder2/" );
        assertEquals(a, BeanUtils.extractPropertyValues(
                        dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieveAll(
                                Require.nothing() ).toSet(),
                        NameObservable.NAME ), "Shoulda deleted data0 and folder.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieve( object0.getId() ), "Shoulda whacked data0.");
        assertNotNull(dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieve( object2.getId() ), "Should notta whacked data2.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entries for data2 or folder2.");
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertTrue(job.isTruncated(), "Shoulda recorded job truncation.");
        assertEquals(0, dbSupport.getDataManager().getCount(
                CanceledJob.class,
                Require.beanPropertyEquals(Identifiable.ID, jobId)), "Should notta created canceled job entry.");

        mockDaoDriver.updateAllBeans( job.setTruncated( false ), JobObservable.TRUNCATED );
        ex = (CancelJobFailedException)
            TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED, new BlastContainer()
            {
                public void test()
                {
                    resource.cancelJob( null, jobId, false );
                }
            } );
        assertEquals(0, ex.getDeletedObjectIds().size(), "Should notta deleted anything.");
        assertFalse(mockDaoDriver.attainOneAndOnly( Job.class ).isTruncated(), "Should notta recorded job truncation.");
    }


    @Test
    public void testCancelJobWithoutForceFlagDoesNotCancelPutJobWithNoWorkRemaining()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT ).getId();
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta created any job entries.");

        CancelJobFailedException ex = (CancelJobFailedException)
                TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED, new BlastContainer()
                {
                    public void test()
                    {
                        resource.cancelJob( null, jobId, false );
                    }
                } );
        assertEquals(0, ex.getDeletedObjectIds().size(), "Should notta deleted anything.");
        assertFalse(mockDaoDriver.attainOneAndOnly( Job.class ).isTruncated(), "Should notta recorded job truncation.");
    }
    
    
    @Test
    public void testCancelJobWithForceFlagCancelsPutJobPartiallyCompleted()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        final S3ObjectToCreate data0 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data0" )
                .setSizeInBytes( 12 );
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" )
                .setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" )
                .setSizeInBytes( 2 * 1024 * 1024 );
        final S3ObjectToCreate folder = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder/" );
        final S3ObjectToCreate folder2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/folder2/" );
        final UUID jobId = resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( 
                        new S3ObjectToCreate [] { data0, data1, data2, folder, folder2 } ) )
                        .getWithoutBlocking();
        assertEquals(6, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda created the job entries.");

        final Tape tape = mockDaoDriver.createTape();
        final S3Object object0 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/data0" );
        mockDaoDriver.getBlobFor( object0.getId() );
        final S3Object object1 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/data1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( object1.getId() );
        final S3Object object2 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/data2" );
        final S3Object f2 = dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                NameObservable.NAME, "some/folder2/" );
        final List< Blob > blobs = dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll(
                Blob.OBJECT_ID, object2.getId() ).toList();
        assertEquals(2, blobs.size(), "Shoulda been 2 blobs for object2.");
        mockDaoDriver.simulateObjectUploadCompletion( f2.getId() );
        mockDaoDriver.simulateObjectUploadCompletion( object2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs.get( 0 ).getId() );
        dbSupport.getDataManager().deleteBeans( 
                JobEntry.class,
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blob1.getId(), blobs.get( 0 ).getId() ) );
        
        final Set< UUID > objectIds = resource.cancelJobInternal( jobId, true );

        assertEquals(1, dbSupport.getNotificationEventDispatcherBtih().getMethodCallMatchingPredicateCount(
                ReflectUtil.getMethod(NotificationEventDispatcher.class, "fire"),
                (data) -> (data.getArgs().get(0) instanceof JobNotificationEvent)), "Shoulda generated a job completion notification.");
        assertEquals(4, objectIds.size(), "Shoulda deleted data0, data2, and the folders.");
        Object a = CollectionFactory.toSet( "some/data1" );
        assertEquals(a, BeanUtils.extractPropertyValues(
                        dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieveAll(
                                Require.nothing() ).toSet(),
                        NameObservable.NAME ), "Shoulda deleted data0, data2, and the folders.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieve( object0.getId() ), "Shoulda whacked data0.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieve( object2.getId() ), "Shoulda whacked data2.");
        assertEquals(0, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda deleted the job entries.");
        assertEquals(1, dbSupport.getDataManager().getCount(
                CanceledJob.class,
                Require.beanPropertyEquals(Identifiable.ID, jobId)), "Shoulda created canceled job entry.");
    }
    
    
    @Test
    public void testJobStillActiveDoesNotBlowUp()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        resource.jobStillActive( null, null );
        assertEquals(0, btih.getTotalCallCount(), "Should notta made call on pool lock support to renew lock on blob since blob unknown.");
    }
    
    
    @Test
    public void testJobStillActiveWithBlobSpecifiedDoesNotBlowUp()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final UUID blobId = UUID.randomUUID();
        resource.jobStillActive( null, blobId );
        assertEquals(0, btih.getTotalCallCount(), "Should notta made any call on pool lock support.");
    }
    
    
    @Test
    public void testStartBlobReadWhenBlobNotInCacheNorOnPoolStorageNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return null;
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        TestUtil.assertThrows(
                null,
                GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT,
                new BlastContainer()
                {
                    public void test()
                    {
                        resource.startBlobRead( null, UUID.randomUUID() );
                    }
                } );
        assertEquals(0, btih.getTotalCallCount(), "Should notta made call on pool lock support to acquire lock on blob.");
    }
    
    
    @Test
    public void testStartBlobReadWhenBlobNotInCacheButIsOnPoolStorageAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return BeanFactory.newBean( DiskFileInfo.class )
                                .setFilePath( "/some/file.txt" );
                    }
                } );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ), poolBlobStore );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                InterfaceProxyFactory.getProxy(JobCreator.class, null),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertEquals("/some/file.txt", resource.startBlobRead( null, blob.getId() ).getWithoutBlocking().getFilePath(), "Shoulda preferred enterprise over archive, and powered on over powered off.");
    }
    
    
    @Test
    public void testStartBlobReadWhenBlobInCacheAndIsOnPoolStorageServicesFromCache()
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

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( blob.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        Object a = cacheManager.getDiskFileFor( blob.getId() ).getFilePath();
        assertEquals(a, resource.startBlobRead( null, blob.getId() ).getWithoutBlocking().getFilePath(), "Shoulda preferred cache over pool storage.");
        assertEquals(0, btih.getTotalCallCount(), "Should notta made call on pool lock support to acquire lock on blob.");
    }
    
    
    @Test
    public void testStartBlobReadWhenBlobInCacheAndIsNotOnPoolStorageServicesFromCache()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( blob.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        Object a = cacheManager.getDiskFileFor( blob.getId() ).getFilePath();
        assertEquals(a, resource.startBlobRead( null, blob.getId() ).getWithoutBlocking().getFilePath(), "Shoulda serviced from cache.");
        assertEquals(0, btih.getTotalCallCount(), "Should notta made call on pool lock support to acquire lock on blob.");
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobNotPartOfAnyJobNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        mockDaoDriver.getBlobFor( o2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        TestUtil.assertThrows(
                null,
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        resource.blobReadCompleted( null, b1.getId() );
                    }
                } );
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobOnlyPartOfVerifyJobNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.VERIFY, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        TestUtil.assertThrows(
                null,
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        resource.blobReadCompleted( null, b1.getId() );
                    }
                } );
        assertEquals(2, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked any job entries.");
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobOnlyPartOfPutJobNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        TestUtil.assertThrows(
                null,
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        resource.blobReadCompleted( null, b1.getId() );
                    }
                } );
        assertEquals(2, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked any job entries.");
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobPartOfSingleGetJobAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertEquals(0, mockDaoDriver.attain(Job.class, chunks.iterator().next().getJobId()).getCachedSizeInBytes(), "Shoulda initially not reported any cached amount.");
        resource.blobReadCompleted( null, b1.getId() );
        assertEquals(10, mockDaoDriver.attain(Job.class, chunks.iterator().next().getJobId()).getCachedSizeInBytes(), "Shoulda updated job cached amount.");
        assertEquals(1, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda whacked job entries for b1.");

        for (JobEntry chunk : chunks) {
        mockDaoDriver.updateBean(
                chunk.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                JobEntry.BLOB_STORE_STATE );
        }
        cacheManager.blobLoadedToCache( b2.getId() );
        resource.blobReadCompleted( null, b2.getId() );
        assertEquals(10, mockDaoDriver.attain(Job.class, chunks.iterator().next().getJobId()).getCachedSizeInBytes(), "Should notta updated job cached amount.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda whacked job entries for b2.");
    }
    
    
    @Test
    public void 
    testBlobReadCompletedWhenBlobPartOfMultipleNonNakedGetJobsNotAllowedWhenImplicitJobResolutionDisabled()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertEquals(4, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked job entries for b1 yet.");

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                resource.blobReadCompleted( null, b1.getId() );
            }
        } );
        assertEquals(4, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked job entries for b1.");
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobPartOfMultipleNonNakedGetJobsAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertEquals(4, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked job entries for b1 yet.");
        resource.blobReadCompleted( null, b1.getId() );
        assertEquals(2, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda whacked job entries for b1.");
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobPartOfMultipleNakedGetJobsAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setNaked( true ), 
                JobObservable.NAKED );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertEquals(4, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked job entries for b1 yet.");
        resource.blobReadCompleted( null, b1.getId() );
        assertEquals(3, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda whacked job entries for b1 for one job.");
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobPartOfMultipleNakedAndNonNakedetJobsAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.updateBean( 
                mockDaoDriver.attain( Job.class, chunks.iterator().next().getJobId() ).setNaked( true ),
                JobObservable.NAKED );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertEquals(6, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked job entries for b1 yet.");
        resource.blobReadCompleted( null, b1.getId() );
        assertEquals(5, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda whacked job entries for b1 for one job.");
    }
    
    
    @Test
    public void testBlobReadCompletedWhenBlobPartOfSpecifiedGetJobAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b1.getId() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertEquals(4, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked job entries for b1 yet.");
        resource.blobReadCompleted( chunks.iterator().next().getJobId(), b1.getId() );
        assertEquals(3, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda whacked job entry for b1.");
    }
    
    
    @Test
    public void testGetBlobsInCacheDelegatesToCacheManager()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        
        final UUID blobId1 = UUID.randomUUID();
        final UUID blobId2 = UUID.randomUUID();
        final UUID blobId3 = UUID.randomUUID();
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( blobId2 );
        
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final BlobsInCacheInformation response =
                resource.getBlobsInCache( new UUID [] { blobId1, blobId2, blobId3 } ).getWithoutBlocking();
        assertEquals(1, response.getBlobsInCache().length, "Shoulda reported the single blob in cache.");
        assertEquals(blobId2, response.getBlobsInCache()[ 0 ], "Shoulda reported the single blob in cache.");
    }
    
    
    @Test
    public void testIsChunkEntirelyInCacheDelegatesToCacheManager()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final JobEntry chunk1 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final JobEntry chunk2 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b2 );
        
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.blobLoadedToCache( b2.getId() );
        
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        assertFalse(resource.isChunkEntirelyInCache( chunk1.getId() ).getWithoutBlocking().booleanValue(), "b1 not in cache, so shoulda reported chunk not in cache.");
        assertTrue(resource.isChunkEntirelyInCache( chunk2.getId() ).getWithoutBlocking().booleanValue(), "b2 is in cache, so shoulda reported chunk in cache.");
    }
    
    
    @Test
    public void testAllocateEntryDelegatesToCacheManager()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        final JobEntry chunk1 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final JobEntry chunk2 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b2 );
        mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b3, b4 );
        
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = 
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        resource.allocateEntry( chunk1.getId() );
        resource.allocateEntry( chunk2.getId() );

        assertTrue(cacheManager.isCacheSpaceAllocated( b1.getId() ), "Shoulda allocated only those chunks we asked to allocate.");
        assertTrue(cacheManager.isCacheSpaceAllocated( b2.getId() ), "Shoulda allocated only those chunks we asked to allocate.");
        assertFalse(cacheManager.isCacheSpaceAllocated( b3.getId() ), "Shoulda allocated only those chunks we asked to allocate.");
        assertFalse(cacheManager.isCacheSpaceAllocated( b4.getId() ), "Shoulda allocated only those chunks we asked to allocate.");
    }
    
    
    @Test
    public void testGetDataStoreTaskDoesNotBlowUp()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final TapeTask t1 = new NoOpTapeTask( "blah", BlobStoreTaskPriority.NORMAL, UUID.randomUUID(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );
        t1.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        final TapeTask t2 = new NoOpTapeTask( "blah", BlobStoreTaskPriority.NORMAL, UUID.randomUUID(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );
        final TapeTask t3 = new NoOpTapeTask( "blah", BlobStoreTaskPriority.NORMAL, UUID.randomUUID(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );
        final TapeTask t4 = new NoOpTapeTask( "blah", BlobStoreTaskPriority.HIGH, UUID.randomUUID(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );
        t4.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        final TapeTask t5 = new NoOpTapeTask( "blah", BlobStoreTaskPriority.HIGH, UUID.randomUUID(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy(
                TapeBlobStore.class, 
                MockInvocationHandler.forReturnType( Set.class, new InvocationHandler()
                {
                    public Object invoke(
                            final Object proxy, 
                            final Method method, 
                            final Object[] args )
                    {
                        return CollectionFactory.toSet( t1, t2, t3 );
                    }
                }, null ) );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                MockInvocationHandler.forReturnType( Set.class, new InvocationHandler()
                {
                    public Object invoke(
                            final Object proxy, 
                            final Method method, 
                            final Object[] args )
                    {
                        return CollectionFactory.toSet( t4 );
                    }
                }, null ) );
        final TargetBlobStore targetBlobStore = InterfaceProxyFactory.getProxy(
                TargetBlobStore.class, 
                MockInvocationHandler.forReturnType( Set.class, new InvocationHandler()
                {
                    public Object invoke(
                            final Object proxy, 
                            final Method method, 
                            final Object[] args )
                    {
                        return CollectionFactory.toSet( t5 );
                    }
                }, null ) );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore ).addTargetBlobStore( targetBlobStore );
        
        final List< BlobStoreTaskInformation > tasks =
                CollectionFactory.toList( resource.getBlobStoreTasks(
                        new BlobStoreTaskState [] { BlobStoreTaskState.READY } )
                        .getWithoutBlocking().getTasks() );
        assertEquals(3, tasks.size(), "Shoulda reported tasks in a ready state.");
        Object a2 = t5.getId();
        assertEquals(a2, tasks.get( 0 ).getId(), "Shoulda sorted tasks as expected.");
        Object a1 = t2.getId();
        assertEquals(a1, tasks.get( 1 ).getId(), "Shoulda sorted tasks as expected.");
        Object a = t3.getId();
        assertEquals(a, tasks.get( 2 ).getId(), "Shoulda sorted tasks as expected.");
    }
    
    
    @Test
    public void testDeleteObjectsEmptyObjectListDoesNothing()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        final DeleteObjectFailure[] failures = extractFailures( dataPlanner.deleteObjects( 
                null, PreviousVersions.DELETE_ALL_VERSIONS, (UUID[])Array.newInstance( UUID.class, 0 ) ) );

        assertEquals(0, failures.length, "Shoulda returned no failures.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda returned an empty set of objects.");
    }
    
    
    @Test
    public void testUndeleteWorksCorrectly()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createABMConfigSingleCopyOnTape();
        mockDaoDriver.updateBean(
        		dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ), DataPolicy.VERSIONING );
        final UUID bucketJasonId = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" ).getId();

        final S3Object object1 = mockDaoDriver.createObject( bucketJasonId, "obj" );
        final S3Object object2 = mockDaoDriver.createObject( bucketJasonId, "obj" );
        mockDaoDriver.updateBean( object2.setLatest( false ),S3Object.LATEST );

        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );

        assertEquals(false, mockDaoDriver.attain( S3Object.class, object1 ).isLatest(), "Should be no latest.");

        assertEquals(false, mockDaoDriver.attain( S3Object.class, object2 ).isLatest(), "Should be no latest.");

        TestUtil.assertThrows(
                "Should only allow undeleting newest version.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test()
                    {
                    	dataPlanner.undeleteObject( null, object1 );
                    }
                } );

        assertEquals(false, mockDaoDriver.attain( S3Object.class, object1 ).isLatest(), "Should be no latest.");

        assertEquals(false, mockDaoDriver.attain( S3Object.class, object2 ).isLatest(), "Should be no latest.");

        dataPlanner.undeleteObject( null, object2 );

        assertEquals(false, mockDaoDriver.attain( S3Object.class, object1 ).isLatest(), "Object 1 should still not be latest.");

        assertEquals(true, mockDaoDriver.attain( S3Object.class, object2 ).isLatest(), "Newest object should be latest again");

        TestUtil.assertThrows(
                "Should already be latest.",
                GenericFailure.CONFLICT,
                new BlastContainer()
                {
                    public void test()
                    {
                    	dataPlanner.undeleteObject( null, object2 );
                    }
                } );

        assertEquals(false, mockDaoDriver.attain( S3Object.class, object1 ).isLatest(), "Object 1 should still not be latest.");

        assertEquals(true, mockDaoDriver.attain( S3Object.class, object2 ).isLatest(), "Newest object should be latest again");
    }
    
    
    @Test
    public void testDeleteObjectsCanDeleteFolderWhenAllContainedObjectsAlsoSpecified()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/1" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        final DeleteObjectFailure[] failures = extractFailures(
                dataPlanner.deleteObjects( null, PreviousVersions.DELETE_ALL_VERSIONS, objectIds ) );

        assertEquals(0, failures.length, "Shoulda returned no failures.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda returned an empty set of objects.");
    }
    
    
    @Test
    public void testDeleteObjectsRollsBackToPreviousRevisionsWhenRollbackSpecified()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean(
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/1" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        final UUID earlierVersionObjectId = 
                mockDaoDriver.createObject( bucketId, "object/1", 20 ).getId();
        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        final DeleteObjectFailure[] failures = extractFailures(
                dataPlanner.deleteObjects( null, PreviousVersions.DELETE_SPECIFIC_VERSION, objectIds ) );

        assertEquals(0, failures.length, "Shoulda returned no failures.");
        final S3Object o = mockDaoDriver.attainOneAndOnly( S3Object.class );
        assertEquals(true, o.isLatest(), "Shoulda returned the rolled back to objects only.");
        assertEquals(earlierVersionObjectId, o.getId(), "Shoulda returned the rolled back to objects only.");
    }
    
    
    @Test
    public void testDeleteObjectsDeletesPreviousRevisionsWhenNoRollbackSpecified()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/1", 20, new Date( 2000 ) ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        mockDaoDriver.createObject( bucketId, "object/1", 20, new Date( 1000 ) ).getId();
        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        final DeleteObjectFailure[] failures = extractFailures(
                dataPlanner.deleteObjects( null, PreviousVersions.DELETE_ALL_VERSIONS, objectIds ) );

        assertEquals(0, failures.length, "Shoulda returned no failures.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda returned an empty set of objects.");
    }
    
    
    @Test
    public void testDeleteObjectsWhenFolderContainsObjectsSucceeds()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/1" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };

        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        final DeleteObjectFailure[] failures = extractFailures( dataPlanner.deleteObjects(
                null, PreviousVersions.DELETE_ALL_VERSIONS, new UUID [] { objectIds[ 0 ], objectIds[ 1 ] } ) );

        assertEquals(0, failures.length, "Shoulda returned exactly zero failures.");

        final List< S3Object > allObjects = dbSupport
                .getServiceManager()
                .getService( S3ObjectService.class )
                .retrieveAll()
                .toList();
        Collections.sort( allObjects, new BeanComparator<>( S3Object.class, S3Object.NAME ) );
        assertEquals(1, allObjects.size(), "Shoulda left exactly 1 object in the database.");
        assertEquals("object/2", allObjects.get( 0 ).getName(), "Shoulda left the unspecified 'data' object in the database.");
    }
    
    
    @Test
    public void testDeleteBucketThrowsExceptionWhenBucketNotEmptyAndDeleteObjectsNotSpecified()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final UUID bucketId1 = createDeleteBucketTestData( dbSupport, true );
        
        final DataPlannerResource dataPlanner = buildDataPlannerWithMockedDependencies( dbSupport );
        
        TestUtil.assertThrows(
                "Shoulda thrown a 409 bucket not empty failure exception.",
                AWSFailure.BUCKET_NOT_EMPTY,
                new BlastContainer()
                {
                    public void test()
                    {
                        dataPlanner.deleteBucket( null, bucketId1, false );
                    }
                } );
    }
    
    
    @Test
    public void testDeleteBucketSucceedsWhenMediaAllocatedToIt()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final UUID bucketId1 = createDeleteBucketTestData( dbSupport, false );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTape().setBucketId( bucketId1 ),
                PersistenceTarget.BUCKET_ID );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createPool().setBucketId( bucketId1 ),
                PersistenceTarget.BUCKET_ID );

        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        dataPlanner.deleteBucket( null, bucketId1, true );
        
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();

        Object a1 = CollectionFactory.toList( "bucket2" );
        assertEquals(a1, new ArrayList<Object>( BeanUtils.extractPropertyValues(
                        serviceManager.getService( BucketService.class ).retrieveAll().toSet(),
                        Bucket.NAME ) ), "Shoulda only had one bucket left.");
        Object a = CollectionFactory.toList( "object2" );
        assertEquals(a, new ArrayList<Object>( BeanUtils.extractPropertyValues(
                        serviceManager.getService( S3ObjectService.class ).retrieveAll().toSet(),
                        S3Object.NAME ) ), "Shoulda only had one object left.");
        assertNull(mockDaoDriver.attainOneAndOnly( Tape.class ).getBucketId(), "Shoulda retained tape and whacked the bucket id.");
        assertNull(mockDaoDriver.attainOneAndOnly( Pool.class ).getBucketId(), "Shoulda retained pool and whacked the bucket id.");
    }
    
    
    @Test
    public void testDeleteBucketSucceedsWhenDeleteObjectsNotSpecifiedButBucketIsEmpty()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final UUID bucketId1 = createDeleteBucketTestData( dbSupport, false );

        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        dataPlanner.deleteBucket( null, bucketId1, false );
        
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();

        Object a1 = CollectionFactory.toList( "bucket2" );
        assertEquals(a1, new ArrayList<Object>( BeanUtils.extractPropertyValues(
                        serviceManager.getService( BucketService.class ).retrieveAll().toSet(),
                        Bucket.NAME ) ), "Shoulda only had one bucket left.");

        Object a = CollectionFactory.toList( "object2" );
        assertEquals(a, new ArrayList<Object>( BeanUtils.extractPropertyValues(
                        serviceManager.getService( S3ObjectService.class ).retrieveAll().toSet(),
                        S3Object.NAME ) ), "Shoulda only had one object left.");
    }
    
    
    @Test
    public void testDeleteBucketDeletesObjectsWhenFlagSet()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final UUID bucketId1 = createDeleteBucketTestData( dbSupport, true );

        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        dataPlanner.deleteBucket( null, bucketId1, true );
        
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();

        Object a1 = CollectionFactory.toList( "bucket2" );
        assertEquals(a1, new ArrayList<Object>( BeanUtils.extractPropertyValues(
                        serviceManager.getService( BucketService.class ).retrieveAll().toSet(),
                        Bucket.NAME ) ), "Shoulda only had one bucket left.");

        Object a = CollectionFactory.toList( "object2" );
        assertEquals(a, new ArrayList<Object>( BeanUtils.extractPropertyValues(
                        serviceManager.getService( S3ObjectService.class ).retrieveAll().toSet(),
                        S3Object.NAME ) ), "Shoulda only had one object left.");
    }


    private static UUID createDeleteBucketTestData(
            final DatabaseSupport dbSupport,
            final boolean createObjectForFirstBucket )
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId1 = mockDaoDriver.createBucket( null, "bucket1" ).getId();
        final UUID bucketId2 = mockDaoDriver.createBucket( null, "bucket2" ).getId();
        if ( createObjectForFirstBucket )
        {
            mockDaoDriver.createObject( bucketId1, "object1", 100L );
        }
        mockDaoDriver.createObject( bucketId2, "object2", 200L );
        return bucketId1;
    }
    
    
    @Test
    public void testDeleteObjectsWithNonExistentObjectReturnsFailureObject()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/1" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        
        final UUID missingObjectId = UUID.fromString( "020f902e-bbaa-4c52-aa5e-0de26250eaac" );
        final DataPlannerResource dataPlanner = 
                buildDataPlannerWithMockedDependencies( dbSupport );
        
        final DeleteObjectFailure[] failures = extractFailures( dataPlanner.deleteObjects( 
                null, PreviousVersions.DELETE_ALL_VERSIONS, new UUID [] { missingObjectId, objectIds[ 2 ] } ) );

        assertEquals(1, failures.length, "Shoulda returned exactly one failure.");
        assertEquals(missingObjectId, failures[ 0 ].getObjectId(), "Shoulda returned an failure about the missing object.");
        assertEquals(DeleteObjectFailureReason.NOT_FOUND, failures[ 0 ].getReason(), "Shoulda returned a folder not empty failure reason.");

        final List< S3Object > allObjects = dbSupport
                .getServiceManager()
                .getService( S3ObjectService.class )
                .retrieveAll()
                .toList();
        Collections.sort( allObjects, new BeanComparator<>( S3Object.class, S3Object.NAME ) );
        assertEquals(2, allObjects.size(), "Shoulda left exactly 2 objects in the database.");
        assertEquals("object/", allObjects.get( 0 ).getName(), "Shoulda left the unspecified folder in the database.");
        assertEquals("object/1", allObjects.get( 1 ).getName(), "Shoulda left the unspecified 'data' object in the database.");
    }
    
    
    @Test
    public void testGetLogicalUsedCapacityNullRequestNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket b1 = mockDaoDriver.createBucket( null, "a" );
        final Bucket b2 = mockDaoDriver.createBucket( null, "b" );
        mockDaoDriver.createObject( b1.getId(), "o0", 10 );
        mockDaoDriver.createObject( b2.getId(), "o1", 100 );
        mockDaoDriver.createObject( b2.getId(), "o2", 1000 );
        
        final DataPlannerResource dataPlanner = buildDataPlannerWithMockedDependencies( dbSupport );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                dataPlanner.getLogicalUsedCapacity( null );
            }
        } );
    }
    
    
    @Test
    public void testGetLogicalUsedCapacityDoesSo()
    {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket b1 = mockDaoDriver.createBucket( null, "a" );
        final Bucket b2 = mockDaoDriver.createBucket( null, "b" );
        mockDaoDriver.createObject( b1.getId(), "o0", 10 );
        mockDaoDriver.createObject( b2.getId(), "o1", 100 );
        mockDaoDriver.createObject( b2.getId(), "o2", 1000 );

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final DataPlannerResource dataPlanner = buildDataPlannerWithMockedDependencies( dbSupport );
        
        int i = 1000;
        while ( --i > 0 && -1 == dataPlanner.getLogicalUsedCapacity( new UUID [] { null } )
                .getWithoutBlocking().getCapacities()[ 0 ] )
        {
            TestUtil.sleep( 10 );
        }
        
        final UUID [] request = new UUID [] { null, b1.getId(), b2.getId() };
        final LogicalUsedCapacityInformation result =
                dataPlanner.getLogicalUsedCapacity( request ).getWithoutBlocking();
        assertEquals(1110, result.getCapacities()[0], "Shoulda returned logical capacity summed across all buckets.");
        assertEquals(10, result.getCapacities()[1], "Shoulda returned logical capacity summed across all buckets.");
        assertEquals(1100, result.getCapacities()[2], "Shoulda returned logical capacity summed across all buckets.");
    }
    
    
    @Test
    public void testStartBlobWriteAllocatesCacheSpace()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        Object a = cacheManager.getDiskFileFor( b1.getId() ).getFilePath();
        assertEquals(a, resource.startBlobWrite( null, b1.getId() ).getWithoutBlocking(), "Shoulda returned path for blob write.");
        assertEquals(2, dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Should notta whacked job entries.");
        assertTrue(cacheManager.isCacheSpaceAllocated( b1.getId() ), "Shoulda allocated cache space for b1 only.");
        assertFalse(cacheManager.isCacheSpaceAllocated( b2.getId() ), "Shoulda allocated cache space for b1 only.");
    }
    
    
    @Test
    public void testStartBlobWriteWhenBlobAlreadyWrittenCompletelyNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                b1.setChecksum( "blah" ).setChecksumType( ChecksumType.values()[ 0 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.startBlobWrite( null, b1.getId() ).getWithoutBlocking();
            }
        } );
    }
    
    
    @Test
    public void
    testBlobWriteCompletedEnforcesConstantMetadataAcrossAllBlobsWhenNoMetadataDisabledPerBlkp3054()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 3, 10 );
        final Blob b1 = blobs.get( 0 );
        final Blob b2 = blobs.get( 1 );
        final Blob b3 = blobs.get( 2 );
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2, b3 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        cacheManager.allocateChunksForBlob( b1.getId() );
        cacheManager.allocateChunksForBlob( b2.getId() );
        cacheManager.allocateChunksForBlob( b3.getId() );
        resource.blobWriteCompleted( 
                entries.iterator().next().getJobId(), b1.getId(), ChecksumType.CRC_32, "mycrc", null, null );

        resource.blobWriteCompleted( 
                entries.iterator().next().getJobId(),
                b2.getId(), 
                ChecksumType.CRC_32, 
                "mycrc", 
                null,
                CollectionFactory.toArray( 
                        S3ObjectProperty.class,
                        CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                                .setObjectId( o1.getId() ).setKey( "key" ).setValue( "value" ) ) ) );
        resource.blobWriteCompleted( 
                entries.iterator().next().getJobId(),
                b3.getId(), 
                ChecksumType.CRC_32, 
                "mycrc", 
                null,
                (S3ObjectProperty[])Array.newInstance( S3ObjectProperty.class, 0 ) );
    }


    @Test
    public void testBlobWriteCompletedAllowsLateMetadataWhenOnlyEtagExists()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 3, 10 );
        final Blob b1 = blobs.get( 0 );
        final Blob b2 = blobs.get( 1 );
        final Blob b3 = blobs.get( 2 );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2, b3 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        cacheManager.allocateChunksForBlob( b1.getId() );
        cacheManager.allocateChunksForBlob( b2.getId() );
        cacheManager.allocateChunksForBlob( b3.getId() );

        // First blob: Only ETag metadata
        resource.blobWriteCompleted(
                chunks.iterator().next().getJobId(),
                b1.getId(),
                ChecksumType.CRC_32,
                "mycrc",
                12345L,
                CollectionFactory.toArray(
                        S3ObjectProperty.class,
                        CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                                .setObjectId( o1.getId() )
                                .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                                .setValue( "etag-value-1" ) ) ) );

        // Second blob: Additional metadata should be allowed since we only had ETag before
        resource.blobWriteCompleted(
                chunks.iterator().next().getJobId(),
                b2.getId(),
                ChecksumType.CRC_32,
                "mycrc",
                12345L,
                CollectionFactory.toArray(
                        S3ObjectProperty.class,
                        CollectionFactory.toSet(
                                BeanFactory.newBean( S3ObjectProperty.class )
                                        .setObjectId( o1.getId() )
                                        .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                                        .setValue( "etag-value-1" ) ) ) );

        // Third blob: Same metadata should work fine
        resource.blobWriteCompleted(
                chunks.iterator().next().getJobId(),
                b3.getId(),
                ChecksumType.CRC_32,
                "mycrc",
                12345L,
                CollectionFactory.toArray(
                        S3ObjectProperty.class,
                        CollectionFactory.toSet(
                                BeanFactory.newBean( S3ObjectProperty.class )
                                        .setObjectId( o1.getId() )
                                        .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                                        .setValue( "etag-value-1" ),
                                BeanFactory.newBean( S3ObjectProperty.class )
                                        .setObjectId( o1.getId() )
                                        .setKey( "Content-Type" )
                                        .setValue( "text/plain" ) ) ) );
    }


    @Test
    public void testBlobWriteCompletedRejectsConflictingEtagWhenOnlyEtagExists()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 2, 10 );
        final Blob b1 = blobs.get( 0 );
        final Blob b2 = blobs.get( 1 );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        cacheManager.allocateChunksForBlob( b1.getId() );
        cacheManager.allocateChunksForBlob( b2.getId() );

        // First blob: Only ETag metadata
        resource.blobWriteCompleted(
                chunks.iterator().next().getJobId(),
                b1.getId(),
                ChecksumType.CRC_32,
                "mycrc",
                12345L,
                CollectionFactory.toArray(
                        S3ObjectProperty.class,
                        CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                                .setObjectId( o1.getId() )
                                .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                                .setValue( "etag-value-1" ) ) ) );

        // Second blob: Different ETag should cause conflict
        TestUtil.assertThrows( "Expected DataPlannerException for conflicting ETag", GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.blobWriteCompleted(
                        chunks.iterator().next().getJobId(),
                        b2.getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        12345L,
                        CollectionFactory.toArray(
                                S3ObjectProperty.class,
                                CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                                        .setObjectId( o1.getId() )
                                        .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                                        .setValue( "etag-value-different" ) ) ) );
            }
        } );
    }


    @Test
    public void testBlobWriteCompletedEnforcesCompatibleMetadataAcrossAllBlobsWhenMetadata()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 3, 10 );
        final Blob b1 = blobs.get( 0 );
        final Blob b2 = blobs.get( 1 );
        final Blob b3 = blobs.get( 2 );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2, b3 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        cacheManager.allocateChunksForBlob( b1.getId() );
        cacheManager.allocateChunksForBlob( b2.getId() );
        cacheManager.allocateChunksForBlob( b3.getId() );
        resource.blobWriteCompleted(
                chunks.iterator().next().getJobId(),
                b1.getId(),
                ChecksumType.CRC_32,
                "mycrc",
                null,
                CollectionFactory.toArray(
                        S3ObjectProperty.class,
                        CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                                .setObjectId( o1.getId() ).setKey( "key" ).setValue( "value" ) ) ) );

        // Test that conflicting values for the same key throw an exception
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.blobWriteCompleted(
                        chunks.iterator().next().getJobId(),
                        b2.getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        null,
                        CollectionFactory.toArray(
                                S3ObjectProperty.class,
                                CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                                        .setObjectId( o1.getId() ).setKey( "key" ).setValue( "different_value" ) ) ) );
            }
        } );

        // Test that matching values for the same key are allowed (overlapping metadata)
        resource.blobWriteCompleted(
                chunks.iterator().next().getJobId(),
                b2.getId(),
                ChecksumType.CRC_32,
                "mycrc",
                null,
                CollectionFactory.toArray(
                        S3ObjectProperty.class,
                        CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                                .setObjectId( o1.getId() ).setKey( "key" ).setValue( "value" ) ) ) );

        // Test that new metadata keys are allowed (additive metadata)
        resource.blobWriteCompleted(
                chunks.iterator().next().getJobId(),
                b3.getId(),
                ChecksumType.CRC_32,
                "mycrc",
                null,
                CollectionFactory.toArray(
                        S3ObjectProperty.class,
                        CollectionFactory.toSet(
                                BeanFactory.newBean( S3ObjectProperty.class )
                                        .setObjectId( o1.getId() ).setKey( "key" ).setValue( "value" ),
                                BeanFactory.newBean( S3ObjectProperty.class )
                                        .setObjectId( o1.getId() ).setKey( "key2" ).setValue( "value2" ) ) ) );
    }
    
    
    @Test
    public void testBlobWriteCompletedEnforcesNotNullMetadataWhenMetadataBLKP3471( )
    {
        
        dbSupport.getServiceManager( ).getService( BucketService.class ).initializeLogicalSizeCache( );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId( ), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId( ) );
        final JobEntry entry = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager( ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore =
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor = InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource =
                newDataPlannerResourceImpl( deadJobMonitor, rpcServer, dbSupport.getServiceManager( ), cacheManager,
                        newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                        new JobProgressManagerImpl( dbSupport.getServiceManager( ), BufferProgressUpdates.NO ),
                        tapeBlobStore, poolBlobStore );
        
        cacheManager.allocateChunksForBlob( b1.getId( ) );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, ( ) -> resource
                .blobWriteCompleted( entry.getJobId(), b1.getId( ), ChecksumType.CRC_32, "mycrc", null,
                        CollectionFactory.toArray( S3ObjectProperty.class, CollectionFactory
                                .toSet( BeanFactory.newBean( S3ObjectProperty.class ).setObjectId( o1.getId( ) )
                                        .setKey( "keyWithNullValue" ).setValue( null ) ) ) ) );
    }
    
    
    @Test
    public void testBlobWriteCompletedMarksBlobAsUploadedInCacheWhenDefaultObjectCreationDate()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        cacheManager.allocateChunksForBlob( b1.getId() );
        final long start = System.currentTimeMillis();
        assertEquals(Boolean.FALSE, resource.blobWriteCompleted(
                        chunks.iterator().next().getJobId(), b1.getId(), ChecksumType.CRC_32, "mycrc", null, null )
                            .getWithoutBlocking(), "Shoulda reported complete.");
        final long end = System.currentTimeMillis();

        assertNotNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksum(), "Shoulda marked blob as uploaded.");
        assertNotNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksumType(), "Shoulda marked blob as uploaded.");
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksum(), "Should notta marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksumType(), "Should notta marked blob as uploaded.");
        assertFalse(cacheManager.isOnDisk( b2.getId() ), "Should notta marked blob as uploaded.");

        final long objectCreationDate = 
                mockDaoDriver.attain( S3Object.class, o1 ).getCreationDate().getTime();
        assertTrue(( start <= objectCreationDate ) && ( end >= objectCreationDate ), "Object creation date shoulda defaulted to current time.");
    }
    
    
    @Test
    public void testBlobWriteCompletedWhenPreviouslyConfiguredObjectCreationDateNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        cacheManager.allocateChunksForBlob( b1.getId() );
        final long start = System.currentTimeMillis();
        mockDaoDriver.updateBean( o1.setCreationDate( new Date() ), S3Object.CREATION_DATE );
        final long end = System.currentTimeMillis();
        TestUtil.sleep( 2 );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                resource.blobWriteCompleted( 
                        chunks.iterator().next().getJobId(), b1.getId(), ChecksumType.CRC_32, "mycrc", null, null );
            }
        } );

        assertNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksum(), "Should notta marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksumType(), "Should notta marked blob as uploaded.");

        final long objectCreationDate = 
                mockDaoDriver.attain( S3Object.class, o1 ).getCreationDate().getTime();
        assertTrue(( start <= objectCreationDate ) && ( end >= objectCreationDate ), "Object creation date shoulda retained current time.");
    }
    
    
    @Test
    public void testBlobWriteCompletedWithCustomCreationDateNoCustomETagNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final Long customCreationDate = Long.valueOf( 987654 );
        cacheManager.allocateChunksForBlob( b1.getId() );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                resource.blobWriteCompleted( 
                        chunks.iterator().next().getJobId(),
                        b1.getId(),
                        ChecksumType.CRC_32, 
                        "mycrc", 
                        customCreationDate, 
                        null );
            }
        } );

        assertNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksum(), "Should notta marked blob as uploaded.");
    }
    
    
    @Test
    public void testBlobWriteCompletedWithNoCustomCreationDateCustomETagNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        cacheManager.allocateChunksForBlob( b1.getId() );
        final S3ObjectProperty metadata = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                .setValue( "value" )
                .setObjectId( o1.getId() );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                resource.blobWriteCompleted(
                        chunks.iterator().next().getJobId(),
                        b1.getId(),
                        ChecksumType.CRC_32, 
                        "mycrc", 
                        null, 
                        new S3ObjectProperty [] { metadata } );
            }
        } );

        assertNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksum(), "Should notta marked blob as uploaded.");
    }
    
    
    @Test
    public void testBlobWriteCompletedMarksBlobAsUploadedInCacheWhenCustomObjectCreationDate()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final Long customCreationDate = Long.valueOf( 987654 );
        final S3ObjectProperty metadata = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                .setValue( "value" )
                .setObjectId( o1.getId() );
        cacheManager.allocateChunksForBlob( b1.getId() );
        assertEquals(Boolean.TRUE, resource.blobWriteCompleted(
                        chunks.iterator().next().getJobId(),
                        b1.getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        customCreationDate,
                        new S3ObjectProperty[] { metadata } ).getWithoutBlocking(), "Shoulda reported complete.");

        assertNotNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksum(), "Shoulda marked blob as uploaded.");
        assertNotNull(mockDaoDriver.attain( Blob.class, b1 ).getChecksumType(), "Shoulda marked blob as uploaded.");
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksum(), "Should notta marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksumType(), "Should notta marked blob as uploaded.");
        assertFalse(cacheManager.isOnDisk( b2.getId() ), "Should notta marked blob as uploaded.");

        final long objectCreationDate = 
                mockDaoDriver.attain( S3Object.class, o1 ).getCreationDate().getTime();
        assertEquals(customCreationDate.longValue(), objectCreationDate, "Object creation date shoulda been custom creation date.");
    }
    
    
    @Test
    public void testBlobWriteCompletedDoesNotPermitChangingCreationDates()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 3, 10 );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries(
                JobRequestType.PUT, blobs.get( 0 ), blobs.get( 1 ), blobs.get( 2 ), b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final Long customCreationDate = Long.valueOf( 987654 );
        final S3ObjectProperty metadata = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                .setValue( "value" )
                .setObjectId( o1.getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 0 ).getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 1 ).getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 2 ).getId() );
        assertNull(resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 0 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        null,
                        null ), "Shoulda reported not complete yet.");
        assertNull(resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 1 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        customCreationDate,
                        new S3ObjectProperty[] { metadata } ), "Shoulda reported not complete yet.");
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                resource.blobWriteCompleted( 
                        entries.iterator().next().getJobId(),
                        blobs.get( 2 ).getId(),
                        ChecksumType.CRC_32, 
                        "mycrc", 
                        Long.valueOf( 3333 ), 
                        null );
            }
        } );
        assertEquals(Boolean.TRUE, resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 2 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        customCreationDate,
                        null ).getWithoutBlocking(), "Shoulda reported complete.");

        for ( final Blob blob : blobs )
        {
            assertNotNull(mockDaoDriver.attain( Blob.class, blob).getChecksum(), "Shoulda marked blob as uploaded.");
            assertNotNull(mockDaoDriver.attain( Blob.class, blob ).getChecksumType(), "Shoulda marked blob as uploaded.");
            assertTrue(cacheManager.isOnDisk( blob.getId() ), "Shoulda marked blob as uploaded.");
        }
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksum(), "Should notta marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksumType(), "Should notta marked blob as uploaded.");
        assertFalse(cacheManager.isOnDisk( b2.getId() ), "Should notta marked blob as uploaded.");

        final long objectCreationDate = 
                mockDaoDriver.attain( S3Object.class, o1 ).getCreationDate().getTime();
        assertEquals(customCreationDate.longValue(), objectCreationDate, "Object creation date shoulda been custom creation date.");
    }
    
    
    @Test
    public void testBlobWriteCompletedMarksBlobAsUploadedInCacheWhenCustomObjectCreationDateAndMultiBlobs()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 3, 10 );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries(
                JobRequestType.PUT, blobs.get( 0 ), blobs.get( 1 ), blobs.get( 2 ), b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final Long customCreationDate = Long.valueOf( 987654 );
        final S3ObjectProperty metadata = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                .setValue( "value" )
                .setObjectId( o1.getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 0 ).getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 1 ).getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 2 ).getId() );
        assertNull(resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 0 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        null,
                        null ), "Shoulda reported not complete yet.");
        assertNull(resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 1 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        customCreationDate,
                        new S3ObjectProperty[] { metadata } ), "Shoulda reported not complete yet.");
        assertEquals(Boolean.TRUE, resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 2 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        null,
                        null ).getWithoutBlocking(), "Shoulda reported complete.");

        for ( final Blob blob : blobs )
        {
            assertNotNull(mockDaoDriver.attain( Blob.class, blob).getChecksum(), "Shoulda marked blob as uploaded.");
            assertNotNull(mockDaoDriver.attain( Blob.class, blob ).getChecksumType(), "Shoulda marked blob as uploaded.");
            assertTrue(cacheManager.isOnDisk( blob.getId() ), "Shoulda marked blob as uploaded.");
        }
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksum(), "Should notta marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksumType(), "Should notta marked blob as uploaded.");
        assertFalse(cacheManager.isOnDisk( b2.getId() ), "Should notta marked blob as uploaded.");

        final long objectCreationDate = 
                mockDaoDriver.attain( S3Object.class, o1 ).getCreationDate().getTime();
        assertEquals(customCreationDate.longValue(), objectCreationDate, "Object creation date shoulda been custom creation date.");
    }
    
    
    @Test
    public void
    testBlobWriteCompletedMarksBlobAsUploadedInCacheWhenCustomObjectCreationDateAndMultiBlobsAndOtherMeta()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 3, 10 );
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries(
                JobRequestType.PUT, blobs.get( 0 ), blobs.get( 1 ), blobs.get( 2 ), b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        final List< S3ObjectProperty > customMeta = CollectionFactory.toList( 
                BeanFactory.newBean( S3ObjectProperty.class )
                    .setKey( "k1" ).setValue( "v1" ).setObjectId( o1.getId() ),
                BeanFactory.newBean( S3ObjectProperty.class )
                    .setKey( "k2" ).setValue( "v2" ).setObjectId( o1.getId() ) );
        final S3ObjectProperty etagMeta = BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                .setValue( "value" )
                .setObjectId( o1.getId() );
        final List< S3ObjectProperty > allMeta = new ArrayList<>( customMeta );
        allMeta.add( etagMeta );
        
        final Long customCreationDate = Long.valueOf( 987654 );
        cacheManager.allocateChunksForBlob( blobs.get( 0 ).getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 1 ).getId() );
        cacheManager.allocateChunksForBlob( blobs.get( 2 ).getId() );
        assertNull(resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 0 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        null,
                        CollectionFactory.toArray( S3ObjectProperty.class, customMeta ) ), "Shoulda reported not complete yet.");
        assertNull(resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 1 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        customCreationDate,
                        CollectionFactory.toArray( S3ObjectProperty.class, allMeta ) ), "Shoulda reported not complete yet.");
        assertEquals(Boolean.TRUE, resource.blobWriteCompleted(
                        entries.iterator().next().getJobId(),
                        blobs.get( 2 ).getId(),
                        ChecksumType.CRC_32,
                        "mycrc",
                        null,
                        CollectionFactory.toArray( S3ObjectProperty.class, customMeta ) )
                            .getWithoutBlocking(), "Shoulda reported complete.");

        for ( final Blob blob : blobs )
        {
            assertNotNull(mockDaoDriver.attain( Blob.class, blob).getChecksum(), "Shoulda marked blob as uploaded.");
            assertNotNull(mockDaoDriver.attain( Blob.class, blob ).getChecksumType(), "Shoulda marked blob as uploaded.");
            assertTrue(cacheManager.isOnDisk( blob.getId() ), "Shoulda marked blob as uploaded.");
        }
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksum(), "Should notta marked blob as uploaded.");
        assertNull(mockDaoDriver.attain( Blob.class, b2 ).getChecksumType(), "Should notta marked blob as uploaded.");
        assertFalse(cacheManager.isOnDisk( b2.getId() ), "Should notta marked blob as uploaded.");

        final long objectCreationDate = 
                mockDaoDriver.attain( S3Object.class, o1 ).getCreationDate().getTime();
        assertEquals(customCreationDate.longValue(), objectCreationDate, "Object creation date shoulda been custom creation date.");
    }
    
    
    @Test
    public void testBlobWriteCompletedWhenBlobAlreadyUploadedNotAllowed()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( 
                b1.setChecksum( "blah" ).setChecksumType( ChecksumType.values()[ 0 ] ),
                ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                    {
                        return "/some/file.txt";
                    }
                } );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        cacheManager.allocateChunksForBlob( b1.getId() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                resource.blobWriteCompleted( 
                        chunks.iterator().next().getJobId(), b1.getId(), ChecksumType.CRC_32, "mycrc", null, null );
            }
        } );
    }
    
    
    @Test
    public void testCleanUpCompletedJobsAndJobChunksDoesSo()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class, null );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final DataPlannerResource resource = newDataPlannerResourceImpl( 
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore, 
                poolBlobStore );
        
        resource.cleanUpCompletedJobsAndJobChunks();
        assertEquals(null, mockDaoDriver.retrieve( job ), "Shoulda cleaned up completed job.");
    }
    
    
    @Test
    public void testQuiesceAndPrepareForShutdownWhenNotForcedDoesShutDownReturns()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy(TapeBlobStore.class, null);
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(PoolBlobStore.class, null);
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );
        
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        
        final Duration duration = new Duration();
        resource.quiesceAndPrepareForShutdown( false );
        assertEquals(0, duration.getElapsedSeconds(), "Shoulda shut down immediately.");

        resource.getCacheState( false );
        resource.getCacheState( true );
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, new BlastContainer()
        {
            @Override
            public void test()
            {
                resource.createPutJob( 
                        BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( 
                                new S3ObjectToCreate [] { data1, data2 } ) ).getWithoutBlocking();
            }
        } );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry entry = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final DetailedJobToReplicate jobToReplicate = new JobReplicationSupport(
                dbSupport.getServiceManager(), entry.getJobId()).getJobToReplicate();
        
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, new BlastContainer()
        {
            @Override
            public void test()
            {
                resource.replicatePutJob( jobToReplicate );
            }
        } );
    }
    
    
    @Test
    public void testQuiesceAndPrepareForShutdownWhenNotForcedDoesNotShutDownThrows()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.IMPORT_IN_PROGRESS );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy(TapeBlobStore.class, null);
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(PoolBlobStore.class, null);
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( 
                        new S3ObjectToCreate [] { data1, data2 } ) ).getWithoutBlocking();
        
        TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED, new BlastContainer()
        {
            @Override
            public void test()
            {
                resource.quiesceAndPrepareForShutdown( false );
            }
        } );
    }
    
    
    @Test
    public void testQuiesceAndPrepareForShutdownWhenForcedDoesShutDownReturns()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy(TapeBlobStore.class, null);
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(PoolBlobStore.class, null);
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3ObjectToCreate data1 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data1" ).setSizeInBytes( 12 );
        final S3ObjectToCreate data2 = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data2" ).setSizeInBytes( 12 );
        resource.createPutJob( 
                BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( 
                        new S3ObjectToCreate [] { data1, data2 } ) ).getWithoutBlocking();

        TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED, new BlastContainer()
        {
            @Override
            public void test()
            {
                resource.quiesceAndPrepareForShutdown( false );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED, new BlastContainer()
        {
            @Override
            public void test()
            {
                resource.quiesceAndPrepareForShutdown( false );
            }
        } );
        
        resource.quiesceAndPrepareForShutdown( true );
    }
    
    
    @Test
    public void testValidateFeatureKeysNowDoesSo()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createFeatureKey( null, null, new Date() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final DeadJobMonitor deadJobMonitor =
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy(TapeBlobStore.class, null);
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(PoolBlobStore.class, null);
        final DataPlannerResource resource = newDataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer,
                dbSupport.getServiceManager(),
                cacheManager,
                newJobCreatorImpl( cacheManager, dbSupport, getBlobStoresByPersistenceType(tapeBlobStore, poolBlobStore) ),
                new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                tapeBlobStore,
                poolBlobStore );

        TestUtil.sleep( 2 );
        resource.validateFeatureKeysNow();

        assertNotNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Shoulda expired key.");
    }


    private DeleteObjectFailure[] extractFailures( final RpcFuture< DeleteObjectsResult > resultFuture )
    {
        assertNotNull(resultFuture, "Shoulda not returned a null result future.");

        final DeleteObjectsResult deleteObjectsResult = resultFuture.get( Timeout.DEFAULT );
        assertNotNull(deleteObjectsResult, "Shoulda not returned a null delete objects result.");

        final DeleteObjectFailure[] failures = deleteObjectsResult.getFailures();
        assertNotNull(failures, "Shoulda not returned a null failure array.");

        return failures;
    }


    private DataPlannerResource buildDataPlannerWithMockedDependencies(
            final DatabaseSupport dbSupport )
    {
        return buildDataPlannerFromBlobStore(
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ) );
    }


    private DataPlannerResource buildDataPlannerFromBlobStore(
            final BeansServiceManager serviceManager,
            final TapeBlobStore blobStore )
    {
        return newDataPlannerResourceImpl( 
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, null ),
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                serviceManager,
                InterfaceProxyFactory.getProxy( DiskManager.class, null ),
                new MockJobCreator(),
                new JobProgressManagerImpl( serviceManager, BufferProgressUpdates.NO ),
                blobStore,
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ) );
    }
    
    
    private DataPlannerResourceImpl newDataPlannerResourceImpl(
            final DeadJobMonitor deadJobMonitor,
            final RpcServer rpcServer, 
            final BeansServiceManager serviceManager,
            final DiskManager cacheManager,
            final JobCreator jobCreator,
            final JobProgressManager jobProgressManager,
            final TapeBlobStore tapeBlobStore,
            final PoolBlobStore poolBlobStore )
    {
        return new DataPlannerResourceImpl(
                deadJobMonitor,
                rpcServer, 
                serviceManager,
                cacheManager,
                jobCreator,
                jobProgressManager,
                tapeBlobStore,
                poolBlobStore,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ) );
    }
    
    
    private JobCreator newJobCreatorImpl( final DiskManager cacheManager,
                                          final DatabaseSupport dbSupport,
                                          final Map<PersistenceType, BlobStore> blobStoresByPersistenceType )
    {
        return newJobCreatorImpl( 
                cacheManager, 
                dbSupport.getServiceManager(), 
                new MockDs3ConnectionFactory(),
                1, 
                Long.valueOf( 1 ),
                blobStoresByPersistenceType);
    }
    
    
    private JobCreator newJobCreatorImpl(
            final DiskManager cacheManager, 
            final DatabaseSupport dbSupport,
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final Map<PersistenceType, BlobStore> blobStoresByPersistenceType)
    {
        return newJobCreatorImpl( 
                cacheManager, 
                dbSupport.getServiceManager(),
                ds3ConnectionFactory,
                1,
                Long.valueOf( 1 ),
                blobStoresByPersistenceType);
    }
    
    
    private JobCreator newJobCreatorImpl(
            final DiskManager cacheManager,
            final BeansServiceManager serviceManager,
            final long preferredBlobSizeInMb,
            final long staticPreferredChunkSizeInMb,
            final Map<PersistenceType, BlobStore> blobStoresByPersistenceType)
    {
        return newJobCreatorImpl(
                cacheManager,
                serviceManager, 
                new MockDs3ConnectionFactory(),
                preferredBlobSizeInMb,
                Long.valueOf( staticPreferredChunkSizeInMb ),
                blobStoresByPersistenceType);
    }
    
    
    private JobCreator newJobCreatorImpl(
            final DiskManager cacheManager,
            final BeansServiceManager serviceManager,
            final long preferredBlobSizeInMb,
            final Map<PersistenceType, BlobStore> blobStoresByPersistenceType)
    {
        return newJobCreatorImpl(
                cacheManager,
                serviceManager, 
                new MockDs3ConnectionFactory(),
                preferredBlobSizeInMb, 
                null,
                blobStoresByPersistenceType);
    }
    
    
    private JobCreator newJobCreatorImpl(
            final DiskManager cacheManager,
            final BeansServiceManager serviceManager,
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final long preferredBlobSizeInMb,
            final Long staticPreferredChunkSizeInMb,
            final Map<PersistenceType, BlobStore> blobStoresByPersistenceType)
    {
        return new JobCreatorImpl( 
                cacheManager, 
                serviceManager,
                ds3ConnectionFactory,
                new JobProgressManagerImpl( serviceManager, BufferProgressUpdates.NO ),
                blobStoresByPersistenceType,
                preferredBlobSizeInMb, 
                staticPreferredChunkSizeInMb );
    }

    private Map<PersistenceType, BlobStore> getBlobStoresByPersistenceType(final TapeBlobStore tapeBlobStore, final PoolBlobStore poolBlobStore) {
        return Map.of(
                PersistenceType.TAPE, tapeBlobStore,
                PersistenceType.POOL, poolBlobStore,
                PersistenceType.AZURE, InterfaceProxyFactory.getProxy(BlobStore.class, null),
                PersistenceType.S3, InterfaceProxyFactory.getProxy(BlobStore.class, null),
                PersistenceType.DS3, InterfaceProxyFactory.getProxy(BlobStore.class, null));
    }

    private static WhereClause s_notInCache = Require.any(Require.beanPropertyNull(DetailedJobEntry.CACHE_STATE), Require.beanPropertyEquals(DetailedJobEntry.CACHE_STATE, CacheEntryState.ALLOCATED));
    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeEach
    public void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }
   @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
