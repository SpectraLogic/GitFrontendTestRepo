/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.driver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobChunkToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.task.MockDs3ConnectionFactory;
import com.spectralogic.s3.dataplanner.frontend.JobCreatorImpl;
import com.spectralogic.s3.dataplanner.frontend.MockJobCreator;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public final class BlobStoreDriverImpl_Test
{
    @Test
    public void testConstructorNullTapeBlobStoreNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        null,
                        poolBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorNullPoolBlobStoreNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        null,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorNullTargetBlobStoreNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        null,
                        null,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        null,
                        targetBlobStore,
                        null,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        null,
                        targetBlobStore,
                        targetBlobStore,
                        null,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        poolBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        null,
                        cacheManager,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorNullCacheManagerNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        poolBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        null,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorNullJobCreatorNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        poolBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        null,
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorNullDs3ConnectionFactoryNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        poolBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        null,
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorNullJobProgressManagerNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        poolBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        new MockJobCreator(),
                        null,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        15 );
            }
        } );
    }


    @Test
    public void testConstructorZeroIntervalNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, null );
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BlobStoreDriverImpl(
                        tapeBlobStore,
                        poolBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        targetBlobStore,
                        dbSupport.getServiceManager(),
                        cacheManager,
                        new MockJobCreator(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        0 );
            }
        } );
    }


    @Test
    public void testAggregatingPutJobClosedOutProperlyForProcessingWhenMinimizeSpanningAcrossMedia()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean(
                job.setMinimizeSpanningAcrossMedia( true ), Job.MINIMIZE_SPANNING_ACROSS_MEDIA );
        mockDaoDriver.updateBean(
                job.setCreatedAt( new Date( 1000 ) ).setAggregating( true ),
                JobObservable.CREATED_AT, Job.AGGREGATING );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobEntry( job.getId(),  b1 );
        mockDaoDriver.createJobEntry( job.getId(),  b2 );

        final BlobStoreDriverImpl driver = newBlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        null ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                3600000 );
        driver.cleanUpAndProcessChunks();
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda reshaped job and marked it as unshapable.");
        assertFalse(
                dbSupport.getServiceManager().getRetriever( Job.class ).attain(
                        Require.nothing() ).isAggregating(),
                "Shoulda reshaped job and marked it as unshapable.");
        driver.shutdown();

    }


    @Test
    public void testAggregatingPutJobClosedOutProperlyForProcessing()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean(
                job.setCreatedAt( new Date( 1000 ) ).setAggregating( true ),
                JobObservable.CREATED_AT, Job.AGGREGATING );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createJobEntry( job.getId(), b2 );

        final BlobStoreDriverImpl driver = newBlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        null ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                3600000 );
        driver.cleanUpAndProcessChunks();

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda reshaped job and marked it as unshapable.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Job.class ).attain(
                        Require.nothing() ).isAggregating(), "Shoulda reshaped job and marked it as unshapable.");
        driver.shutdown();
    }


    @Test
    public void testReadEntriesWithNoSourceAreRechunked() {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Tape tape = mockDaoDriver.createTape(TapeState.NORMAL);
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.putBlobOnTape(tape.getId(), b1.getId());
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry jobEntry = mockDaoDriver.createJobEntry( job.getId(), b1 );

        //clear the read source id
        mockDaoDriver.updateBean( jobEntry.setReadFromTapeId( null ).setBlobStoreState(JobChunkBlobStoreState.IN_PROGRESS),
                JobEntry.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE);
        //run the driver
        final BlobStoreDriverImpl driver = newBlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        null ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                3600000 );
        driver.cleanUpAndProcessChunks();
        //make sure we re-set the read source ID and chunk number
        final JobEntry updatedEntry = dbSupport.getServiceManager().getRetriever( JobEntry.class ).retrieve(
                Require.beanPropertyEquals( JobEntry.ID, jobEntry.getId() ) );
        assertNotNull( updatedEntry.getReadFromTapeId(), "Shoulda re-set read source ID." );
        //make sure state is back to pending
        assertEquals( JobChunkBlobStoreState.PENDING, updatedEntry.getBlobStoreState(),
                "Shoulda re-set state to pending." );

        //clear read source again
        mockDaoDriver.updateBean( jobEntry.setReadFromTapeId( null ).setBlobStoreState(JobChunkBlobStoreState.IN_PROGRESS),
                JobEntry.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE);

        //set tape offline and make sure re truncate upon rechunking
        mockDaoDriver.updateBean( tape.setState( TapeState.OFFLINE ), Tape.STATE );
        driver.cleanUpAndProcessChunks();
        assertNull( dbSupport.getServiceManager().getRetriever( JobEntry.class ).retrieve(
                Require.beanPropertyEquals( JobEntry.ID, jobEntry.getId() ) ),
                "Shoulda removed entry from job since it is not anywhere currently" );
        driver.shutdown();
    }


    @Test
    public void testAggregatingPutJobDoesntRechunkAllocatedBlobsNotInCache()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null);
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(),
                tapePartition.getId(),
                TapeType.LTO6,
                WritePreferenceLevel.NORMAL );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final User user = mockDaoDriver.createUser( "myUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dataPolicy.getId(), "myBucket" );
        final Job job = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT );
        mockDaoDriver.updateBean(
                job.setCreatedAt( new Date( 1000 ) ).setAggregating( true ),
                JobObservable.CREATED_AT, Job.AGGREGATING );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 1024 * 1024 );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createJobEntry( job.getId(), b2 );
        mockDaoDriver.createJobEntry( job.getId(), b3 );

        final MockDiskManager mockCacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        mockCacheManager.allocateChunksForBlob( b1.getId() );
        mockCacheManager.allocateChunksForBlob( b2.getId() );
        mockCacheManager.blobLoadedToCache( b2.getId() );

        final BlobStoreDriverImpl driver = newBlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                dbSupport.getServiceManager(),
                mockCacheManager,
                new JobCreatorImpl(
                        mockCacheManager,
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        null ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                3600000 );

        driver.cleanUpAndProcessChunks();

        JobEntry entry = dbSupport.getServiceManager().getRetriever( JobEntry.class ).retrieve(
                Require.beanPropertyEquals( BlobObservable.BLOB_ID, b1.getId() ) ) ;
        assertNotNull(entry, "Shoulda retained entry for allocated but not uploaded blob.");

        entry = dbSupport.getServiceManager().getRetriever( JobEntry.class ).retrieve(
                 Require.beanPropertyEquals( BlobObservable.BLOB_ID, b2.getId() ) ) ;
        assertNotNull(entry, "Shoulda retained entry for fully uploaded blob not yet persisted.");

        entry = dbSupport.getServiceManager().getRetriever( JobEntry.class ).retrieve(
                Require.beanPropertyEquals( BlobObservable.BLOB_ID, b3.getId() ) ) ;
        assertNotNull(entry, "Shoulda retained entry for unallocated blob.");
        driver.shutdown();
    }


    @Test
    public void testAggregatingPutJobClosedOutProperlyForProcessingWhenReplicatingToDs3Target()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean(
                job.setCreatedAt( new Date( 1000 ) ).setAggregating( true ),
                JobObservable.CREATED_AT, Job.AGGREGATING );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1024 * 1024 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createJobEntry( job.getId(), b2 );

        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(),
                DataReplicationRuleType.PERMANENT,
                target.getId() );

        final BlobStoreDriverImpl driver = newBlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        null ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                3600000 );
        driver.cleanUpAndProcessChunks();

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda reshaped job and marked it as unshapable.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Job.class ).attain(
                        Require.nothing() ).isAggregating(), "Shoulda reshaped job and marked it as unshapable.");
        driver.shutdown();
    }


    @Test
    public void testAggregatingGetJobClosedOutProperlyForProcessingWhenReadingLocally()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.updateBean(
                job.setCreatedAt( new Date( 1000 ) ).setAggregating( true ),
                JobObservable.CREATED_AT, Job.AGGREGATING );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createJobEntry( job.getId(), b2 );

        final TapePartition tp = mockDaoDriver.createTapePartition( null, null );
        final Tape tape = mockDaoDriver.createTape( tp.getId(), TapeState.NORMAL );
        mockDaoDriver.putBlobOnTape( tape.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b2.getId() );

        final PoolPartition pp = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "pp1" );
        final Pool pool = mockDaoDriver.createPool( pp.getId(), PoolState.NORMAL );
        mockDaoDriver.putBlobOnPool( pool.getId(), b2.getId(), null, null );

        final BlobStoreDriverImpl driver = newBlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                newJobCreatorImpl( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                3600000 );
        driver.cleanUpAndProcessChunks();

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda reshaped job and marked it as unshapable.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Job.class ).attain(
                        Require.nothing() ).isAggregating(), "Shoulda reshaped job and marked it as unshapable.");
        driver.shutdown();
    }


    @Test
    public void testAggregatingGetJobClosedOutProperlyForProcessingWhenReadingFromDs3Target()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.updateBean(
                job.setCreatedAt( new Date( 1000 ) ).setAggregating( true ),
                JobObservable.CREATED_AT, Job.AGGREGATING );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createJobEntry( job.getId(), b2 );

        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.updateBean(
                target.setDefaultReadPreference( TargetReadPreferenceType.AFTER_NEARLINE_POOL ),
                ReplicationTarget.DEFAULT_READ_PREFERENCE );

        final MockDs3ConnectionFactory ds3ConnectionFactory =
                new MockDs3ConnectionFactory();
        final BlobPersistenceContainer blobPersistenceContainer =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( b1.getId() );
        bp1.setAvailableOnTapeNow( true );
        final BlobPersistence bp2 = BeanFactory.newBean( BlobPersistence.class );
        bp2.setId( b2.getId() );
        bp2.setAvailableOnTapeNow( true );
        blobPersistenceContainer.setBlobs( new BlobPersistence [] { bp1, bp2 } );
        ds3ConnectionFactory.setGetBlobPersistenceResponse( blobPersistenceContainer );

        final JobChunkToReplicate remoteEntry = BeanFactory.newBean( JobChunkToReplicate.class );
        remoteEntry.setBlobId( b1.getId() );
        remoteEntry.setId( UUID.randomUUID() );
        remoteEntry.setChunkNumber( 222 );
        final JobChunkToReplicate remoteEntry2 = BeanFactory.newBean( JobChunkToReplicate.class );
        remoteEntry2.setBlobId( b2.getId() );
        remoteEntry2.setId( UUID.randomUUID() );
        remoteEntry2.setChunkNumber( 223 );
        final JobChunkToReplicate remoteChunk = BeanFactory.newBean( JobChunkToReplicate.class );
        remoteChunk.setId( remoteEntry.getId() );
        final DetailedJobToReplicate remoteJob = BeanFactory.newBean( DetailedJobToReplicate.class );
        remoteJob.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        remoteJob.getJob().setChunks( new JobChunkToReplicate [] { remoteEntry, remoteEntry2 } );
        ds3ConnectionFactory.setCreateGetJobResponse( remoteJob.getJob() );

        final TapePartition tp = mockDaoDriver.createTapePartition( null, null );
        final Tape tape = mockDaoDriver.createTape( tp.getId(), TapeState.NORMAL );
        mockDaoDriver.putBlobOnTape( tape.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), b2.getId() );

        final BlobStoreDriverImpl driver = new BlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null ),
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        ds3ConnectionFactory,
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        2,
                        Long.valueOf( 2 ) ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                ds3ConnectionFactory,
                3600000 );
        driver.cleanUpAndProcessChunks();

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda reshaped job and marked it as unshapable.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Job.class ).attain(
                        Require.nothing() ).isAggregating(), "Shoulda reshaped job and marked it as unshapable.");
    }


    @Test
    public void testAggregatingVerifyJobClosedOutProperlyForProcessing()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        mockDaoDriver.updateBean(
                job.setCreatedAt( new Date( 1000 ) ).setAggregating( true ),
                JobObservable.CREATED_AT, Job.AGGREGATING );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createJobEntry( job.getId(), b2 );

        final TapePartition tp = mockDaoDriver.createTapePartition( null, null );
        final Tape tape = mockDaoDriver.createTape( tp.getId(), TapeState.NORMAL );
        mockDaoDriver.putBlobOnTape( tape.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b2.getId() );

        final PoolPartition pp = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "pp1" );
        final Pool pool = mockDaoDriver.createPool( pp.getId(), PoolState.NORMAL );
        mockDaoDriver.putBlobOnPool( pool.getId(), b2.getId(), null, null );

        final BlobStoreDriverImpl driver = newBlobStoreDriverImpl(
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                InterfaceProxyFactory.getProxy( PoolBlobStore.class, null ),
                dbSupport.getServiceManager(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                newJobCreatorImpl( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                3600000 );
        driver.cleanUpAndProcessChunks();

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "Shoulda reshaped job and marked it as unshapable.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Job.class ).attain(
                        Require.nothing() ).isAggregating(), "Shoulda reshaped job and marked it as unshapable.");
        driver.shutdown();
    }


    @Test
    public void testJobChunksOrderedDeterministricallyByJobCreationDateThenChunkNumber()
    {
        final Set<DetailedJobEntry> chunks = BeanUtils.getSortedSet( DetailedJobEntry.class );

        final UUID jobId1 = UUID.randomUUID();
        final UUID jobId2 = UUID.randomUUID();
        final UUID jobId3 = UUID.randomUUID();
        final DetailedJobEntry chunk1 = BeanFactory.newBean( DetailedJobEntry.class )
                .setChunkNumber( 0 ).setCreatedAt( new Date( 4000 ) ).setJobId( jobId1 ).setPriority(BlobStoreTaskPriority.HIGH);
        final DetailedJobEntry chunk2 = BeanFactory.newBean( DetailedJobEntry.class )
                .setChunkNumber( 1 ).setCreatedAt( new Date( 4000 ) ).setJobId( jobId1 ).setPriority(BlobStoreTaskPriority.HIGH);
        final DetailedJobEntry chunk3 = BeanFactory.newBean( DetailedJobEntry.class )
                .setChunkNumber( 0 ).setCreatedAt( new Date( 2000 ) ).setJobId( jobId2 ).setPriority(BlobStoreTaskPriority.NORMAL);
        final DetailedJobEntry chunk4 = BeanFactory.newBean( DetailedJobEntry.class )
                .setChunkNumber( 1 ).setCreatedAt( new Date( 2000 ) ).setJobId( jobId2 ).setPriority(BlobStoreTaskPriority.NORMAL);
        final DetailedJobEntry chunk5 = BeanFactory.newBean( DetailedJobEntry.class )
                .setChunkNumber( 2 ).setCreatedAt( new Date( 2000 ) ).setJobId( jobId2 ).setPriority(BlobStoreTaskPriority.NORMAL);
        final DetailedJobEntry chunk6 = BeanFactory.newBean( DetailedJobEntry.class )
                .setChunkNumber( 0 ).setCreatedAt( new Date( 3000 ) ).setJobId( jobId3 ).setPriority(BlobStoreTaskPriority.NORMAL);
        final List<DetailedJobEntry> unsortedChunks =
                CollectionFactory.toList( chunk1, chunk2, chunk3, chunk4, chunk5, chunk6 );
        Collections.shuffle( unsortedChunks );

        for ( final DetailedJobEntry chunk : unsortedChunks )
        {
            chunks.add( chunk );
        }

        final List<DetailedJobEntry> sortedChunks = new ArrayList<>( chunks );
        assertEquals(chunk1, sortedChunks.get( 0 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk2, sortedChunks.get( 1 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk3, sortedChunks.get( 2 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk4, sortedChunks.get( 3 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk5, sortedChunks.get( 4 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk6, sortedChunks.get( 5 ), "Shoulda sorted chunks by job creation date, then chunk number.");
    }


    @Test
    public void testJobChunksOrderedDeterministricallyByChunkNumberWhenNullJobInfo()
    {
        final Set<JobEntry> chunks = BeanUtils.getSortedSet( JobEntry.class );

        final JobEntry chunk1 = BeanFactory.newBean( JobEntry.class )
                .setChunkNumber( 0 );
        final JobEntry chunk2 = BeanFactory.newBean( JobEntry.class )
                .setChunkNumber( 1 );
        final JobEntry chunk3 = BeanFactory.newBean( JobEntry.class )
                .setChunkNumber( 2 );
        final JobEntry chunk4 = BeanFactory.newBean( JobEntry.class )
                .setChunkNumber( 3 );
        final JobEntry chunk5 = BeanFactory.newBean( JobEntry.class )
                .setChunkNumber( 4 );
        final JobEntry chunk6 = BeanFactory.newBean( JobEntry.class )
                .setChunkNumber( 5 );
        final List<JobEntry> unsortedChunks =
                CollectionFactory.toList( chunk1, chunk2, chunk3, chunk4, chunk5, chunk6 );
        Collections.shuffle( unsortedChunks );

        for ( final JobEntry chunk : unsortedChunks )
        {
            chunks.add( chunk );
        }

        final List<JobEntry> sortedChunks = new ArrayList<>( chunks );
        assertEquals(chunk1, sortedChunks.get( 0 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk2, sortedChunks.get( 1 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk3, sortedChunks.get( 2 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk4, sortedChunks.get( 3 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk5, sortedChunks.get( 4 ), "Shoulda sorted chunks by job creation date, then chunk number.");
        assertEquals(chunk6, sortedChunks.get( 5 ), "Shoulda sorted chunks by job creation date, then chunk number.");
    }


    private static void waitUntilShutdown( final BlobStoreDriverImpl driver )
    {
        int i = 1000;
        while ( --i > 0 && !driver.isShutdown() )
        {
            TestUtil.sleep( 10 );
        }
    }


    private BlobStoreDriverImpl newBlobStoreDriverImpl(
            final TapeBlobStore tapeBlobStore,
            final PoolBlobStore poolBlobStore,
            final BeansServiceManager serviceManager,
            final DiskManager cacheManager,
            final JobCreator jobCreator,
            final JobProgressManager jobProgressManager,
            final int intervalInMillisToCheckForNewActivity )
    {
        final TargetBlobStore targetBlobStore =
                InterfaceProxyFactory.getProxy( TargetBlobStore.class, null );
        return newBlobStoreDriverImpl(
                tapeBlobStore,
                poolBlobStore,
                targetBlobStore,
                targetBlobStore,
                targetBlobStore,
                serviceManager,
                cacheManager,
                jobCreator,
                jobProgressManager,
                intervalInMillisToCheckForNewActivity );
    }


    private BlobStoreDriverImpl newBlobStoreDriverImpl(
            final TapeBlobStore tapeBlobStore,
            final PoolBlobStore poolBlobStore,
            final TargetBlobStore ds3TargetBlobStore,
            final TargetBlobStore azureTargetBlobStore,
            final TargetBlobStore s3TargetBlobStore,
            final BeansServiceManager serviceManager,
            final DiskManager cacheManager,
            final JobCreator jobCreator,
            final JobProgressManager jobProgressManager,
            final int intervalInMillisToCheckForNewActivity )
    {
        return new BlobStoreDriverImpl(
                tapeBlobStore,
                poolBlobStore,
                ds3TargetBlobStore,
                azureTargetBlobStore,
                s3TargetBlobStore,
                serviceManager,
                cacheManager,
                jobCreator,
                jobProgressManager,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                intervalInMillisToCheckForNewActivity );
    }


    private JobCreator newJobCreatorImpl( final BeansServiceManager serviceManager )
    {
        return new JobCreatorImpl(
                new MockDiskManager( serviceManager ),
                serviceManager,
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                2,
                Long.valueOf( 2 ) );
    }

    private static Map<PersistenceType, BlobStore> blobStores() {
        return Map.of(
                PersistenceType.DS3, InterfaceProxyFactory.getProxy(BlobStore.class, null),
                PersistenceType.TAPE, InterfaceProxyFactory.getProxy(BlobStore.class, null),
                PersistenceType.AZURE, InterfaceProxyFactory.getProxy(BlobStore.class, null),
                PersistenceType.S3, InterfaceProxyFactory.getProxy(BlobStore.class, null),
                PersistenceType.POOL, InterfaceProxyFactory.getProxy(BlobStore.class, null)
        );
    }

    private static DatabaseSupport dbSupport;

    @BeforeAll
    public static void setDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void setUp() {
        dbSupport.reset();
    }
}
