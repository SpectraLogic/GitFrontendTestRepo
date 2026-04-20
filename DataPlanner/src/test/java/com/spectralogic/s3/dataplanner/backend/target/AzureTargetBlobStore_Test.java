/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.task.MockAzureConnectionFactory;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.s3.common.platform.persistencetarget.BlobDestinationUtils;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.ThrottledRunnable;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class AzureTargetBlobStore_Test 
{
    @Test
    public void testConstructorNullAzureConnectionFactoryNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new AzureTargetBlobStore(
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
                new AzureTargetBlobStore(
                        InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
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
                new AzureTargetBlobStore(
                        InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
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
                new AzureTargetBlobStore(
                        InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
                        new MockDiskManager( null ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {
        
        
        new AzureTargetBlobStore(
                InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
    }


    @Test
    public void testWriteChunkWorks()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget("azureTarget");
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID nodeId = mockDaoDriver.attainOneAndOnly(Node.class).getId();

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createReplicationTargetsForChunk(AzureDataReplicationRule.class, AzureBlobDestination.class, chunk1.getId());
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());

        final AzureTargetBlobStore store = new AzureTargetBlobStore(
                new MockAzureConnectionFactory(),
                mockDiskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals( 0,  store.getTasks().size(), "Should notta been any tasks initially.");
        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals( 1,  store.getTasks().size(), "Shoulda created task.");
        });


        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.createReplicationTargetsForChunk(AzureDataReplicationRule.class, AzureBlobDestination.class, chunk2.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b2.getId());

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(2, store.getTasks().size(), "Shoulda created task.");
            assertEquals(2, store.getTasks().size(), "Should notta created task.");
        });
    }


    @Test
    public void testReadChunkWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID nodeId = mockDaoDriver.attainOneAndOnly(Node.class).getId();
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );

        final AzureTarget target = mockDaoDriver.createAzureTarget("azureTarget");
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.allocateBlob( b1.getId() );
        mockDaoDriver.updateBean(chunk1.setReadFromAzureTargetId(target.getId()), JobEntry.READ_FROM_AZURE_TARGET_ID);


        final AzureTargetBlobStore store = new AzureTargetBlobStore(
                new MockAzureConnectionFactory(),
                mockDiskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals((Object) 0,  store.getTasks().size(), "Should notta been any tasks initially.");

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals((Object) 1, store.getTasks().size(), "Shoulda created task.");
        });

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.allocateBlob( b2.getId() );
        mockDaoDriver.updateBean(chunk2.setReadFromAzureTargetId(target.getId()), JobEntry.READ_FROM_AZURE_TARGET_ID);

        store.taskSchedulingRequired();
        TestUtil.waitUpTo( 1, TimeUnit.SECONDS, () -> {
            assertEquals(2,  store.getTasks().size(), "Shoulda created task.");
        }
        );

    }
    
    
    @Test
    public void testImportWorks()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( null );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( null );
        
        final AzureTargetBlobStore store = new AzureTargetBlobStore( 
                new MockAzureConnectionFactory(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals((Object) 0,  store.getTasks().size(), "Should notta been any tasks initially.");

        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
                {
                    store.importTarget( null );
                }
            } );
        
        store.importTarget( 
                BeanFactory.newBean( ImportAzureTargetDirective.class )
                .setTargetId( target1.getId() ) );
        assertEquals((Object) 1,  store.getTasks().size(), "Shoulda created task.");

        store.importTarget( 
                BeanFactory.newBean( ImportAzureTargetDirective.class )
                .setTargetId( target2.getId() ) );
        assertEquals((Object) 2,  store.getTasks().size(), "Shoulda created task.");

        store.importTarget( 
                BeanFactory.newBean( ImportAzureTargetDirective.class )
                .setTargetId( target1.getId() )
                .setCloudBucketName( "b1" ) );
        assertEquals((Object) 2,  store.getTasks().size(), "Should notta created task.");

        store.importTarget( 
                BeanFactory.newBean( ImportAzureTargetDirective.class )
                .setTargetId( target1.getId() )
                .setCloudBucketName( "b2" ) );
        assertEquals((Object) 2,  store.getTasks().size(), "Should notta created task.");
    }
    
    
    @Test
    public void testRefreshPersistenceTargetEnvironmentNowDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final MockAzureConnectionFactory connection = new MockAzureConnectionFactory();
        connection.setConnectException( new RuntimeException( "I can't connect." ) );
        mockDaoDriver.createAzureTarget( "t1" );
        
        final AzureTargetBlobStore store = new AzureTargetBlobStore( 
                connection,
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );

        final Object actual4 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 0, actual4, "Should notta reported any target failure initially.");
        store.refreshEnvironmentNow();
        final Object actual3 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 1, actual3, "Shoulda created target failure due to connection failure.");

        connection.setConnectException( null );
        store.refreshEnvironmentNow();
        final Object actual2 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 0, actual2, "Shoulda whacked target failure due to connection success.");

        connection.setConnectException( new RuntimeException( "I can't connect." ) );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        mockDaoDriver.updateBean( target2.setQuiesced( Quiesced.PENDING ), ReplicationTarget.QUIESCED );
        store.refreshEnvironmentNow();
        final Object actual1 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 1, actual1, "Shoulda created target failure for connection failure, but not for quiesced target.");
        final Object actual = mockDaoDriver.attain( target2 ).getQuiesced();
        assertEquals((Object) Quiesced.YES, actual, "Shoulda marked target as quiesced since no active task running for it.");
    }
    
    
    @Test
    public void testTaskExecutionWorksProperly()
    {
        final String tn = Thread.currentThread().getName();
        try
        {
            final MockAzureConnectionFactory azureConnection = new MockAzureConnectionFactory();
            final DatabaseSupport dbSupport =
                    DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
            final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                    new MockCacheFilesystemDriver( dbSupport );
            final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                    new MockTierExistingCacheImpl() ) );
            
            final AzureTargetBlobStore store = new AzureTargetBlobStore( 
                    azureConnection,
                    cacheManager,
                    new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO ),
                    dbSupport.getServiceManager() );
            
            final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
            final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
            final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
            final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
            mockDaoDriver.updateBean( 
                    blob1.setChecksum( "v" ).setChecksumType( ChecksumType.values()[ 0 ] ), 
                    ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
            final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
            
            final JobEntry chunk =
                    mockDaoDriver.createJobWithEntry( JobRequestType.PUT,
                                                  blob1 );
    
            final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
            final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
            final AzureTarget target2 = mockDaoDriver.createAzureTarget( "target2" );
            mockDaoDriver.updateBean( target2.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
            mockDaoDriver.createAzureDataReplicationRule(
                    dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
            mockDaoDriver.createAzureDataReplicationRule(
                    dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
            
            cacheManager.allocateChunksForBlob( blob1.getId() );
            mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
            cacheManager.blobLoadedToCache( blob1.getId() );
            
            runBlobStoreProcessor( store );
            final Object actual11 = dbSupport.getServiceManager().getRetriever( SystemFailure.class ).getCount();
            assertEquals((Object) 0, actual11, "Shoulda deleted system failure for writes being stalled since cloud out not in use.");

            mockDaoDriver.createReplicationTargetsForChunk(AzureDataReplicationRule.class, AzureBlobDestination.class, chunk.getId());
    
            azureConnection.setConnectException( null );

            mockDaoDriver.createFeatureKey( FeatureKeyType.AWS_S3_CLOUD_OUT );
            runBlobStoreProcessor( store );
            final Object actual10 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
            assertEquals((Object) 0, actual10, "Should notta generated target failure.");
            final Object actual9 = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
            assertEquals((Object) 0, actual9, "Should notta recorded any blobs on target yet.");
            final Object actual8 = mockDaoDriver.attainOneAndOnly( SystemFailure.class ).getType();
            assertEquals((Object) SystemFailureType.MICROSOFT_AZURE_WRITES_REQUIRE_FEATURE_LICENSE, actual8, "Shoulda generated system failure for writes being stalled.");

            runBlobStoreProcessor( store );
            final Object actual7 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
            assertEquals((Object) 0, actual7, "Should notta generated target failure.");
            final Object actual6 = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
            assertEquals((Object) 0, actual6, "Should notta recorded any blobs on target yet.");
            final Object actual5 = mockDaoDriver.attainOneAndOnly( SystemFailure.class ).getType();
            assertEquals((Object) SystemFailureType.MICROSOFT_AZURE_WRITES_REQUIRE_FEATURE_LICENSE, actual5, "Should notta generated another system failure for writes being stalled.");

            mockDaoDriver.createFeatureKey( FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT );
            runBlobStoreProcessor( store );
            final Object actual4 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
            assertEquals((Object) 0, actual4, "Should notta generated target failure.");
            final Object actual3 = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
            assertEquals((Object) 1, actual3, "Should notta recorded all blobs on target yet.");
            final Object actual2 = dbSupport.getServiceManager().getRetriever( SystemFailure.class ).getCount();
            assertEquals((Object) 0, actual2, "Shoulda deleted system failure for writes being unstalled.");

            mockDaoDriver.updateBean( target2.setState( TargetState.ONLINE ), ReplicationTarget.STATE );

            assertFalse(mockDaoDriver.attain( chunk ).isPendingTargetCommit(), "Should notta reported pending commit yet.");
            runBlobStoreProcessor( store );
            BlobDestinationUtils.cleanupCompletedEntriesAndDestinations( dbSupport.getServiceManager(), store.m_jobProgressManager);
            assertNull(
                    mockDaoDriver.retrieve( chunk ),
                    "Shoulda reported chunk complete."
                    );
            final Object actual1 = dbSupport.getServiceManager().getRetriever( BlobAzureTarget.class ).getCount();
            assertEquals((Object) 2, actual1, "Shoulda recorded all blobs on target.");
            final Object actual = dbSupport.getServiceManager().getRetriever( SystemFailure.class ).getCount();
            assertEquals((Object) 0, actual, "Shoulda deleted system failure for writes being unstalled.");

            mockCacheFilesystemDriver.shutdown();
        }
        finally
        {
            Thread.currentThread().setName( tn );
        }
    }
    
    
    private void runBlobStoreProcessor( final TargetBlobStore store )
    {
        try
        {
            final Field field = BaseTargetBlobStore.class.getDeclaredField(
                    BaseTargetBlobStore.FIELD_TARGET_TASK_STARTER );
            field.setAccessible( true );
            final ThrottledRunnable runnable = (ThrottledRunnable)field.get( store );
            runnable.run( InterfaceProxyFactory.getProxy(
                    ThrottledRunnable.RunnableCompletionNotifier.class, null ) );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        
        waitUntilIdle( store );
    }
    
    
    private void waitUntilIdle( final TargetBlobStore store )
    {
        final Duration duration = new Duration();
        while ( 10 > duration.getElapsedSeconds() )
        {
            boolean idle = true;
            for ( final BlobStoreTask task : store.getTasks() )
            {
                if ( BlobStoreTaskState.READY != task.getState()
                        && BlobStoreTaskState.COMPLETED != task.getState() )
                {
                    idle = false;
                }
            }
            if ( idle )
            {
                return;
            }
            TestUtil.sleep( 10 );
        }
        Assertions.fail( "Store never became idle." );
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
