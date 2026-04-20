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
import com.spectralogic.s3.common.dao.service.target.SuspectBlobAzureTargetService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;

import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.target.task.MockS3ConnectionFactory;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public final class S3TargetBlobStore_Test
{
    @Test
    public void testConstructorNullS3ConnectionFactoryNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new S3TargetBlobStore(
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
                new S3TargetBlobStore(
                        InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
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
                new S3TargetBlobStore(
                        InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
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
                new S3TargetBlobStore(
                        InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
                        new MockDiskManager( null ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        null );
            }
        } );
    }


    @Test
    public void testConstructorHappyConstruction()
    {
        new S3TargetBlobStore(
                InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
    }


    @Test
    public void testWriteChunkWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target = mockDaoDriver.createS3Target("s3target");
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );


        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createReplicationTargetsForChunk(S3DataReplicationRule.class, S3BlobDestination.class, chunk1.getId());
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());

        final S3TargetBlobStore store = new S3TargetBlobStore(
                new MockS3ConnectionFactory(),
                mockDiskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals(0,  store.getTasks().size(), "Should notta been any tasks initially.");
        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(1,  store.getTasks().size(), "Shoulda created task.");
        });

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.createReplicationTargetsForChunk(S3DataReplicationRule.class, S3BlobDestination.class, chunk2.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b2.getId());

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(2,  store.getTasks().size(), "Shoulda created task.");
        });
    }


    @Test
    public void testReadChunkWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );

        final S3Target target = mockDaoDriver.createS3Target("s3target");
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.allocateBlob( b1.getId() );
        mockDaoDriver.updateBean(chunk1.setReadFromS3TargetId(target.getId()), JobEntry.READ_FROM_S3_TARGET_ID);


        final S3TargetBlobStore store = new S3TargetBlobStore(
                new MockS3ConnectionFactory(),
                mockDiskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals(0,  store.getTasks().size(), "Should notta been any tasks initially.");

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(1, store.getTasks().size(), "Shoulda created task.");
        });

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.allocateBlob( b2.getId() );
        mockDaoDriver.updateBean(chunk2.setReadFromS3TargetId(target.getId()), JobEntry.READ_FROM_S3_TARGET_ID);

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(2,  store.getTasks().size(), "Shoulda created task.");
        });
    }

    @Test
    public void testReadChunkFailure()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );

        final S3Target target = mockDaoDriver.createS3Target("s3target");
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.allocateBlob( b1.getId() );
        mockDaoDriver.updateBean(chunk1.setReadFromS3TargetId(target.getId()), JobEntry.READ_FROM_S3_TARGET_ID);

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.allocateBlob( b2.getId() );
        mockDaoDriver.updateBean(chunk2.setReadFromS3TargetId(target.getId()), JobEntry.READ_FROM_S3_TARGET_ID);

        final S3TargetBlobStore store = new S3TargetBlobStore(
                new MockS3ConnectionFactory(),
                mockDiskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals(0,  store.getTasks().size(), "Should notta been any tasks initially.");

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(1,  store.getTasks().size(), "Shoulda created task.");
        });

        final S3TargetBlobStore spyStore = spy( store );
        doThrow( new RuntimeException( "Simulated read failure" ) ).when( spyStore ).createReadChunkFromS3TargetTask( any() );

        final RuntimeException e = assertThrows( RuntimeException.class, () -> spyStore.read( mock( ReadDirective.class ) ) );
        assertEquals( "Simulated read failure", e.getMessage() );
    }

    @Test
    public void testImportWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final S3Target target1 = mockDaoDriver.createS3Target( null );
        final S3Target target2 = mockDaoDriver.createS3Target( null );
        
        final S3TargetBlobStore store = new S3TargetBlobStore( 
                new MockS3ConnectionFactory(),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals(0,  store.getTasks().size(), "Should notta been any tasks initially.");

        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
                {
                    store.importTarget( null );
                }
            } );
        
        store.importTarget( 
                BeanFactory.newBean( ImportS3TargetDirective.class )
                .setTargetId( target1.getId() ) );
        assertEquals(1,  store.getTasks().size(), "Shoulda created task.");

        store.importTarget( 
                BeanFactory.newBean( ImportS3TargetDirective.class )
                .setTargetId( target2.getId() ) );
        assertEquals(2,  store.getTasks().size(), "Shoulda created task.");

        store.importTarget( 
                BeanFactory.newBean( ImportS3TargetDirective.class )
                .setTargetId( target1.getId() )
                .setCloudBucketName( "b1" ) );
        assertEquals(2,  store.getTasks().size(), "Should notta created task.");

        store.importTarget( 
                BeanFactory.newBean( ImportS3TargetDirective.class )
                .setTargetId( target1.getId() )
                .setCloudBucketName( "b2" ) );
        assertEquals(2,  store.getTasks().size(), "Should notta created task.");
    }
    
    
    @Test
    public void testRefreshPersistenceTargetEnvironmentNowDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final MockS3ConnectionFactory connection = new MockS3ConnectionFactory();
        connection.setConnectException( new RuntimeException( "I can't connect." ) );
        mockDaoDriver.createS3Target( "t1" );
        
        final S3TargetBlobStore store = new S3TargetBlobStore( 
                connection,
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Should notta reported any target failure initially.");
        store.refreshEnvironmentNow();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda created target failure due to connection failure.");

        connection.setConnectException( null );
        store.refreshEnvironmentNow();
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda whacked target failure due to connection success.");

        connection.setConnectException( new RuntimeException( "I can't connect." ) );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        mockDaoDriver.updateBean( target2.setQuiesced( Quiesced.PENDING ), ReplicationTarget.QUIESCED );
        store.refreshEnvironmentNow();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda created target failure for connection failure, but not for quiesced target.");
        assertEquals(Quiesced.YES, mockDaoDriver.attain( target2 ).getQuiesced(), "Shoulda marked target as quiesced since no active task running for it.");
    }
    
    
    @Test
    public void testTaskExecutionWorksProperly()
    {
        final String tn = Thread.currentThread().getName();
        try
        {
            final MockS3ConnectionFactory s3Connection = new MockS3ConnectionFactory();
            final DatabaseSupport dbSupport =
                    DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
            final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                    new MockCacheFilesystemDriver( dbSupport );
            final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                    new MockTierExistingCacheImpl() ) );
            
            final S3TargetBlobStore store = new S3TargetBlobStore( 
                    s3Connection,
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
            
            final JobEntry chunk =
                    mockDaoDriver.createJobWithEntry( JobRequestType.PUT,
                                                  blob1);
    
            final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
            final S3Target target = mockDaoDriver.createS3Target( "target1" );
            final S3Target target2 = mockDaoDriver.createS3Target( "target2" );
            mockDaoDriver.updateBean( target2.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
            mockDaoDriver.createS3DataReplicationRule(
                    dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
            mockDaoDriver.createS3DataReplicationRule(
                    dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
            
            cacheManager.allocateChunksForBlob( blob1.getId() );
            mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
            cacheManager.blobLoadedToCache( blob1.getId() );
            
            runBlobStoreProcessor( store );
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda deleted system failure for writes being stalled since cloud out not in use.");
            mockDaoDriver.createReplicationTargetsForChunk(S3DataReplicationRule.class, S3BlobDestination.class, chunk.getId());
    
            s3Connection.setConnectException( null );

            mockDaoDriver.createFeatureKey( FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT );
            runBlobStoreProcessor( store );
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Should notta generated target failure.");
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobS3Target.class).getCount(), "Should notta recorded any blobs on target yet.");
            assertEquals(SystemFailureType.AWS_S3_WRITES_REQUIRE_FEATURE_LICENSE, mockDaoDriver.attainOneAndOnly( SystemFailure.class ).getType(), "Shoulda generated system failure for writes being stalled.");

            runBlobStoreProcessor( store );
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Should notta generated target failure.");
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobS3Target.class).getCount(), "Should notta recorded any blobs on target yet.");
            assertEquals(SystemFailureType.AWS_S3_WRITES_REQUIRE_FEATURE_LICENSE, mockDaoDriver.attainOneAndOnly( SystemFailure.class ).getType(), "Should notta generated another system failure for writes being stalled.");

            mockDaoDriver.createFeatureKey( FeatureKeyType.AWS_S3_CLOUD_OUT );
            runBlobStoreProcessor( store );
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Should notta generated target failure.");
            assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobS3Target.class).getCount(), "Should notta recorded all blobs on target yet.");
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda deleted system failure for writes being unstalled.");

            mockDaoDriver.updateBean( target2.setState( TargetState.ONLINE ), ReplicationTarget.STATE );

            assertFalse(mockDaoDriver.attain( chunk ).isPendingTargetCommit(), "Should notta reported pending commit yet.");
            runBlobStoreProcessor( store );
            BlobDestinationUtils.cleanupCompletedEntriesAndDestinations( dbSupport.getServiceManager(), store.m_jobProgressManager);
            assertNull(
                    mockDaoDriver.retrieve( chunk ),
                    "Shoulda reported chunk complete."
                     );
            assertEquals(2,  dbSupport.getServiceManager().getRetriever(BlobS3Target.class).getCount(), "Shoulda recorded all blobs on target.");
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(SystemFailure.class).getCount(), "Shoulda deleted system failure for writes being unstalled.");

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
        fail( "Store never became idle." );
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
