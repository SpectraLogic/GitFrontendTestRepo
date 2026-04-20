/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.task.MockDs3ConnectionFactory;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.s3.common.platform.persistencetarget.BlobDestinationUtils;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
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

public final class Ds3TargetBlobStore_Test 
{

    @Test
    public void testConstructorNullDs3ConnectionFactoryNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new Ds3TargetBlobStore(
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
                new Ds3TargetBlobStore(
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
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
                new Ds3TargetBlobStore(
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
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
                new Ds3TargetBlobStore(
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        new MockDiskManager( null ),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {
        new Ds3TargetBlobStore(
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ), 
                dbSupport.getServiceManager() );
    }
    
    
    @Test
    public void testWriteChunkWorks()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Ds3Target target = mockDaoDriver.createDs3Target("ds3target");
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID nodeId = mockDaoDriver.attainOneAndOnly(Node.class).getId();

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk1.getId());
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        
        final Ds3TargetBlobStore store = new Ds3TargetBlobStore( 
                new MockDs3ConnectionFactory(),
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
        mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk2.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b2.getId());

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(2,  store.getTasks().size(), "Shoulda created task.");
            assertEquals(2,  store.getTasks().size(), "Should notta created task.");
        });
    }
    
    
    @Test
    public void testReadChunkWorks()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID nodeId = mockDaoDriver.attainOneAndOnly(Node.class).getId();
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );

        final Ds3Target target = mockDaoDriver.createDs3Target("ds3target");
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.allocateBlob(b1.getId());
        mockDaoDriver.updateBean(chunk1.setReadFromDs3TargetId(target.getId()), JobEntry.READ_FROM_DS3_TARGET_ID);
        final DetailedJobToReplicate remoteJob = BeanFactory.newBean( DetailedJobToReplicate.class );
        remoteJob.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        remoteJob.getJob().setId( UUID.randomUUID() );
        final MockDs3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory();
        ds3ConnectionFactory.setCreateGetJobResponse( remoteJob.getJob() );
        final Ds3TargetBlobStore store = new Ds3TargetBlobStore(
                ds3ConnectionFactory,
                mockDiskManager,
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );
        assertEquals(0,  store.getTasks().size(), "Should notta been any tasks initially.");

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(1,  store.getTasks().size(), "Shoulda created task.");
        });

        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.allocateBlob(b2.getId());
        mockDaoDriver.updateBean(chunk2.setReadFromDs3TargetId(target.getId()), JobEntry.READ_FROM_DS3_TARGET_ID);

        store.taskSchedulingRequired();
        TestUtil.waitUpTo(1, TimeUnit.SECONDS, () -> {
            assertEquals(2, store.getTasks().size(), "Shoulda created task.");
        });
    }
    
    
    @Test
    public void testRefreshPersistenceTargetEnvironmentNowDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final MockDs3ConnectionFactory connection = new MockDs3ConnectionFactory();
        connection.setConnectException( new RuntimeException( "I can't connect." ) );
        mockDaoDriver.createDs3Target( "t1" );
        
        final Ds3TargetBlobStore store = new Ds3TargetBlobStore( 
                connection,
                new MockDiskManager( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                dbSupport.getServiceManager() );

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta reported any target failure initially.");
        store.refreshEnvironmentNow();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda created target failure due to connection failure.");

        connection.setConnectException( null );
        store.refreshEnvironmentNow();
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda whacked target failure due to connection success.");

        connection.setConnectException( new RuntimeException( "I can't connect." ) );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.updateBean( target2.setQuiesced( Quiesced.PENDING ), ReplicationTarget.QUIESCED );
        store.refreshEnvironmentNow();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda created target failure for connection failure, but not for quiesced target.");
        assertEquals(Quiesced.YES, mockDaoDriver.attain( target2 ).getQuiesced(), "Shoulda marked target as quiesced since no active task running for it.");
    }
    
    
    @Test
    public void testTaskExecutionWorksProperly()
    {
        final String tn = Thread.currentThread().getName();
        try
        {
            final MockDs3ConnectionFactory ds3Connection = new MockDs3ConnectionFactory();
            final DatabaseSupport dbSupport =
                    DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
            final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                    new MockCacheFilesystemDriver( dbSupport );
            final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                    new MockTierExistingCacheImpl() ) );
            final JobProgressManager jobProgressManager = new JobProgressManagerImpl( dbSupport.getServiceManager(), BufferProgressUpdates.NO);

            final Ds3TargetBlobStore store = new Ds3TargetBlobStore( 
                    ds3Connection,
                    cacheManager,
                    jobProgressManager,
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
                                                  blob1 );
    
            final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
            final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
            final Ds3Target target2 = mockDaoDriver.createDs3Target( "target2" );
            mockDaoDriver.updateBean( target2.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
            mockDaoDriver.createDs3DataReplicationRule(
                    dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
            mockDaoDriver.createDs3DataReplicationRule(
                    dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
            
            cacheManager.allocateChunksForBlob( blob1.getId() );
            mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
            cacheManager.blobLoadedToCache( blob1.getId() );
            
            mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk.getId());
    
            ds3Connection.setIsJobExistantResponse( false );
            ds3Connection.setGetBlobPersistenceResponse(
                    BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( 
                            (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ) );
            ds3Connection.setIsBucketExistantResponse( true );
            ds3Connection.setBlobsReady( CollectionFactory.toList( blob1.getId() ) );
            ds3Connection.setPutBlobException( null );
            ds3Connection.setReplicatePutJobException( null );
            ds3Connection.setCreateBucketException( null );
            
            runBlobStoreProcessor( store );
            assertEquals(0,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated target failure.");
            assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Should notta recorded blobs on offline target yet.");
            assertEquals(target.getId(),  mockDaoDriver.attainOneAndOnly(BlobDs3Target.class).getTargetId(), "Shoulda replicated to online taret.");

            mockDaoDriver.updateBean( target2.setState( TargetState.ONLINE ), ReplicationTarget.STATE );

            runBlobStoreProcessor( store );
            BlobDestinationUtils.cleanupCompletedEntriesAndDestinations(dbSupport.getServiceManager(), jobProgressManager);
            assertEquals(2,  dbSupport.getServiceManager().getRetriever(BlobDs3Target.class).getCount(), "Shoulda recorded blobs on target yet.");

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
