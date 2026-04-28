/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.*;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.ds3.LocalBlobDestinationService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BaseCreateJobParams;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobbingPolicy;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreateGetJobParams;
import com.spectralogic.s3.common.rpc.dataplanner.domain.S3ObjectToCreate;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.dataplanner.backend.target.task.MockDs3ConnectionFactory;
import com.spectralogic.s3.dataplanner.cache.JobCreatedListener;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.TimeUnit;


import static org.junit.jupiter.api.Assertions.*;

public final class JobCreatorImpl_Test
{
    private static final long PREFERRED_BLOB_SIZE = 1L * 1024 * 1024 * 1024;
    private static final long MAX_BLOB_SIZE = 100L * 1024 * 1024 * 1024;;

    @Test
    public void testConstructorNullCacheManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test() throws Throwable
            {
                new JobCreatorImpl(
                        null,
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        Long.valueOf( 1 ) );
            }
        } );
    }


    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobCreatorImpl(
                        new MockDiskManager( null ),
                        null,
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        Long.valueOf( 1 ) );
            }
        } );
    }


    @Test
    public void testConstructorNullDs3ConnectionFactoryNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        null,
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        1,
                        Long.valueOf( 1 ) );
            }
        } );
    }


    @Test
    public void testConstructorNullJobProgressManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        null,
                        null,
                        1,
                        Long.valueOf( 1 ) );
            }
        } );
    }


    @Test
    public void testConstructorZeroPreferredBlobSizeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new JobCreatorImpl(
                        new MockDiskManager( dbSupport.getServiceManager() ),
                        dbSupport.getServiceManager(),
                        new MockDs3ConnectionFactory(),
                        InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                        blobStores(),
                        0,
                        Long.valueOf( 1 ) );
            }
        } );
    }


    @Test
    public void testConstructorZeroPreferredChunkSizeAllowed()
    {
        new JobCreatorImpl(
                new MockDiskManager( dbSupport.getServiceManager() ),
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                1,
                Long.valueOf( 0 ) );
    }


    @Test
    public void testAddListenerNullListenerNotAllowed()
    {
        final JobCreator jobCreator = new JobCreatorImpl(
                new MockDiskManager( dbSupport.getServiceManager() ),
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                1,
                Long.valueOf( 1 ) );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                jobCreator.addJobCreatedListener( null );
            }
        } );

        final JobCreatedListener listener = InterfaceProxyFactory.getProxy( JobCreatedListener.class, null );
        jobCreator.addJobCreatedListener( listener );
        jobCreator.addJobCreatedListener( listener );
    }


    @Test
    public void testPreferredBlobSizeNotConstraintedByPreferredChunkSize()
    {
        final JobCreator jobCreator = new JobCreatorImpl(
                new MockDiskManager( dbSupport.getServiceManager() ),
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                11,
                Long.valueOf( 10 ) );
        assertEquals(11 * 1024L * 1024,  jobCreator.getPreferredBlobSizeInBytes(), "Should notta limited preferred blob size by preferred chunk size.");
    }


    @Test
    public void testPreferredChunkSizeNotConstraintedByPreferredBlobSize()
    {
        final JobCreator jobCreator = new JobCreatorImpl( 
                new MockDiskManager( dbSupport.getServiceManager() ), 
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                10,
                11L);
        assertEquals(10 * 1024L * 1024,  jobCreator.getPreferredBlobSizeInBytes(), "Should notta limited preferred chunk size by preferred blob size.");
    }


    @Test
    public void testBlobStoreStateCache()
    {final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );


        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( archivePool.getId() ).setBlobId( b3.getId() )
                .setBucketId( o3.getBucketId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( enterprisePool.getId() ).setBlobId( b1.getId() )
                .setBucketId( o1.getBucketId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );
        final List< JobEntry> jobEntries = new ArrayList<>();

        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId());

        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId);
            jobEntries.add( entry);

        }
        UUID[] uuidArray = blobSet.toArray(new UUID[0]);
        final JobCreator jobCreator = new JobCreatorImpl(
                cacheManager,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                10,
                11L);
        assertEquals(10 * 1024L * 1024,  jobCreator.getPreferredBlobSizeInBytes(), "Should notta limited preferred chunk size by preferred blob size.");
        UUID jobId = UUID.randomUUID();
        BaseCreateJobParams<?> params = BeanFactory.newBean( CreateGetJobParams.class )
                .setName( "jobImplTest" )
                .setUserId( bucket.getUserId() )
                .setBlobIds( uuidArray);
        jobCreator.createGetOrVerifyJob(params, jobId, JobRequestType.GET, JobChunkClientProcessingOrderGuarantee.NONE, jobEntries);
        Optional<JobEntry> blob5Entry = jobEntries.stream()
                .filter(entry -> b5.getId().equals(entry.getBlobId()))
                .findFirst();
        Optional<JobEntry> zeroLengthEntry = jobEntries.stream()
                .filter(entry -> b6.getId().equals(entry.getBlobId()))
                .findFirst();
        Optional<JobEntry> enterprisePoolEntry = jobEntries.stream()
                .filter(entry -> b1.getId().equals(entry.getBlobId()))
                .findFirst();
        Optional<JobEntry> archivePoolEntry = jobEntries.stream()
                .filter(entry -> b3.getId().equals(entry.getBlobId()))
                .findFirst();
        assertEquals(JobChunkBlobStoreState.COMPLETED, blob5Entry.get().getBlobStoreState());
        assertEquals(JobChunkBlobStoreState.COMPLETED, zeroLengthEntry.get().getBlobStoreState());
        assertTrue(blob5Entry.get().getChunkNumber() > zeroLengthEntry.get().getChunkNumber());
        assertEquals(JobChunkBlobStoreState.COMPLETED, enterprisePoolEntry.get().getBlobStoreState());
        assertEquals(JobChunkBlobStoreState.COMPLETED, archivePoolEntry.get().getBlobStoreState());
        assertEquals(enterprisePool.getId(), enterprisePoolEntry.get().getReadFromPoolId());
        assertNull( enterprisePoolEntry.get().getReadFromTapeId());
        assertNull( archivePoolEntry.get().getReadFromTapeId());
    }

    @Test
    public void testJobCreatorPut()
    {
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );

        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final User user = mockDaoDriver.createUser( "myUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), dataPolicy.getId(), "myBucket" );

        Set<UUID> blobSet = new HashSet<>();
                S3ObjectCreatorImpl creator = new S3ObjectCreatorImpl(
                PREFERRED_BLOB_SIZE,
                MAX_BLOB_SIZE,
                BlobbingPolicy.ENABLED,
                null,
                bucket.getId(),
                getObjectsToCreate());
        final List<JobEntry> jobEntries = creator.getBlobs().stream().map(blob -> {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class);
            entry.setBlobId(blob.getId());
            blobSet.add(blob.getId());
            return entry;
        }).toList();
        UUID[] uuidArray = blobSet.toArray(new UUID[0]);
        final JobCreator jobCreator = new JobCreatorImpl(
                cacheManager,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                10,
                11L);
        assertEquals(10 * 1024L * 1024,  jobCreator.getPreferredBlobSizeInBytes(), "Should notta limited preferred chunk size by preferred blob size.");

        BaseCreateJobParams<?> params = BeanFactory.newBean( CreateGetJobParams.class )
                .setName( "jobImplTest" )
                .setUserId( bucket.getUserId() )
                .setBlobIds( uuidArray);

        UUID jobID = jobCreator.createPutJob(params, null, creator, jobEntries);
        for (JobEntry entry : jobEntries) {
            assertEquals(JobChunkBlobStoreState.PENDING, entry.getBlobStoreState());
        }
        assertNotNull(jobID);
        int count = dbSupport.getServiceManager().getService(LocalBlobDestinationService.class).getCount();
        assertEquals(jobEntries.size(), count);
        //Clean up blob and indirectly the job entry table which are large in this test
        //The database resetter sometimes times out on this
        mockDaoDriver.deleteAll(Blob.class);
    }


    @Test
    public void testBlobStoreStateCacheVerify()
    {final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );


        cacheManager.blobLoadedToCache( b5.getId() );

        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));

        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( archivePool.getId() ).setBlobId( b3.getId() )
                .setBucketId( o3.getBucketId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( BlobPool.class )
                .setPoolId( enterprisePool.getId() ).setBlobId( b1.getId() )
                .setBucketId( o1.getBucketId() ) );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );
        final List< JobEntry> jobEntries = new ArrayList<>();


        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId());
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId);
            jobEntries.add( entry);

        }
        UUID[] uuidArray = blobSet.toArray(new UUID[0]);
        final JobCreator jobCreator = new JobCreatorImpl(
                cacheManager,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                10,
                11L);
        assertEquals(10 * 1024L * 1024,  jobCreator.getPreferredBlobSizeInBytes(), "Should notta limited preferred chunk size by preferred blob size.");
        UUID jobId = UUID.randomUUID();
        BaseCreateJobParams<?> params = BeanFactory.newBean( CreateGetJobParams.class )
                .setName( "jobImplTest" )
                .setUserId( bucket.getUserId() )
                .setBlobIds( uuidArray);
        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                jobCreator.createGetOrVerifyJob(params, jobId, JobRequestType.VERIFY, JobChunkClientProcessingOrderGuarantee.NONE, jobEntries);
            }
        } );



    }

    @Test
    public void testCreateGetJobWithSingleZeroLengthBlobInCache()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b1 = mockDaoDriver.createBlobs( o1.getId(), 1, 0 ).get( 0 );

        cacheManager.blobLoadedToCache( b1.getId() );

        final List<JobEntry> jobEntries = new ArrayList<>();
        final JobEntry entry = BeanFactory.newBean( JobEntry.class ).setBlobId( b1.getId() );
        jobEntries.add( entry );

        final JobCreator jobCreator = new JobCreatorImpl(
                cacheManager,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                10,
                11L );

        final UUID jobId = UUID.randomUUID();
        final BaseCreateJobParams<?> params = BeanFactory.newBean( CreateGetJobParams.class )
                .setName( "testZeroLengthCached" )
                .setUserId( bucket.getUserId() )
                .setBlobIds( new UUID[]{ b1.getId() } );

        jobCreator.createGetOrVerifyJob( params, jobId, JobRequestType.GET, JobChunkClientProcessingOrderGuarantee.NONE, jobEntries );

        assertEquals( 1, jobEntries.size() );
        assertEquals( JobChunkBlobStoreState.COMPLETED, entry.getBlobStoreState(),
                "Zero-length blob in cache should be marked COMPLETED" );
    }

    @Test
    public void testBlobStoreCacheAllocated()
    {final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "d" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "a" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "c" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "b", -1 );
        final List< Blob > b4 = new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 1 ) );
        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "cach" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        final S3Object o6 = mockDaoDriver.createObject( bucket.getId(), "zerolength", -1 );
        final Blob b6 = mockDaoDriver.createBlobs( o6.getId(), 1, 0 ).get( 0 );

        cacheManager.blobLoadedToCache( b5.getId() );
        cacheManager.allocateChunksForBlob( b1.getId() );
        cacheManager.allocateChunksForBlob( b2.getId() );
        mockDaoDriver.putBlobsOnTape(t2.getId(), b1, b2);
        mockDaoDriver.putBlobsOnTape(t1.getId(), b4.get(0), b3, b4.get(1), b4.get(2));



        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );
        final List< JobEntry> jobEntries = new ArrayList<>();


        Set<UUID> blobSet =  CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId(), b4.get( 0 ).getId(),
                b4.get( 1 ).getId(), b4.get( 2 ).getId(), b5.getId(), b6.getId());
        for (UUID blobId : blobSet) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId);
            jobEntries.add( entry);

        }
        UUID[] uuidArray = blobSet.toArray(new UUID[0]);
        final JobCreator jobCreator = new JobCreatorImpl(
                cacheManager,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                10,
                11L);
        assertEquals(10 * 1024L * 1024,  jobCreator.getPreferredBlobSizeInBytes(), "Should notta limited preferred chunk size by preferred blob size.");
        UUID jobId = UUID.randomUUID();
        BaseCreateJobParams<?> params = BeanFactory.newBean( CreateGetJobParams.class )
                .setName( "jobImplTest" )
                .setUserId( bucket.getUserId() )
                .setBlobIds( uuidArray);
        jobCreator.createGetOrVerifyJob(params, jobId, JobRequestType.GET, JobChunkClientProcessingOrderGuarantee.NONE, jobEntries);
        Optional<JobEntry> blob5Entry = jobEntries.stream()
                .filter(entry -> b5.getId().equals(entry.getBlobId()))
                .findFirst();
        Optional<JobEntry> zeroLengthEntry = jobEntries.stream()
                .filter(entry -> b6.getId().equals(entry.getBlobId()))
                .findFirst();
        Optional<JobEntry> b1Entry = jobEntries.stream()
                .filter(entry -> b1.getId().equals(entry.getBlobId()))
                .findFirst();
        Optional<JobEntry> b2Entry = jobEntries.stream()
                .filter(entry -> b2.getId().equals(entry.getBlobId()))
                .findFirst();

        assertEquals(JobChunkBlobStoreState.COMPLETED, blob5Entry.get().getBlobStoreState());
        assertEquals(JobChunkBlobStoreState.COMPLETED, zeroLengthEntry.get().getBlobStoreState());
        assertTrue(blob5Entry.get().getChunkNumber() > zeroLengthEntry.get().getChunkNumber());
        assertEquals(JobChunkBlobStoreState.PENDING, b1Entry.get().getBlobStoreState());
        assertNull( b1Entry.get().getReadFromPoolId());
        assertNotNull( b1Entry.get().getReadFromTapeId());
        assertEquals(JobChunkBlobStoreState.PENDING, b2Entry.get().getBlobStoreState());
        assertNull( b2Entry.get().getReadFromPoolId());
        assertNotNull( b2Entry.get().getReadFromTapeId());

    }

    @Test
    public void testChunkNumberWithInOrderGuarantee() {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "multiBlob1", -1 );
        final List<Blob> o1Blobs = mockDaoDriver.createBlobs( o1.getId(), 3, 10 );
        cacheManager.blobLoadedToCache( o1Blobs.getLast().getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "multiBlob2", -1 );
        final List<Blob> o2Blobs = mockDaoDriver.createBlobs( o2.getId(), 2, 10 );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "singleBlob", 1 );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );

        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "singleBlobInCache", 1 );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        cacheManager.blobLoadedToCache( b4.getId() );

        final S3Object o5 = mockDaoDriver.createObject( bucket.getId(), "zeroLength", 0 );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );

        mockDaoDriver.putBlobsOnTape(t1.getId(), o1Blobs.get(0), o1Blobs.get(1), o1Blobs.get(2), o2Blobs.get(0), o2Blobs.get(1), b3, b4, b5);

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ),
                TapePartition.QUIESCED );
        final List< JobEntry> jobEntries = new ArrayList<>();


        Map<UUID, Blob> blobMap = new HashMap<>();
        blobMap.put(o1Blobs.get(0).getId(), o1Blobs.get(0));
        blobMap.put(o1Blobs.get(1).getId(), o1Blobs.get(1));
        blobMap.put(o1Blobs.get(2).getId(), o1Blobs.get(2));
        blobMap.put(o2Blobs.get(0).getId(), o2Blobs.get(0));
        blobMap.put(o2Blobs.get(1).getId(), o2Blobs.get(1));
        blobMap.put(b3.getId(), b3);
        blobMap.put(b4.getId(), b4);
        blobMap.put(b5.getId(), b5);

        for (UUID blobId : blobMap.keySet()) {
            final JobEntry entry = BeanFactory.newBean(JobEntry.class).setBlobId(blobId);
            jobEntries.add( entry);
        }

        final JobCreator jobCreator = new JobCreatorImpl(
                cacheManager,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory(),
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                blobStores(),
                10,
                11L);
        assertEquals(10 * 1024L * 1024,  jobCreator.getPreferredBlobSizeInBytes(), "Should notta limited preferred chunk size by preferred blob size.");
        UUID jobId = UUID.randomUUID();
        BaseCreateJobParams<?> params = BeanFactory.newBean( CreateGetJobParams.class )
                .setName( "jobImplTest" )
                .setUserId( bucket.getUserId() )
                .setBlobIds( blobMap.keySet().toArray(new UUID[0]) );
        jobCreator.createGetOrVerifyJob(params, jobId, JobRequestType.GET, JobChunkClientProcessingOrderGuarantee.IN_ORDER, jobEntries);

        // Verify that sequential chunk numbers have blobs with ascending byte offsets
        jobEntries.sort(Comparator.comparingLong(JobEntry::getChunkNumber));

        long lastByteOffset = 0L;
        for (final JobEntry entry : jobEntries) {
            final Blob curBlob = blobMap.get(entry.getBlobId());
            assertNotNull(curBlob);
            assertTrue(curBlob.getByteOffset() >= lastByteOffset, "Blobs should be ordered by byte offset");
            lastByteOffset = curBlob.getByteOffset();
        }

        // Verify blob store states
        final Map<UUID, JobChunkBlobStoreState> expectedBlobStoreStates = new HashMap<>();
        expectedBlobStoreStates.put(o1Blobs.get(0).getId(), JobChunkBlobStoreState.PENDING);
        expectedBlobStoreStates.put(o1Blobs.get(1).getId(), JobChunkBlobStoreState.PENDING);
        expectedBlobStoreStates.put(o1Blobs.get(2).getId(), JobChunkBlobStoreState.COMPLETED);
        expectedBlobStoreStates.put(o2Blobs.get(0).getId(), JobChunkBlobStoreState.PENDING);
        expectedBlobStoreStates.put(o2Blobs.get(1).getId(), JobChunkBlobStoreState.PENDING);
        expectedBlobStoreStates.put(b3.getId(), JobChunkBlobStoreState.PENDING);
        expectedBlobStoreStates.put(b4.getId(), JobChunkBlobStoreState.COMPLETED);
        expectedBlobStoreStates.put(b5.getId(), JobChunkBlobStoreState.COMPLETED);

        jobEntries.forEach(entry -> assertEquals(expectedBlobStoreStates.get(entry.getBlobId()), entry.getBlobStoreState()));
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



    private void initializeBasicInfrastructure() {
        mockDaoDriver = new MockDaoDriver(dbSupport);
        bsm = dbSupport.getServiceManager();
        cacheManager = new MockDiskManager(dbSupport.getServiceManager());
    }
    private void initializeTapeEnvironment() {
        // Initialize Library
        library = BeanFactory.newBean(TapeLibrary.class)
                .setSerialNumber("sn")
                .setName("name")
                .setManagementUrl("url");
        dbSupport.getDataManager().createBean(library);

        // Initialize Partition
        partition = BeanFactory.newBean(TapePartition.class)
                .setName("myPartition")
                .setSerialNumber("a")
                .setLibraryId(library.getId())
                .setImportExportConfiguration(ImportExportConfiguration.values()[0]);
        dbSupport.getDataManager().createBean(partition);

        // Initialize Storage Domain and Member
        sd = mockDaoDriver.createStorageDomain("sd1");
        sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(),
                partition.getId(),
                TapeType.values()[0]);

        sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition.getId(), TapeType.values()[ 0 ] );

        // Initialize Tapes
        t1 = (Tape)BeanFactory.newBean(Tape.class)
                .setPartitionId(partition.getId())
                .setStorageDomainMemberId(sdm.getId())
                .setState(TapeState.NORMAL)
                .setType(TapeType.values()[0])
                .setBarCode("11")
                .setId(UUID.fromString("af2992c4-8a60-4842-b915-60f9f6cb4af2"));
        dbSupport.getDataManager().createBean(t1);

        t2 = (Tape)BeanFactory.newBean(Tape.class)
                .setPartitionId(partition.getId())
                .setStorageDomainMemberId(sdm.getId())
                .setState(TapeState.NORMAL)
                .setType(TapeType.values()[0])
                .setBarCode("22")
                .setId(UUID.fromString("211aad0a-a346-11e3-9368-002590c1177c"));
        dbSupport.getDataManager().createBean(t2);

        archivePool = getArchivePool();
        dbSupport.getDataManager().createBean( archivePool );

        enterprisePool = getEnterprisePool();
        dbSupport.getDataManager().createBean( enterprisePool );
    }



    private Pool getArchivePool() {
        return BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setType( PoolType.NEARLINE )
                .setAvailableCapacity( 10000  )
                .setUsedCapacity(  20000  )
                .setHealth( PoolHealth.OK )
                .setLastAccessed( new Date() )
                .setLastModified( new Date() )
                .setLastVerified( new Date() )
                .setMountpoint( "/foo" )
                .setName(  "foo" )
                .setPoweredOn( true )
                .setState( PoolState.NORMAL );
    }

    private Pool getEnterprisePool() {
        return BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setType( PoolType.ONLINE )
                .setAvailableCapacity( 10000 )
                .setUsedCapacity(  20000  )
                .setHealth( PoolHealth.OK )
                .setLastAccessed( new Date() )
                .setLastModified( new Date() )
                .setLastVerified( new Date() )
                .setMountpoint( "/foo1" )
                .setName(  "foo1" )
                .setPoweredOn( true )
                .setState( PoolState.NORMAL );
    }

    private Set< S3ObjectToCreate > getObjectsToCreate()
    {
        final Set< S3ObjectToCreate > retval = new HashSet<>();
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o1" ).setSizeInBytes( PREFERRED_BLOB_SIZE * 10 ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o2" ).setSizeInBytes( 1000 ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o3" ).setSizeInBytes( 5 * MAX_BLOB_SIZE - 1 ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o4" ).setSizeInBytes( 5 * MAX_BLOB_SIZE ) );
        retval.add( BeanFactory.newBean( S3ObjectToCreate.class )
                .setName( "o5" ).setSizeInBytes( 5 * MAX_BLOB_SIZE + 1 ) );
        return retval;
    }


    private MockDaoDriver mockDaoDriver;
    private BeansServiceManager bsm;
    private MockDiskManager cacheManager;
    private TapeLibrary library;
    private TapePartition partition;
    private StorageDomain sd;
    private StorageDomainMember sdm;
    private StorageDomain sd2;
    private StorageDomainMember sdm2;
    private Tape t1;
    private Tape t2;
    private Pool archivePool;
    private Pool enterprisePool;
    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
    public static void resetDb() { dbSupport.reset(); }


    @BeforeEach
    public void setUp() {
        initializeBasicInfrastructure();
        initializeTapeEnvironment();
    }


    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }
}
