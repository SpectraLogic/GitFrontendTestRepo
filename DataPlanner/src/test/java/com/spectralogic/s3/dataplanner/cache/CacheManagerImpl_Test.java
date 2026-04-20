/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.cache;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class CacheManagerImpl_Test
{

    @Test
    public void testAllocateMoreCacheThanAvailableDueToDataThrowsDataplannerException()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "testBucket" );

        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1048576 );
        assertEquals(cacheFilesystemDriver.getFilesystem().getMaxCapacityInBytes(), Long.valueOf( 1048576 ), "Failed to constrain the available cache in MockCacheFilesystemDriver.");

        final CacheManager cacheManager = new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.values()[ 0 ] )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );
        final DataManager transaction =
                dbSupport.getDataManager().startTransaction();
        final S3Object s3obj = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket.getId() ).setName( String.valueOf( "testObjectBean" ) )
                .setType( S3ObjectType.values()[ 0 ] );
        transaction.createBean( s3obj );

        final Blob blob = BeanFactory.newBean( Blob.class )
                .setObjectId( s3obj.getId() ).setByteOffset( 0 ).setLength( 1073741824 );
        transaction.createBean( blob );
        final UUID blobId = blob.getId();

        transaction.createBean( BeanFactory.newBean( JobEntry.class)
                .setChunkNumber( 0 )
                .setBlobId( blob.getId() )
                .setJobId( job.getId() ) );
        transaction.commitTransaction();

        TestUtil.assertThrows(
                "Should have thrown DataPlannerException when trying to allocate more cache than available.",
                DataPlannerException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        cacheManager.allocateChunksForBlob( blobId );
                    }
                } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateMoreCacheThanAvailableDueToFilesystemOverheadThrowsDataplannerException()
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "testBucket" );

        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, Integer.MAX_VALUE );
        Object a = cacheFilesystemDriver.getFilesystem().getMaxCapacityInBytes();
        assertEquals(a, Long.valueOf( Integer.MAX_VALUE ), "Failed to constrain the available cache in MockCacheFilesystemDriver.");

        final CacheManager cacheManager = new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() , Integer.MAX_VALUE );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.values()[ 0 ] )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );
        final DataManager transaction =
                dbSupport.getDataManager().startTransaction();
        final S3Object s3obj1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket.getId() ).setName( String.valueOf( "testObjectBean1" ) )
                .setType( S3ObjectType.values()[ 0 ] );
        transaction.createBean( s3obj1 );
        final S3Object s3obj2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket.getId() ).setName( String.valueOf( "testObjectBean2" ) )
                .setType( S3ObjectType.values()[ 0 ] );
        transaction.createBean( s3obj2 );

        final Blob blob1 = BeanFactory.newBean( Blob.class )
                .setObjectId( s3obj1.getId() ).setByteOffset( 0 ).setLength( 1 );
        transaction.createBean( blob1 );

        transaction.createBean( BeanFactory.newBean( JobEntry.class)
                .setChunkNumber( 0 )
                .setBlobId( blob1.getId() )
                .setJobId( job.getId() ) );
        transaction.commitTransaction();

        TestUtil.assertThrows(
                "Should have thrown DataPlannerException when trying to allocate more cache than available.",
                DataPlannerException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        cacheManager.allocateChunksForBlob( blob1.getId() );
                    }
                } );


        dbSupport.getDataManager().updateBeans(
                CollectionFactory.toSet( Blob.LENGTH ), blob1.setLength( 0 ), Require.nothing() );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        assertEquals(0, cacheManager.getSoonAvailableCapacityInBytes(), "Shoulda reported all capacity used.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetCacheStateDoesNotBlowUp()
    {
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final CacheManager cacheManager = new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() );
        cacheManager.getCacheState( false );
        cacheManager.getCacheState( true );

        cacheManager.getCacheState( false );
        cacheManager.getCacheState( true );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.values()[ 0 ] )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        cacheManager.getCacheState( false );
        cacheManager.getCacheState( true );

        final int numberOfObjects = 20;
        UUID blobId = null;
        final List< S3Object > objects = new ArrayList<>();
        final DataManager transaction =
                dbSupport.getDataManager().startTransaction();
        for ( int i = 0; i < numberOfObjects; ++i )
        {
            final S3Object o = BeanFactory.newBean( S3Object.class )
                    .setBucketId( bucket.getId() ).setName( String.valueOf( i ) )
                    .setType( S3ObjectType.values()[ 0 ] );
            transaction.createBean( o );
            objects.add( o );

            final Blob blob = BeanFactory.newBean( Blob.class )
                    .setObjectId( o.getId() ).setByteOffset( 0 ).setLength( 1 );
            transaction.createBean( blob );
            blobId = blob.getId();

            transaction.createBean( BeanFactory.newBean( JobEntry.class)
                    .setChunkNumber( i )
                    .setBlobId( blob.getId() )
                    .setJobId( job.getId() ) );
        }
        transaction.commitTransaction();

        for ( int i = 0; i < numberOfObjects; ++i )
        {
            cacheManager.allocateChunksForBlob( blobId );
        }

        final Duration duration = new Duration();
        cacheManager.getCacheState( false );
        cacheManager.getCacheState( true );
        LOG.info( "It took " + duration + " to generate the cache state for "
                  + numberOfObjects + " objects." );
    }

    @Test
    public void throttleCacheAllocation_Test()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final Bucket b1 = mockDaoDriver.createBucket(null, "a");
        final Bucket b2 = mockDaoDriver.createBucket(null, "b");

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver(dbSupport, 100);
        final CacheManagerImpl cacheManager = createCacheManager(dbSupport, 0);

        // With no rules allocating the entire cache should be allowed
        cacheManager.throttleCacheAllocation(JobRequestType.GET, BlobStoreTaskPriority.NORMAL, b1.getId(), 100);

        // Add rule: Max 50% of cache for bucket b1 (50 bytes)
        final CacheThrottleRule b1Rule = BeanFactory.newBean(CacheThrottleRule.class).setBucketId(b1.getId()).setMaxCachePercent(0.5);
        mockDaoDriver.create(b1Rule);

        // Allocating 51 bytes should fail.
        TestUtil.assertThrows(
                "Should have throttled",
                DataPlannerException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        cacheManager.throttleCacheAllocation(JobRequestType.GET, BlobStoreTaskPriority.NORMAL, b1.getId(), 51);
                    }
                } );

        final Job b1Job = mockDaoDriver.createJob(b1.getId(), null, JobRequestType.GET);
        final Job b2Job = mockDaoDriver.createJob(b2.getId(), null, JobRequestType.GET);

        // Allocating 40 bytes to b1 should pass.
        final S3Object obj1 = mockDaoDriver.createObject(b1.getId(), "o1", 40);
        final Blob blob1 = mockDaoDriver.getBlobFor(obj1.getId());
        mockDaoDriver.createJobEntry(b1Job.getId(), blob1);
        cacheManager.allocateChunksForBlob(blob1.getId());

        // Allocating 10 more bytes to b1 should pass.
        final S3Object obj2 = mockDaoDriver.createObject(b1.getId(), "o2", 10);
        final Blob blob2 = mockDaoDriver.getBlobFor(obj2.getId());
        mockDaoDriver.createJobEntry(b1Job.getId(), blob2);
        cacheManager.allocateChunksForBlob(blob2.getId());

        // Allocating 1 more byte to b1 should fail.
        TestUtil.assertThrows(
                "Should have throttled",
                DataPlannerException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        cacheManager.throttleCacheAllocation(JobRequestType.GET, BlobStoreTaskPriority.NORMAL, b1.getId(), 1);
                    }
                } );

        // Bucket b2 should be unaffected by the allocation rule.
        // With 50 bytes consumed by b1, 50 bytes remain in the 100-byte cache.
        final S3Object obj3 = mockDaoDriver.createObject(b2.getId(), "o3", 40);
        final Blob blob3 = mockDaoDriver.getBlobFor(obj3.getId());
        mockDaoDriver.createJobEntry(b2Job.getId(), blob3);
        cacheManager.allocateChunksForBlob(blob3.getId());

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void allocateChunksForBlobRespectsCacheThrottleRule_Test()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final long totalCacheSize = 1000;
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver(dbSupport, totalCacheSize);
        // Use 0 overhead for easier byte calculation
        final CacheManagerImpl cacheManager = createCacheManager(dbSupport, 0);

        final Bucket bucket = mockDaoDriver.createBucket(null, "b1");

        // Create a rule: Max 50% capacity for this bucket. (500 bytes)
        final CacheThrottleRule rule = BeanFactory.newBean(CacheThrottleRule.class)
                .setBucketId(bucket.getId())
                .setMaxCachePercent(0.5);
        mockDaoDriver.create(rule);

        // Blob 1: 300 bytes.
        final S3Object obj1 = mockDaoDriver.createObject(bucket.getId(), "o1", 300);
        final Blob blob1 = mockDaoDriver.getBlobFor(obj1.getId());
        final Job job1 = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT);
        mockDaoDriver.createJobEntry(job1.getId(), blob1);

        cacheManager.allocateChunksForBlob(blob1.getId());
        assertTrue(cacheManager.isCacheSpaceAllocated(blob1.getId()));

        // Blob 2: 201 bytes. 300 + 201 = 501 > 500. Should fail.
        final S3Object obj2 = mockDaoDriver.createObject(bucket.getId(), "o2", 201);
        final Blob blob2 = mockDaoDriver.getBlobFor(obj2.getId());
        final Job job2 = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT);
        mockDaoDriver.createJobEntry(job2.getId(), blob2);

        TestUtil.assertThrows(
                "Should have throttled allocation exceeding 50%",
                DataPlannerException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        cacheManager.allocateChunksForBlob(blob2.getId());
                    }
                } );

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateCacheIsNotThrottledWhenCacheBurstEnabled() throws InterruptedException
    {
        final S3Object [] objects = new S3Object[ 30 ];
        final Blob [] blobs = new Blob[ objects.length ];
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        for ( int i = 0; i < objects.length; ++i )
        {
            objects[ i ] = mockDaoDriver.createObject( null, "o" + i );
            blobs[ i ] = mockDaoDriver.getBlobFor( objects[ i ].getId() );
        }

        UUID lastJobId = null;
        final JobEntry[] chunks = new JobEntry[ objects.length ];
        for ( int i = 0; i < objects.length; ++i )
        {
            if ( i % 10 == 0 )
            {
                chunks[ i ] = mockDaoDriver.createJobWithEntry(
                        JobRequestType.PUT, blobs[ i ] );
            }
            else
            {
                chunks[ i ] = mockDaoDriver.createJobEntry(
                        lastJobId, blobs[ i ] );
            }
            lastJobId = chunks[ i ].getJobId();
        }
        dbSupport.getDataManager().updateBeans(
                CollectionFactory.toSet( JobObservable.PRIORITY ),
                BeanFactory.newBean( Job.class ).setPriority( BlobStoreTaskPriority.NORMAL ),
                Require.nothing() );

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final CacheManager cacheManager = new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() );
        final CountDownLatch latch1 = new CountDownLatch( 3 );
        for ( int jo = 0; jo < 30; jo += 10 )
        {
            final int jobOffset = jo;
            SystemWorkPool.getInstance().submit( new Runnable()
            {
                public void run()
                {
                    cacheManager.allocateChunksForBlob( blobs[ jobOffset + 0 ].getId() );
                    cacheManager.allocateChunksForBlob( blobs[ jobOffset + 1 ].getId() );
                    cacheManager.allocateChunksForBlob( blobs[ jobOffset + 2 ].getId() );
                    cacheManager.allocateChunksForBlob( blobs[ jobOffset + 3 ].getId() );
                    cacheManager.allocateChunksForBlob( blobs[ jobOffset + 4 ].getId() );
                    cacheManager.allocateChunksForBlob( blobs[ jobOffset + 5 ].getId() );
                    latch1.countDown();
                }
            } );
        }
        assertTrue(latch1.await( 10, TimeUnit.SECONDS ), "Shoulda succeeded.");
    }

    @Test
    public void testIsChunkEntirelyInCacheReturnsFalseIfNotEntirelyInCache()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );

        final CacheManager cacheManager = new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() );

        final S3Object o = mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.createObject( null, "o4" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( blob );

        mockDaoDriver.simulateObjectUploadCompletion( o.getId() );
        cacheManager.allocateChunk( chunk.getId() );
        assertFalse(cacheManager.isChunkEntirelyInCache( chunk.getId() ), "Shoulda reported not fully in cache due to blob not in cache.");

        cacheFilesystemDriver.writeCacheFile( blob.getId(), 10 );
        cacheManager.blobLoadedToCache( blob.getId() );
        assertTrue(cacheManager.isChunkEntirelyInCache( chunk.getId() ), "Shoulda reported fully in cache since it is.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testIsChunkEntirelyInCacheReturnsFalseIfNotEntirelyUploadedBlkp2810()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );

        final CacheManager cacheManager = new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() );

        final S3Object o = mockDaoDriver.createObjectStub( null, "o2", 10 );
        mockDaoDriver.createObject( null, "o4" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( blob );
        cacheManager.allocateChunk( chunk.getId() );
        cacheFilesystemDriver.writeCacheFile( blob.getId(), 10 );
        cacheManager.blobLoadedToCache( blob.getId() );
        assertFalse(cacheManager.isChunkEntirelyInCache( chunk.getId() ), "Shoulda reported not fully in cache due to missing checksum for blob.");

        mockDaoDriver.simulateObjectUploadCompletion( o.getId() );
        assertTrue(cacheManager.isChunkEntirelyInCache( chunk.getId() ), "Shoulda reported fully in cache since it is.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateEmptyIdsDoesNothing()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );
        allocateBlobs( mockDaoDriver, cache, new HashSet< UUID >() );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testCacheManagerDeletesAllEntriesNotInCacheOnInitialization()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );

        final BlobCacheService bcs = dbSupport.getServiceManager().getService( BlobCacheService.class );
        //create blobs and objects:
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 50 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 30 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );

        mockDaoDriver.allocateBlob( b1.getId() );
        final BlobCache bc2 = mockDaoDriver.markBlobInCache( b2.getId() );
        mockDaoDriver.markBlobInCache( b3.getId() );
        bcs.update( bc2.setState( CacheEntryState.PENDING_DELETE ), BlobCache.STATE );

        final MockAsyncBlobCacheDeleter fileDeleter = new MockAsyncBlobCacheDeleter( dbSupport );
        final CacheManager cacheManager = new CacheManagerImpl(dbSupport.getServiceManager(),
                fileDeleter,
                new MockCacheSpaceReclaimer(dbSupport, fileDeleter, 0),
                1,
                183);
        //NOTE: the mock deleter does not account for filesystem overhead, so we assert 150 instead of 152
        assertEquals(150, fileDeleter.getBytesPendingDelete());
        assertEquals(31, cacheManager.getUsedCapacityInBytes());

    }

    @Test
    public void testAllocateWhenNotEnoughSpaceEverNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 + 1 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        mockDaoDriver.createJobWithEntries( b1, b2 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
            }
        } );

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateWhenNotEnoughSpaceTemporarilyNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 1024 * 1024 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final JobEntry jobEntry = mockDaoDriver.createJobWithEntry( b1 );
        mockDaoDriver.createJobWithEntry( b2 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT, new BlastContainer()
        {
            public void test()
            {
                allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
            }
        } );

        mockDaoDriver.delete( JobEntry.class, jobEntry);
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateWhenCacheFilesystemShrunkButResultIsCachedIsAllowedToProceed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 10 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        mockDaoDriver.createJobWithEntries( b1, b2, b3 );

        cache.allocateChunksForBlob(b1.getId());
        cache.allocateChunksForBlob(b2.getId());
        cache.allocateChunksForBlob(b3.getId());
        assertTrue(cache.isCacheSpaceAllocated(b1.getId()));
        assertTrue(cache.isCacheSpaceAllocated(b2.getId()));
        assertTrue(cache.isCacheSpaceAllocated(b3.getId()));
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( CacheFilesystem.MAX_CAPACITY_IN_BYTES ),
                cacheFilesystemDriver.getFilesystem().setMaxCapacityInBytes( Long.valueOf( 150 ) ) );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b3.getId() ) );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateWhenCacheFilesystemShrunkNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 10 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        mockDaoDriver.createJobWithEntries( b1, b2, b3 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( CacheFilesystem.MAX_CAPACITY_IN_BYTES ),
                cacheFilesystemDriver.getFilesystem().setMaxCapacityInBytes( Long.valueOf( 150 ) ) );
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT, new BlastContainer()
        {
            public void test()
            {
                allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
            }
        } );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b3.getId() ) );

        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( CacheFilesystem.MAX_CAPACITY_IN_BYTES ),
                cacheFilesystemDriver.getFilesystem().setMaxCapacityInBytes( Long.valueOf( 250 ) ) );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateWhenCacheFilesystemWentAwayNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        mockDaoDriver.createJobWithEntries( b1, b2 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
        dbSupport.getDataManager().deleteBean(
                CacheFilesystem.class,
                cacheFilesystemDriver.getFilesystem().getId() );
        TestUtil.assertThrows( null, GenericFailure.NOT_FOUND, new BlastContainer()
        {
            public void test()
            {
                allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateWhenBlobIsTooLargeForCacheNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 2 * 1024 * 1024 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        mockDaoDriver.createJobWithEntries( b1 );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateOccursImmediatelyIfPossible()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockAsyncBlobCacheDeleter fileDeleter =
                new MockAsyncBlobCacheDeleter(dbSupport);
        final CacheManagerImpl cache = createCacheManager( dbSupport, fileDeleter );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 10 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        mockDaoDriver.createJobWithEntries( b1, b2, b3 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b3.getId() ) );
        assertEquals(0, fileDeleter.getWaitUntilDeletionsDoneCallCount(), "Should notta needed to quiesce deletes.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAllocateRequiresQuiesceIfBlobBeingAllocatedIsPendingDelete()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 299 );
        final MockAsyncBlobCacheDeleter fileDeleter =
                new MockAsyncBlobCacheDeleter(dbSupport);
        final MockCacheSpaceReclaimer reclaimer = new MockCacheSpaceReclaimer(dbSupport, fileDeleter, 0);
        final CacheManagerImpl cache = new CacheManagerImpl(dbSupport.getServiceManager(),
                fileDeleter,
                reclaimer,
                0,
                0);

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 99 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 101 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId() ) );
        assertEquals(0, fileDeleter.getWaitUntilDeletionsDoneCallCount(), "Should notta needed to quiesce deletes.");
        assertEquals(199, cache.getUsedCapacityInBytes(), "Shoulda retained b1 and b2.");

        assertTrue(reclaimer.neverAttemptedToReclaimMoreThan(0), "Shoulda not have attempted reclaim yet");
        mockDaoDriver.createJobWithEntries( b1 );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b3.getId() ) );
        assertTrue(reclaimer.attemptedToReclaim(1), "Shoulda attempted to reclaim at least 1 byte to fit o3");
        assertEquals(1, fileDeleter.getWaitUntilDeletionsDoneCallCount(), "Shoulda needed to quiesce deletes to reclaim space.");
        assertEquals(200, cache.getUsedCapacityInBytes(), "Shoulda retained b1 and b3.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testBlobLoadedToCacheNullBlobIdNotAllowed()
    {
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                cache.blobLoadedToCache( null );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testBlobLoadedToCacheBeforeCapacityAllocatedNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        mockDaoDriver.createJobWithEntries( b1 );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                cache.blobLoadedToCache( b1.getId() );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testBlobLoadedToCacheButFileDoesNotExistNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        mockDaoDriver.createJobWithEntries( b1 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                cache.blobLoadedToCache( b1.getId() );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testBlobLoadedToCacheButFileLoadedDoesNotMatchStatedSizeNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );

        File file = cacheFilesystemDriver.writeCacheFile( b1.getId(), 99 );
        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test()
            {
                cache.blobLoadedToCache( b1.getId() );
            }
        } );
        assertFalse(file.exists(), "File shoulda been whacked.");

        file = cacheFilesystemDriver.writeCacheFile( b1.getId(), 101 );
        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test()
            {
                cache.blobLoadedToCache( b1.getId() );
            }
        } );
        assertFalse(file.exists(), "File shoulda been whacked.");

        file = cacheFilesystemDriver.writeCacheFile( b1.getId(), 100 );
        cache.blobLoadedToCache( b1.getId() );
        assertTrue(file.exists(), "File shoulda been retained.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testBlobLoadedToCacheButFileLoadedSizeUpdatedAfterInitialAllocationAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver(dbSupport, 1024 * 1024);
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        final long originalSize = 100;
        final long unusedSpace = 2;
        final long actualSize = originalSize - unusedSpace;

        final S3Object o1 = mockDaoDriver.createObject(null, "o1", originalSize);
        final Blob b1 = dbSupport.getServiceManager().getRetriever(Blob.class).attain(
                Blob.OBJECT_ID, o1.getId());

        assertEquals(0, cache.getUsedCapacityInBytes());

        // Verify used capacity reflects original blobs ize
        allocateBlobs(mockDaoDriver, cache, CollectionFactory.toSet(b1.getId()));
        assertEquals(originalSize, cache.getUsedCapacityInBytes());
        File file = cacheFilesystemDriver.writeCacheFile(b1.getId(), actualSize);

        // Update blob's expected size
        dbSupport.getServiceManager().getService(BlobService.class).update(b1.setLength(actualSize), Blob.LENGTH);
        allocateBlobs(mockDaoDriver, cache, CollectionFactory.toSet(b1.getId()));
        cache.blobLoadedToCache(b1.getId());

        // Verify used capacity reflects the actual size of the blob
        assertEquals(actualSize, cache.getUsedCapacityInBytes());

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testContainsNullBlobIdNotAllowed()
    {
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                dbSupport.getServiceManager().getService(BlobCacheService.class).contains( null );
            }
        } );
    }


    @Test
    public void testDeleteInterleavedWithReclaimWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1000 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );
        final AsyncBlobCacheDeleter asyncBlobCacheDeleter = new MockAsyncBlobCacheDeleter(dbSupport);

        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final ArrayBlockingQueue< Blob > blobs =
                new ArrayBlockingQueue<>( 1000 );
        blobs.addAll( mockDaoDriver.createBlobs( o.getId(), 1000, 0, 1 ) );
        allocateBlobs( mockDaoDriver, cache, BeanUtils.< UUID >extractPropertyValues( blobs, Identifiable.ID ) );
        final BlobCacheService bcs = dbSupport.getServiceManager().getService( BlobCacheService.class );
        final Runnable deleter = new Runnable()
        {
            public void run()
            {
                final int delay = new SecureRandom().nextInt( 5 ) + 3;
                TestUtil.sleep( delay );
                while ( true )
                {
                    final Blob blob = blobs.poll();
                    if ( null == blob )
                    {
                        return;
                    }
                    asyncBlobCacheDeleter.waitUntilCurrentDeletionsDone();
                    final BlobCache bc = bcs.retrieveByBlobId(blob.getId());
                    bcs.update( bc.setState( CacheEntryState.PENDING_DELETE ), BlobCache.STATE );
                    asyncBlobCacheDeleter.delete(bc);
                }
            }
        };
        for ( int i = 0; i < 10; ++i )
        {
            SystemWorkPool.getInstance().submit( deleter );
        }

        asyncBlobCacheDeleter.waitUntilCurrentDeletionsDone();
        assertEquals(bcs.getSum(BlobCache.SIZE_IN_BYTES, Require.nothing()), cache.getUsedCapacityInBytes(), "Cache shoulda reported remaining size correctly");
        assertEquals(0, bcs.getSum(BlobCache.SIZE_IN_BYTES, Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)), "Should not have been anything left pending delete.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReallocateWhenReallocationSourceHasCacheFileAndDeleteFalseNotAllowed() throws InterruptedException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport, 1 );
        assertEquals(0, cache.getUsedCapacityInBytes(), "Shoulda been an empty cache using a single cache filesystem.");

        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.PUT )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 150 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 50 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId() ) );
        assertEquals(252, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        cacheFilesystemDriver.writeCacheFile( b2.getId(), 22 );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                cache.reallocate( b2.getId(), b3.getId(), false );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReallocateWhenReallocationSourceHasCacheFileAndDeleteTrueAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport, 1 );
        assertEquals(0, cache.getUsedCapacityInBytes(), "Shoulda been an empty cache using a single cache filesystem.");

        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.PUT )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 150 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 50 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId() ) );
        assertEquals(252, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        cacheFilesystemDriver.writeCacheFile( b2.getId(), 22 );

        cache.reallocate( b2.getId(), b3.getId(), true );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReallocateWhenReallocationSourceHasNotBeenAllocatedNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport, 1 );
        assertEquals(0, cache.getUsedCapacityInBytes(), "Shoulda been an empty cache using a single cache filesystem.");

        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.PUT )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 150 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 50 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );
        assertEquals(101, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                cache.reallocate( b2.getId(), b3.getId(), false );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReallocateWhenReallocationTargetHasAlreadyBeenAllocatedNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport, 1 );
        assertEquals(0, cache.getUsedCapacityInBytes(), "Shoulda been an empty cache using a single cache filesystem.");

        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.PUT )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 150 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 50 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId() ) );
        assertEquals(252, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b3.getId() ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                cache.reallocate( b2.getId(), b3.getId(), false );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReallocateWhenReallocationSourceSmallerThanTargetNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport, 1 );
        assertEquals(0, cache.getUsedCapacityInBytes(), "Shoulda been an empty cache using a single cache filesystem.");

        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.PUT )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 150 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );

        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "o4", 75 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o4.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
        assertEquals(151, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        cache.reallocate( b2.getId(), b1.getId(), false );
        assertEquals(152, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                cache.reallocate( b2.getId(), b4.getId(), false );
            }
        } );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReallocateValidRequestWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport, 1 );
        assertEquals(0, cache.getUsedCapacityInBytes(), "Shoulda been an empty cache using a single cache filesystem.");

        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.PUT )
                .setUserId( user.getId() );
        dbSupport.getDataManager().createBean( job );

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 150 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 50 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "o4", 75 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o4.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId() ) );
        assertEquals(252, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        cache.reallocate( b2.getId(), b3.getId(), false );
        assertEquals(253, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");

        cache.reallocate( b2.getId(), b4.getId(), false );
        assertEquals(254, cache.getUsedCapacityInBytes(), "Shoulda reported allocated cache space.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testPhysicalCacheReconciliationPerformance()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1, 1024 * 1024 );

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o", -1 );

        final Job job = BeanFactory.newBean( Job.class )
                .setBucketId( bucket.getId() ).setRequestType( JobRequestType.PUT )
                .setUserId( bucket.getUserId() );
        dbSupport.getDataManager().createBean( job );

        final CacheManagerImpl cache1 = createCacheManager( dbSupport );

        final int numberOfBlobs = 100;
        final List< Blob > blobs =
                mockDaoDriver.createBlobs( object.getId(), numberOfBlobs - 10, 2 );
        mockDaoDriver.createJobEntries( job.getId(), blobs );
        final List< Blob > inactiveBlobs =
                mockDaoDriver.createBlobs( object.getId(), 10, 2 * numberOfBlobs, 2 );

        allocateBlobs( mockDaoDriver, cache1, BeanUtils.toMap( blobs ).keySet() );
        allocateBlobs( mockDaoDriver, cache1, BeanUtils.toMap( inactiveBlobs ).keySet() );
        for ( final Blob blob : blobs )
        {
            cacheFilesystemDriver.writeCacheFile( blob.getId(), 2 );
            cache1.blobLoadedToCache( blob.getId() );
        }
        for ( final Blob blob : inactiveBlobs )
        {
            cacheFilesystemDriver.writeCacheFile( blob.getId(), 2 );
            cache1.blobLoadedToCache( blob.getId() );
        }

        final Duration duration = new Duration();
        createCacheManager( dbSupport );
        LOG.info( "Reconciling blobs with 90% retention rate at "
                + ( numberOfBlobs * 100 / duration.getElapsedMillis() )
                + " per second." );
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetCacheStateReturnsNonNull()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 10 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        mockDaoDriver.createJobWithEntries( b1, b2, b3 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetCacheSizeInBytes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        assertEquals(1024 * 1024, cache.getCacheSizeInBytes(), "cache size should match supplied mock value");
    }
    @Test
    public void testAutoReclaimDoesNotReclaimWhenCacheUtilizationTooLowToBeWorthIt()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 500 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        mockDaoDriver.createJobWithEntries( b1 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        TestUtil.sleep( 25 );
        assertEquals(300, cache.getUsedCapacityInBytes(), "Shoulda retained all blobs in cache.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testAutoReclaimDoesPartialReclaimWhenCacheUtilizationHighEnoughToBeWorthIt()
    {
        final long gb = 1024L * 1024 * 1024;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 305 * gb );
        final AsyncBlobCacheDeleter deleter = new MockAsyncBlobCacheDeleter(dbSupport);
        final MockCacheSpaceReclaimer reclaimer = new MockCacheSpaceReclaimer(dbSupport, deleter, 0);
        final CacheManagerImpl cache = new CacheManagerImpl(dbSupport.getServiceManager(),
                deleter,
                reclaimer,
                0,
                0);

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 * gb );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 * gb );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 * gb );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId() ) );
        TestUtil.sleep( 100 );
        assertTrue(reclaimer.neverAttemptedToReclaimMoreThan(0), "Should not have reclaimed");
        assertEquals(100 * gb, cache.getUsedCapacityInBytes(), "Should not have reclaimed blobs while still below threshold.");

        mockDaoDriver.createJobWithEntries( b1 );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );

        TestUtil.sleep( 100 );
        assertTrue(reclaimer.neverAttemptedToReclaimMoreThan(0), "Should not have reclaimed");
        assertEquals(200 * gb, cache.getUsedCapacityInBytes(), "Should not have reclaimed while still below threshold.");

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b3.getId() ) );

        int i = 20;
        while ( --i > 0 && 200 * gb != cache.getUsedCapacityInBytes() )
        {
            TestUtil.sleep( 100 );
        }
        assertTrue(reclaimer.reclaimAttempted(), "Should have reclaimed");
        assertEquals(200 * gb, cache.getUsedCapacityInBytes(), "Shoulda reclaimed blobs partially in cache.");

        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testAutoReclaimDoesFullReclaimWhenCacheUtilizationHighEnoughToBeWorthIt()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 320 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        mockDaoDriver.createJobWithEntries( b1 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b2.getId(), b3.getId() ) );
        TestUtil.sleep( 100 );
        assertEquals(200, cache.getUsedCapacityInBytes(), "Should not have reclaimed blobs that were part of a job during allocation.");

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId() ) );

        int i = 20;
        while ( --i > 0 && 100 != cache.getUsedCapacityInBytes() )
        {
            TestUtil.sleep( 100 );
        }
        assertEquals(100, cache.getUsedCapacityInBytes(), "Shoulda reclaimed all blobs possible.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReclaimUponAllocateNotPerformedIfNotNecessary()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 900 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", 301 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o4.getId() );
        mockDaoDriver.createJobWithEntries( b1, b4 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        TestUtil.sleep( 25 );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b4.getId() ) );
        TestUtil.sleep( 25 );
        assertEquals(601, cache.getUsedCapacityInBytes(), "Shoulda reclaimed all blobs possible in cache.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReclaimUponAllocatePerformedInFullIfNecessary()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 500 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", 301 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o4.getId() );
        mockDaoDriver.createJobWithEntries( b1, b4 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        TestUtil.sleep( 25 );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b4.getId() ) );
        TestUtil.sleep( 25 );
        assertEquals(401, cache.getUsedCapacityInBytes(), "Shoulda reclaimed all blobs possible in cache.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testForceFullReclaimNowDoesSo()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 5000 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", 301 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o4.getId() );
        mockDaoDriver.createJobWithEntries( b1, b4 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b4.getId() ) );
        TestUtil.sleep( 25 );
        assertEquals(601, cache.getUsedCapacityInBytes(), "Should notta reclaimed any blobs in cache.");

        cache.forceFullCacheReclaimNow();
        assertEquals(401, cache.getUsedCapacityInBytes(), "Shoulda reclaimed all eligible blobs in cache.");

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testReclaimReclaimsLeastRecentlyAccessedFirst()
    {
        final long gb = 1024L * 1024 * 1024;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 10000 * gb );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        Assertions.assertNotNull(cache.getCacheState( false ), "Shoulda generated something for cache state.");
        Assertions.assertNotNull(cache.getCacheState( true ), "Shoulda generated something for cache state.");

        final List< Blob > blobs = new ArrayList<>();
        for ( int i = 0; i < 50; ++i )
        {
            TestUtil.sleep( 5 );
            final S3Object o = mockDaoDriver.createObject( null, "o" + i, ( 0 == i ) ? 1 : 200 * gb );
            final Blob b = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                    Blob.OBJECT_ID, o.getId() );
            blobs.add( b );
            allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b.getId() ) );
            if ( 0 == i )
            {
                cacheFilesystemDriver.writeCacheFile( b.getId(), 1 );
                cache.blobLoadedToCache( b.getId() );
            }

            if ( 2 == i % 5 )
            {
                dbSupport.getServiceManager().getService(BlobCacheService.class).getFileIffInCache( blobs.get( 0 ).getId() );
                dbSupport.getServiceManager().getService(BlobCacheService.class).contains( blobs.get( 1 ).getId() );
            }
        }

        TestUtil.sleep( 50 );

        boolean inCache = false;
        for ( final Blob b : blobs )
        {
            if ( blobs.get( 0 ) == b )
            {
                assertTrue(cache.isInCache( b.getId() ), "Shoulda contained blob that was recently accessed.");
            }
            else if ( inCache )
            {
                assertTrue(cache.isInCache( b.getId() ), "Shoulda whacked least recently accessed blobs first.");
            }
            else
            {
                inCache = ( cache.isInCache( b.getId() ) );
            }
        }

        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetTotalCapacityWhenCapacityLimitedByPercentUtilizationReturnsPercentLimit()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheFilesystem filesystem = cacheFilesystemDriver.getFilesystem();
        mockDaoDriver.updateBean(
                filesystem
                        .setMaxCapacityInBytes( Long.valueOf( Long.MAX_VALUE ) )
                        .setMaxPercentUtilizationOfFilesystem( Double.valueOf( 0.01 ) ),
                CacheFilesystem.MAX_CAPACITY_IN_BYTES,
                CacheFilesystem.MAX_PERCENT_UTILIZATION_OF_FILESYSTEM );
        final CacheManagerImpl cache = createCacheManager( dbSupport );
        assertEquals((long)( 0.01 * new File( filesystem.getPath() ).getTotalSpace() ), cache.getTotalCapacityInBytes(), "Max shoulda been based on max percent.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetTotalCapacityWhenCapacityLimitedOnlyByPercentUtilizationReturnsPercentLimit()
    {
        //final DatabaseSupport dbSupport =
               // DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheFilesystem filesystem = cacheFilesystemDriver.getFilesystem();
        mockDaoDriver.updateBean(
                filesystem
                        .setMaxCapacityInBytes( null )
                        .setMaxPercentUtilizationOfFilesystem( Double.valueOf( 0.01 ) ),
                CacheFilesystem.MAX_CAPACITY_IN_BYTES,
                CacheFilesystem.MAX_PERCENT_UTILIZATION_OF_FILESYSTEM );
        final CacheManagerImpl cache = createCacheManager( dbSupport );
        assertEquals((long)( 0.01 * new File( filesystem.getPath() ).getTotalSpace() ), cache.getTotalCapacityInBytes(), "Max shoulda been based on max percent.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetTotalCapacityWhenCapacityLimitedByCapacityInBytesReturnsCapacityInBytes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheFilesystem filesystem = cacheFilesystemDriver.getFilesystem();
        mockDaoDriver.updateBean(
                filesystem
                        .setMaxCapacityInBytes( Long.valueOf( 66 ) )
                        .setMaxPercentUtilizationOfFilesystem( Double.valueOf( 1 ) ),
                CacheFilesystem.MAX_CAPACITY_IN_BYTES,
                CacheFilesystem.MAX_PERCENT_UTILIZATION_OF_FILESYSTEM );
        final CacheManagerImpl cache = createCacheManager( dbSupport );
        assertEquals(66, cache.getTotalCapacityInBytes(), "Max shoulda been based on max capacity in bytes.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetTotalCapacityWhenCapacityLimitedOnlyByCapacityInBytesReturnsCapacityInBytes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheFilesystem filesystem = cacheFilesystemDriver.getFilesystem();
        mockDaoDriver.updateBean(
                filesystem
                        .setMaxCapacityInBytes( Long.valueOf( 66 ) )
                        .setMaxPercentUtilizationOfFilesystem( null ),
                CacheFilesystem.MAX_CAPACITY_IN_BYTES,
                CacheFilesystem.MAX_PERCENT_UTILIZATION_OF_FILESYSTEM );
        final CacheManagerImpl cache = createCacheManager( dbSupport );
        assertEquals(66, cache.getTotalCapacityInBytes(), "Max shoulda been based on max capacity in bytes.");
        cacheFilesystemDriver.shutdown();
    }

    @Test
    public void testGetJobLockedCacheInBytes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final CacheManagerImpl cache = createCacheManager( dbSupport );

        assertEquals(0L, cache.getJobLockedCacheInBytes());

        // Blobs allocated in cache
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 100 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 1000 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        // Blob defined in job but not yet allocated in cache nor available in pool
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", 10000 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o4.getId() );

        // Blob in pool and associated with chunk, but not in cache
        final S3Object o5 = mockDaoDriver.createObject( null, "o5", 100000 );
        final Blob b5 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o5.getId() );

        final Job job1 = mockDaoDriver.createJob(null, null, JobRequestType.GET);
        final Job job2 = mockDaoDriver.createJob(null, null, JobRequestType.GET);
        final Set<JobEntry> entries = mockDaoDriver.createJobEntries( job1.getId(), b1, b2 );
        entries.addAll(mockDaoDriver.createJobEntries( job2.getId(), b2, b3 ));
        mockDaoDriver.createJobWithEntries( b3, b4 );
        final JobEntry poolChunk = mockDaoDriver.createJobWithEntry( b5 );

        allocateBlobs( mockDaoDriver, cache, CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );

        final UUID thisNodeId = dbSupport.getServiceManager().getService( NodeService.class ).getThisNode().getId();

        assertEquals( 1110L, cache.getJobLockedCacheInBytes() );

        cacheFilesystemDriver.shutdown();
    }

    private void allocateBlobs( final MockDaoDriver mockDaoDriver, final CacheManagerImpl cache, final Set< UUID > blobIds ) {
        //This function simulates blobs already being in cache because they were part of a previous job that no longer exists
        if (blobIds.isEmpty()) {
            return;
        }
        final Job job = mockDaoDriver.createJob(null, null, JobRequestType.GET);
        final Set<Blob> blobs = mockDaoDriver.getServiceManager().getService(BlobService.class).retrieveAll(blobIds).toSet();
        //create a job chunk
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blobs);
        for ( final Blob blob : blobs )
        {
            cache.allocateChunks( BeanUtils.toMap( chunks ).keySet() );
        }
        //clear job
        mockDaoDriver.delete(Job.class, job.getId());
        assertEquals(0, mockDaoDriver.getServiceManager().getService(JobEntryService.class).getCount(
                Require.beanPropertyEquals(
                        JobEntry.JOB_ID,
                        job.getId()
                )
        ), "shoulda deleted job entries");
    }


    private  CacheManagerImpl createCacheManager(final DatabaseSupport dbSupport) {
        final AsyncBlobCacheDeleter deleter = new MockAsyncBlobCacheDeleter(dbSupport);
        return new CacheManagerImpl(dbSupport.getServiceManager(),
                deleter,
                new MockCacheSpaceReclaimer(dbSupport, deleter, 0),
                0,
                0);
    }


    private  CacheManagerImpl createCacheManager(final DatabaseSupport dbSupport, long overhead) {
        final AsyncBlobCacheDeleter deleter = new MockAsyncBlobCacheDeleter(dbSupport);
        return new CacheManagerImpl(dbSupport.getServiceManager(),
                deleter,
                new MockCacheSpaceReclaimer(dbSupport, deleter, overhead),
                overhead,
                0);
    }


    private  CacheManagerImpl createCacheManager(final DatabaseSupport dbSupport, MockAsyncBlobCacheDeleter deleter) {
        return new CacheManagerImpl(dbSupport.getServiceManager(),
                deleter,
                new MockCacheSpaceReclaimer(dbSupport, deleter, 0),
                0,
                0);
    }


    private final static Logger LOG = Logger.getLogger( CacheManagerImpl_Test.class );
    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
    public static void resetDb() { dbSupport.reset(); }

    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(20, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }
}
