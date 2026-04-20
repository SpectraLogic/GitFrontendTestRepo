package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;


import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CacheSpaceReclaimer_Test  {

    @Test
    public void testReclaimerSucceedsWhenCacheEmpty() {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final CacheSpaceReclaimer cacheSpaceReclaimer = new CacheSpaceReclaimerImpl(dbSupport.getServiceManager().getService(BlobCacheService.class), new MockAsyncBlobCacheDeleter(dbSupport), 0);
        final Object expected1 = cacheSpaceReclaimer.run(0);
        assertEquals(expected1,  0L );
        final Object expected = cacheSpaceReclaimer.run(1000);
        assertEquals(expected,  0L);
    }

    @Test
    public void testLockedBlobsNotReclaimed() {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockAsyncBlobCacheDeleter deleter = new MockAsyncBlobCacheDeleter(dbSupport);
        final CacheSpaceReclaimer cacheSpaceReclaimer = new CacheSpaceReclaimerImpl(dbSupport.getServiceManager().getService(BlobCacheService.class), deleter, 0);
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 50 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 30 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", 20 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() ); //not in cache

        mockDaoDriver.markBlobsInCache( CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );

        final UUID lockHolder = UUID.randomUUID();
        cacheSpaceReclaimer.lockBlobs( lockHolder, CollectionFactory.toSet( b1.getId(), b2.getId(), b3.getId() ) );
        long reclaimed = cacheSpaceReclaimer.run(Long.MAX_VALUE);
        final Object actual2 = deleter.getBytesPendingDelete();
        assertEquals( 0, Integer.valueOf(actual2.toString()), "Should have reclaimed nothing");
        assertEquals( 0, reclaimed, "Should have reclaimed nothing");
        deleter.waitUntilCurrentDeletionsDone();
        cacheSpaceReclaimer.unlockBlobs( lockHolder );

        cacheSpaceReclaimer.lockBlobs( lockHolder, CollectionFactory.toSet( b1.getId(), b2.getId() ) );
        reclaimed = cacheSpaceReclaimer.run(Long.MAX_VALUE);
        final Object actual1 = deleter.getBytesPendingDelete();
        assertEquals( 30, Integer.valueOf(actual1.toString()), "Should have reclaimed only b3");
        assertEquals( 30, reclaimed, "Should have reclaimed only b3");
        deleter.waitUntilCurrentDeletionsDone();
        cacheSpaceReclaimer.unlockBlobs( lockHolder );

        cacheSpaceReclaimer.lockBlobs( lockHolder, CollectionFactory.toSet( b4.getId() ) );
        reclaimed = cacheSpaceReclaimer.run(Long.MAX_VALUE);
        final Object actual = deleter.getBytesPendingDelete();
        assertEquals(150, Integer.valueOf(actual.toString()), "Should have reclaimed b1 and b2");
        assertEquals( 150,  reclaimed, "Should have reclaimed b1 and b2");
        deleter.waitUntilCurrentDeletionsDone();
        cacheSpaceReclaimer.unlockBlobs( lockHolder );
    }
    private static DatabaseSupport dbSupport ;
    @BeforeAll
    public static void setUpDB() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
