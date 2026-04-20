package com.spectralogic.s3.common.dao.service.planner;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public class BlobCacheServiceImpl_Test  {

    @Test
    public void testContainsReturnsTrueIffBlobIsBothAllocatedAndLoadedIntoCache()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final BlobCacheService cache = dbSupport.getServiceManager().getService( BlobCacheService.class );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        assertFalse(cache.contains( b1.getId() ), "Unallocated blob should notta been contained by cache.");
        final BlobCache bc = cache.allocate( b1.getId(), b1.getLength(), cacheFilesystemDriver.getFilesystem() );
        assertFalse(cache.contains( b1.getId() ), "Allocated blob not loaded yet should notta been contained by cache.");

        cacheFilesystemDriver.writeCacheFile( b1.getId(), 100 );
        cache.cacheEntryLoaded( bc, cacheFilesystemDriver.getFilesystem().getCacheSafetyEnabled() );
        assertTrue(cache.contains( b1.getId() ), "Loaded blob shoulda been contained by cache.");

        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testGetFileIffInCacheNullBlobIdNotAllowed()
    {
        final BlobCacheService cache = dbSupport.getServiceManager().getService( BlobCacheService.class );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new TestUtil.BlastContainer()
        {
            public void test()
            {
                cache.getFileIffInCache( null );
            }
        } );
    }


    @Test
    public void testGetFileIffInCacheReturnsTrueIffBlobIsBothAllocatedAndLoadedIntoCache()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final BlobCacheService cache = dbSupport.getServiceManager().getService( BlobCacheService.class );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        assertNull(cache.getFileIffInCache( b1.getId() ), "Unallocated blob should notta been contained by cache.");
        final BlobCache bc = cache.allocate( b1.getId(), b1.getLength(), cacheFilesystemDriver.getFilesystem() );
        assertNull(cache.getFileIffInCache( b1.getId() ), "Allocated blob not loaded yet should notta been contained by cache.");

        cacheFilesystemDriver.writeCacheFile( b1.getId(), 100 );
        cache.cacheEntryLoaded( bc, cacheFilesystemDriver.getFilesystem().getCacheSafetyEnabled() );
        assertNotNull(cache.getFileIffInCache( b1.getId() ), "Loaded blob shoulda been contained by cache.");

        cacheFilesystemDriver.shutdown();
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
