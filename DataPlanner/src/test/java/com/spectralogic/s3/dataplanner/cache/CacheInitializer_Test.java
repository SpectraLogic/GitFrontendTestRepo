package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.platform.cache.CacheUtils;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class CacheInitializer_Test  {

    @Test
    public void testVerificationOfProperlyFormedFileWithValidMarkerButNoBlobMarksFilePendingDelete()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final File file = cacheFilesystemDriver.writeCacheFile( UUID.randomUUID(), 10 );
        new CacheInitializer(cacheFilesystemDriver.getFilesystem(), new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 10).init();
        final BlobCache bc = mockDaoDriver.attainOneAndOnly(BlobCache.class);
        assertEquals(CacheEntryState.PENDING_DELETE, bc.getState(), "Shoulda created entry pending delete.");
        file.deleteOnExit();
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testVerificationOfProperlyFormedFileForManagedBlobWithValidMarkerRetainsFile()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final S3Object o1 = mockDaoDriver.createObject(null, "o1");
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );
        final File file = cacheFilesystemDriver.writeCacheFile( b1.getId(), 10 );
        assertTrue(file.exists(), "File should exist.");
        new CacheInitializer(cacheFilesystemDriver.getFilesystem(), new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 10).init();
        assertTrue(file.exists(), "Shoulda retained file.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testVerificationOfMalformedFileDeletesFile() throws IOException
    {
        
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final File file = new File(
                cacheFilesystemDriver.getFilesystem().getPath() + Platform.FILE_SEPARATOR + "oops" );
        file.createNewFile();
        new CacheInitializer(cacheFilesystemDriver.getFilesystem(), new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 10).init();
        assertFalse(file.exists(), "Shoulda deleted malformed file.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testVerificationOfProperlyFormedValidMarkerWithoutFileDeletesValidMarker()
    {
        
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final File file = cacheFilesystemDriver.writeCacheFile( UUID.randomUUID(), 10 );
        final File validMarker = new File( file.getAbsolutePath() + ".v" );
        assertTrue(file.exists(), "File shoulda existed initially.");
        file.delete();
        new CacheInitializer(cacheFilesystemDriver.getFilesystem(), new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 10).init();
        assertFalse(validMarker.exists(), "Shoulda deleted valid marker.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testVerificationOfProperlyFormedFileWithoutValidMarkerDeletesFile()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final File file = cacheFilesystemDriver.writeCacheFile( UUID.randomUUID(), 10 );
        final File validMarker = new File( file.getAbsolutePath() + ".v" );
        assertTrue(validMarker.exists(), "Valid marker shoulda existed initially.");
        validMarker.delete();
        new CacheInitializer(cacheFilesystemDriver.getFilesystem(), new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 10).init();
        final BlobCache bc = mockDaoDriver.attainOneAndOnly(BlobCache.class);
        assertEquals(CacheEntryState.PENDING_DELETE, bc.getState(), "Shoulda created entry pending delete.");
        final Object expected = file.length();
        assertEquals(expected, bc.getSizeInBytes(), "Entry shoulda been size of file.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testPhysicalCacheReconcilesWhatWasInItUponInitialization() throws IOException
    {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1, 1024 * 1024 );
        final CacheFilesystem filesystem = cacheFilesystemDriver.getFilesystem();

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

        mockDaoDriver.createJobEntries(CollectionFactory.toSet( b1, b2, b3 ) );
        mockDaoDriver.getServiceManager().getUpdater(JobEntry.class).update(Require.nothing(), (entry) -> entry.setBlobStoreState( JobChunkBlobStoreState.PENDING ), JobEntry.BLOB_STORE_STATE);

        final UUID randomId = UUID.randomUUID();
        final UUID randomId2 = UUID.randomUUID();
        new CacheInitializer(cacheFilesystemDriver.getFilesystem(), new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 0).init();
        cacheFilesystemDriver.writeCacheFile( b1.getId(), 100 );
        cacheFilesystemDriver.writeCacheFile( b2.getId(), 150 );
        cacheFilesystemDriver.writeCacheFile( b3.getId(), 50, false );
        cacheFilesystemDriver.writeCacheFile( randomId, 50 );
        cacheFilesystemDriver.writeCacheFile( randomId2, 60 );

        final File file1 = new File( CacheUtils.getPath( filesystem, b1.getId() ) );
        final File file2 = new File( CacheUtils.getPath( filesystem, b2.getId() ) );
        final File file3 = new File( CacheUtils.getPath( filesystem, b3.getId() ) );
        final File file4 = new File( CacheUtils.getPath( filesystem, randomId ) );
        final File file5 = new File( CacheUtils.getPath( filesystem, randomId2 ) );
        file5.delete();
        final File validFile5 = new File( CacheUtils.getPath( filesystem, randomId2 ) + ".v" );

        final File file6 = new File( CacheUtils.getPath( filesystem, randomId ).replace(randomId.toString(), "asdf.asdf") );
        cacheFilesystemDriver.writeCacheFileInternal(file6, 50, false);

        assertTrue(file1.exists(), "Shoulda written file");
        assertTrue(file2.exists(), "Shoulda written file");
        assertTrue(file3.exists(), "Shoulda written file");
        assertTrue(file4.exists(), "Shoulda written file");
        assertFalse(file5.exists(), "Shoulda deleted file");
        assertTrue(validFile5.exists(), "Shoulda written file");
        assertTrue(file6.exists(), "Shoulda written file");

        mockDaoDriver.updateBean(filesystem.setNeedsReconcile(true), CacheFilesystem.NEEDS_RECONCILE);

        //NOTE: clear this table to simulate a first-time startup after upgrading to use BlobCaches
        mockDaoDriver.deleteAll(BlobCache.class);

        final long usedCapacity = new CacheInitializer(cacheFilesystemDriver.getFilesystem(), new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 1).init();

        assertTrue(file1.exists(), "Cache shoulda imported valid cache file that is part of a job.");
        assertTrue(file2.exists(), "Cache shoulda imported valid cache file that is part of a job.");
        //NOTE: this will be deleted by CacheManagerImpl on initialization
        assertEquals(CacheEntryState.ALLOCATED, dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b3.getId()).getState(), "Shoulda created allocated entry for incomplete cache file that is part of job.");
        assertEquals(CacheEntryState.PENDING_DELETE, dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(null).getState(), "Shoulda created a cache entry pending delete for blob that doesn't exist");
        final Object actual = dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(null).getPath();
        assertEquals(file4.getPath(), actual, "Shoulda created a cache entry pending delete for blob that doesn't exist");
        assertFalse(validFile5.exists(), "Shoulda deleted orphaned .v file synchronously");
        assertFalse(file6.exists(), "Shoulda deleted non-cache file synchronously");
        assertEquals(354,  usedCapacity, "Shoulda updated used capacity to match reconciled state.");

        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testPhysicalCacheInitializesCorrectlyWhenNoReconcileNeeded() throws IOException
    {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1, 1024 * 1024 );
        final CacheFilesystem filesystem = cacheFilesystemDriver.getFilesystem();

        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );


        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 100 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o1.getId() );

        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 150 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o2.getId() );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 50 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o3.getId() );

        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "o4", 50 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                Blob.OBJECT_ID, o4.getId() );

        final File file1 = cacheFilesystemDriver.writeCacheFile( b1.getId(), 100 );
        final File file2 = cacheFilesystemDriver.writeCacheFile( b2.getId(), 150 );
        final File file3 = cacheFilesystemDriver.writeCacheFile( b3.getId(), 50, false );
        long usedCapacity = new CacheInitializer(filesystem, new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 1).init();
        final File file4 = cacheFilesystemDriver.writeCacheFile( b4.getId(), 50, false );

        assertEquals(303,  usedCapacity, "Shoulda updated used capacity from database.");

        assertNotNull(dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b1.getId()), "Shoulda reconciled b1");
        assertEquals(CacheEntryState.IN_CACHE, dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b1.getId()).getState(), "Shoulda marked b1 as in cache");
        assertNotNull(dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b2.getId()), "Shoulda reconciled b2");
        assertEquals(CacheEntryState.IN_CACHE, dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b2.getId()).getState(), "Shoulda marked b2 as in cache");
        assertEquals(CacheEntryState.ALLOCATED, dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b3.getId()).getState(), "Shoulda marked b3 as allocated but not in cache");
        assertNull(dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b4.getId()), "Should not have discovered b4");

        mockDaoDriver.updateBean(filesystem.setNeedsReconcile(false), CacheFilesystem.NEEDS_RECONCILE);

        usedCapacity = new CacheInitializer(filesystem, new MockTierExistingCacheImpl(), dbSupport.getServiceManager(), 1).init();

        assertNotNull(dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b1.getId()), "Shoulda still had b1");
        assertNotNull(dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b2.getId()), "Shoulda still had b2");
        assertTrue(file3.exists(), "File3 should not have been deleted (will be deleted asynchronously),");
        assertEquals(CacheEntryState.ALLOCATED, dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b3.getId()).getState(), "b3 should still be allocated but not in cache");

        assertNull(dbSupport.getServiceManager().getService(BlobCacheService.class).retrieveByBlobId(b4.getId()), "Should not have discovered b4 because should not have attempted reconcile");
        assertTrue(file4.exists(), "File4 should not have been discovered by cache manager not needing reconcile,");

        assertEquals(303, usedCapacity, "Shoulda updated used capacity from database.");
        cacheFilesystemDriver.shutdown();
    }

    private static DatabaseSupport dbSupport;

    @BeforeAll
    public static void setUpDB() {
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
