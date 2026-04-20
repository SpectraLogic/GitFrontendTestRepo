/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;


public final class AsyncBlobCacheDeleter_Test 
{
    @Test
    public void testBasicUsageWorks()
    {

        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );

        final UUID id1 = UUID.randomUUID();
        final UUID id2 = UUID.randomUUID();
        final UUID id3 = UUID.randomUUID();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        final File file2 = cacheFilesystemDriver.writeCacheFile( id2, 200 );
        final File file3 = cacheFilesystemDriver.writeCacheFile( id3, 300 );
        final BlobCache bc1 = BeanFactory.newBean(BlobCache.class).setPath(file1.getPath()).setSizeInBytes(file1.length()).setBlobId(id1);
        final BlobCache bc2 = BeanFactory.newBean(BlobCache.class).setPath(file2.getPath()).setSizeInBytes(file2.length()).setBlobId(id2);
        final BlobCache bc3 = BeanFactory.newBean(BlobCache.class).setPath(file3.getPath()).setSizeInBytes(file3.length()).setBlobId(id3);
        bc1.setId(UUID.randomUUID());
        bc2.setId(UUID.randomUUID());
        bc3.setId(UUID.randomUUID());
        
        final AsyncBlobCacheDeleter manager = new AsyncBlobCacheDeleterImpl(dbSupport.getServiceManager().getService(BlobCacheService.class), 10 );

        manager.delete(bc1);
        manager.delete(bc2);
        manager.waitUntilCurrentDeletionsDone();

        assertFalse(file1.exists(), "Shoulda deleted files.");
        assertFalse(file2.exists(), "Shoulda deleted files.");
        assertTrue(file3.exists(), "Should notta deleted file yet.");
        final Object actual1 = manager.getBytesPendingDelete();
        assertEquals( 0, Integer.valueOf( actual1.toString()), "Shoulda noted no unavailable space after deleted quiesced.");

        manager.delete(bc3);
        manager.waitUntilCurrentDeletionsDone();

        assertFalse(file1.exists(), "Shoulda deleted files.");
        assertFalse(file2.exists(), "Shoulda deleted files.");
        assertFalse(file3.exists(), "Shoulda deleted files.");
        final Object actual = manager.getBytesPendingDelete();
        assertEquals( 0, Integer.valueOf(actual.toString()), "Shoulda noted no unavailable space after deleted quiesced.");
    }
    
    
    @Test
    public void testConcurrentUsageWorks() throws InterruptedException
    {
        
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final BlobCacheService blobCacheService = dbSupport.getServiceManager().getService(BlobCacheService.class);
        final int numberOfProducers = 4;
        final int numberOfFilesPerProducer = 200;
        final List< Set< BlobCache > > files = new ArrayList<>();
        for ( int i = 0; i < numberOfProducers; ++i )
        {
            final Set< BlobCache > producerFiles = new HashSet<>();
            for ( int j = 0; j < numberOfFilesPerProducer; ++j )
            {
                producerFiles.add( createFile(blobCacheService) );
            }
            files.add( producerFiles );
        }

        final AsyncBlobCacheDeleter manager = new AsyncBlobCacheDeleterImpl(blobCacheService, 10);
        final Duration duration = new Duration();
        final Set< Producer > producers = new HashSet<>();
        for ( final Set< BlobCache > producerFiles : files )
        {
            final Producer producer = new Producer( manager, producerFiles, blobCacheService);
            SystemWorkPool.getInstance().submit( producer );
            producers.add( producer );
        }
        
        manager.waitUntilCurrentDeletionsDone();
        boolean somebodyReportedUnavailableSpace = false;
        boolean somebodyReportedPendingDelete = false;
        for ( final Producer producer : producers )
        {
            producer.m_latch.await();
            somebodyReportedUnavailableSpace =
                    somebodyReportedUnavailableSpace || producer.m_hadUnavailableSpace;
            somebodyReportedPendingDelete =
                    somebodyReportedPendingDelete || producer.m_hadPendingDelete;
        }
        manager.waitUntilCurrentDeletionsDone();
        
        LOG.info( "Deleted files in " + duration + " at " 
                  + ( numberOfProducers * numberOfFilesPerProducer * 1000 ) / duration.getElapsedMillis()
                  + "/sec." );
        for ( final Set< BlobCache > producerFiles : files )
        {
            for ( final BlobCache f : producerFiles )
            {
                assertFalse(new File(f.getPath()).exists(), "Shoulda been deleted.");
                final Object actual = manager.getBytesPendingDelete();
                assertEquals( 0, Integer.valueOf(actual.toString()), "Should notta been any queued deletes by now.");
            }
        }
        int i = 0;
        while( 20 > i && 0 != manager.getBytesPendingDelete() )
        {
            TestUtil.sleep( 50 );
            ++i;
        }
        final Object actual = manager.getBytesPendingDelete();
        assertEquals( 0, Integer.valueOf(actual.toString()), "Shoulda noted no unavailable space after deleted quiesced.");
        assertTrue(somebodyReportedUnavailableSpace, "Some producer shoulda at some point reported unavailable space.");
        assertTrue(somebodyReportedPendingDelete, "Some producer shoulda at some point reported pending delete.");
    }
    
    
    private final static class Producer implements Runnable
    {
        private Producer( 
                final AsyncBlobCacheDeleter manager,
                final Set< BlobCache > filesToDelete,
                final BlobCacheService blobCacheService )
        {
            m_filesToDelete = filesToDelete;
            m_manager = manager;
            m_blobCacheService = blobCacheService;
        }
        
        
        public void run()
        {
            for ( final BlobCache bc : m_filesToDelete )
            {
                m_manager.delete( bc );
                m_hadUnavailableSpace = ( m_manager.getBytesPendingDelete() > 0 ) || m_hadUnavailableSpace;
                m_hadPendingDelete = ( m_manager.getEntriesPendingDelete() > 0 ) || m_hadPendingDelete;
            }
            m_latch.countDown();
        }
        
        
        private volatile boolean m_hadUnavailableSpace;
        private volatile boolean m_hadPendingDelete;
        private final Set< BlobCache > m_filesToDelete;
        private final AsyncBlobCacheDeleter m_manager;
        private final BlobCacheService m_blobCacheService;
        private final CountDownLatch m_latch = new CountDownLatch( 1 );
    } // end inner class def
    
    
    private BlobCache createFile(BlobCacheService blobCacheService)
    {
        try
        {
            final File file = File.createTempFile(
                    getClass().getSimpleName(),
                    UUID.randomUUID().toString() );
            final BlobCache retval = BeanFactory.newBean(BlobCache.class).setPath(file.getPath()).setSizeInBytes(file.length()).setState(CacheEntryState.PENDING_DELETE).setLastAccessed(new Date(System.currentTimeMillis()));
            blobCacheService.create(retval);
            file.deleteOnExit();
            return retval;
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private final static Logger LOG = Logger.getLogger( AsyncBlobCacheDeleter_Test.class );
    private DatabaseSupport dbSupport;
    @BeforeEach
    public void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        dbSupport.reset();
    }
}
