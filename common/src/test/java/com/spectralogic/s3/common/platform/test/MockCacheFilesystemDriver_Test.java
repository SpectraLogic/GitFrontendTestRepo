/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.test;

import java.io.File;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.cache.CacheUtils;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class MockCacheFilesystemDriver_Test 
{
    @Test
    public void testSimplestConstructorCreatesCacheFilesystemThatCanBeWrittenTo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockCacheFilesystemDriver driver = new MockCacheFilesystemDriver( dbSupport );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.attain( driver.getFilesystem() );
        mockDaoDriver.attainOneAndOnly( CacheFilesystem.class );

        final UUID blobId = UUID.randomUUID();
        final File file = new File( CacheUtils.getPath( driver.getFilesystem(), blobId ) );
        final File validFile = new File(
                file.getAbsolutePath() + CacheUtils.CACHE_FILE_VALID_SUFFIX );
        assertFalse(
                file.exists(),
                "File should notta existed initially."
                 );
        assertFalse(
                validFile.exists(),
                "File should notta existed initially."
                 );
        driver.writeCacheFile( blobId, 11 );
        assertTrue(file.exists(), "File shoulda been written.");
        assertEquals(11,  file.length(), "File shoulda been written.");
        assertTrue(validFile.exists(), "File shoulda been written.");

        driver.writeCacheFile( blobId, 5000 );
        assertTrue(file.exists(), "File shoulda been written.");
        assertEquals(5000,  file.length(), "File shoulda been written.");
        assertTrue(validFile.exists(), "File shoulda been written.");

        driver.shutdown();

        assertTrue(driver.isShutdown());

        assertFalse(
                file.exists(),
                "Shoulda cleaned up all files."
                 );
        assertFalse(
                validFile.exists(),
                "Shoulda cleaned up all files."
                 );
    }
}
