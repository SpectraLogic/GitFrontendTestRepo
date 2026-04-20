/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.testfrmwrk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import com.spectralogic.util.shutdown.ShutdownListener;
import org.apache.commons.io.FileUtils;

import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.common.platform.cache.CacheUtils;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class MockCacheFilesystemDriver extends BaseShutdownable
{
    public MockCacheFilesystemDriver( final BeansServiceManager bsm, final CacheFilesystem filesystem )
    {
        m_filesystem = bsm.getRetriever( CacheFilesystem.class ).attain( filesystem.getId() );
        try
        {
            FileUtils.cleanDirectory( new File( m_filesystem.getPath() ) );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        addShutdownListener( new CleanupFilesystemShutdownListener() );
    }
    
    
    private final class CleanupFilesystemShutdownListener implements ShutdownListener
    {
        @Override
        public void shutdownOccurred()
        {
            try
            {
                FileUtils.deleteDirectory( new File( m_filesystem.getPath() ) );
            }
            catch ( final IOException ex )
            {
                throw new RuntimeException( ex );
            }
        }

        @Override
        public boolean isShutdownListenerNotificationCritical() {
            return false;
        }
    } // end inner class def


    public MockCacheFilesystemDriver( final DatabaseSupport dbSupport )
    {
        this( dbSupport, 1, 500 * 1024 * 1024 );
    }
    
    
    public MockCacheFilesystemDriver( final DatabaseSupport dbSupport, final long capacity )
    {
        this( dbSupport, 1, capacity );
    }
    
    
    public MockCacheFilesystemDriver(
            final DatabaseSupport dbSupport, 
            final int filesystemNumber, 
            final long capacity )
    {
        this( dbSupport.getServiceManager(),
              createDefaultFilesystem( dbSupport, filesystemNumber, capacity ) );
    }
    
    
    private static CacheFilesystem createDefaultFilesystem( 
            final DatabaseSupport dbSupport,
            final int filesystemNumber,
            final long capacity )
    {
        final String path;
        try
        {
            final String prefix = MockCacheFilesystemDriver.class.getSimpleName() + filesystemNumber;
            path = Files.createTempDirectory( prefix ).toString();
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        
        final CacheFilesystem filesystem = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( path ).setMaxCapacityInBytes( Long.valueOf( capacity ) )
                .setNodeId( getNodeId( dbSupport.getServiceManager() ) );
        dbSupport.getDataManager().createBean( filesystem );
        return filesystem;
    }
    
    
    private static UUID getNodeId( final BeansServiceManager bsm )
    {
        return bsm.getService( NodeService.class ).getThisNode().getId();
    }
    
    
    public CacheFilesystem getFilesystem()
    {
        final CacheFilesystem retval = BeanFactory.newBean( CacheFilesystem.class );
        BeanCopier.copy( retval, m_filesystem );
        return retval;
    }
    
    
    public File writeCacheFile( final UUID blobId, final long size )
    {
        return writeCacheFile( blobId, size, true );
    }


    public File writeCacheFile( final UUID blobId, final long size, boolean writeValidMarker )
    {
        final File file = new File( CacheUtils.getPath( m_filesystem, blobId ) );
        writeCacheFileInternal( file, size, writeValidMarker );
        return file;
    }
    
    
    public void writeCacheFileInternal( final File file, final long size, final boolean writeValidMarker )
    {
        try
        {
            Path pathToFile = Paths.get( file.getPath() );
            Files.createDirectories( pathToFile.getParent() );
            Files.deleteIfExists( pathToFile );
    
            final FileOutputStream writer = new FileOutputStream( file );
            long remaining = size;
            while ( 0 < remaining )
            {
                final int bytesToWrite = ( 4096 > remaining ) ? (int)remaining : 4096;
                remaining -= bytesToWrite;
                final byte [] blockOfData = new byte[ bytesToWrite ];
                writer.write( blockOfData );
            }
            writer.close();
            
            if ( writeValidMarker )
            {
                final File validationFile = new File(
                        file.getAbsolutePath() + CacheUtils.CACHE_FILE_VALID_SUFFIX );
                validationFile.createNewFile();
                validationFile.deleteOnExit();
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        
        file.deleteOnExit();
    }
    
    
    private final CacheFilesystem m_filesystem;
}
