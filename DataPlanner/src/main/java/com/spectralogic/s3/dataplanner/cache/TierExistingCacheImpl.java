/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.cache;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.cache.CacheUtils;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.attribute.PosixFilePermission.*;

public class TierExistingCacheImpl implements TierExistingCache
{
    
    
    public TierExistingCacheImpl( final String filesystem ) throws RuntimeException
    {
        m_filesystem = filesystem;
        m_cachePath = Paths.get( filesystem );
    }
    
    
    /*
     * Allow to be run manually, just in case.  Also useful for testing.
     */
    public static void main( String[] args ) throws IOException, InterruptedException
    {
        TierExistingCache tierExistingCache = new TierExistingCacheImpl( "/usr/local/bluestorm/frontend/cachedir" );
        tierExistingCache.moveFilesIntoTierStructure();
    }
    
    
    @Override public void moveFilesIntoTierStructure() throws IOException, InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool( MOVE_THREADS );
        final Duration totalDuration = new Duration();
        final AtomicInteger fileCount = new AtomicInteger( 0 );
        
        // These threads will pull from the queue for work, and quit when they get poisoned.
        for ( int i = 0; i < MOVE_THREADS; i++ )
        {
            executorService.submit( fileMover( fileCount ) );
        }
        
        final MonitoredWork work =
                new MonitoredWork( MonitoredWork.StackTraceLogging.NONE, "Generating cache listing" );
    
        try ( final DirectoryStream< Path > stream = Files.newDirectoryStream( m_cachePath ) )
        {
            int minDepth = Integer.MAX_VALUE;
            final Duration incrementalDuration = new Duration();
            for ( final Path p : stream )
            {
                /*
                 * If we can't put to the queue that's serviced by 24 threads within a minute, it's likely all the worker
                 * threads died for some reason, so we'll get out also.
                 */
                m_pathQueue.offer( p, 1, TimeUnit.MINUTES );
                final int currentDepth = m_pathQueue.size();
            
                // Keeping track of the min queue depth after we've put bunches into it, for monitoring purposes
                if ( 1000 < fileCount.get() )
                {
                    if ( currentDepth < minDepth )
                    {
                        minDepth = currentDepth;
                    }
                }
            
                if ( UPDATE_INTERVAL_SECONDS <= incrementalDuration.getElapsedSeconds() )
                {
                    LOG.info( String.format(
                            "One time restructuring of cache filesystem %s - files moved so far: %d (%d/second) - " +
                                    "%d/%d",
                        
                            m_filesystem, fileCount.get(), fileCount.get() / totalDuration.getElapsedSeconds(), minDepth,
                            currentDepth ) );
                    incrementalDuration.reset();
                }
            }
        }
        finally
        {
            work.completed();
        }
    
        m_pathQueue.put( POISON_PILL );
        int waitCount = 0;
        /*
         * Wait up to 60 seconds for all the worker threads to finish.  Typical minimum speed 500 files moved per
         * second, so 60 seconds should be more than enough time.
         */
        while ( ( POISON_PILL != m_pathQueue.peek() ) && ( 60 < waitCount++ ) )
        {
            Thread.sleep( 1000 );
        }
        
        if ( 0 < fileCount.get() )
        {
            LOG.info( String.format( "Completed one time restructuring of cache filesystem %s, %d files in %s",
                    m_filesystem, fileCount.get(), totalDuration.toString() ) );
        }
        else if ( 60 < waitCount )
        {
            LOG.error( "Waited too long for worker threads to complete.  There was likely a problem with the cache " +
                    "restructuring." );
        }
        executorService.shutdown();
    }
    
    
    private Runnable fileMover( final AtomicInteger fileCount )
    {
        return () -> {
            while ( true )
            {
                Path path;
                try
                {
                    path = m_pathQueue.take();
                    if ( POISON_PILL == path )
                    {
                        m_pathQueue.put( POISON_PILL );
                        return;
                    }
                }
                catch ( InterruptedException e )
                {
                    LOG.error( "Failed while trying to pull item from queue.  Exiting worker thread", e );
                    return;
                }
                /*
                 * By using file name convention, we bypass the filesystem directory check hit, which greatly
                 * speeds up this operation when in the millions of files.
                 */
                if ( 36 <= path.getFileName()
                               .toString()
                               .length() )
                {
                    fileCount.getAndIncrement();
                    UUID blobId = CacheUtils.getBlobId( path.toFile() );
                    try
                    {
                        Files.move( path, Paths.get( m_filesystem, CacheUtils.getTierPath( blobId ), path.getFileName()
                                                                                                         .toString() ),
                                ATOMIC_MOVE );
                    }
                    catch ( IOException e )
                    {
                        // Ignore it.  Hope for the best
                        LOG.error( "Failed while trying to move cache files into tiering structure, continuing.", e );
                    }
                }
            }
        };
    }
    
    
    @Override public void createTieredCacheStructure()
    {
        if ( !Files.exists( m_cachePath ) )
        {
            throw new RuntimeException(
                    "Cache file system directory path does not exist, cannot setup tiering structure - " +
                            m_filesystem );
        }
        
        final Set< PosixFilePermission > perms =
                EnumSet.of( OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, GROUP_EXECUTE, OTHERS_READ,
                        OTHERS_EXECUTE );
        
        Consumer< Path > setUser = getSetUserConsumer();
        Consumer< Path > setGroup = getSetGroupConsumer();
        
        final MonitoredWork work = new MonitoredWork( MonitoredWork.StackTraceLogging.NONE,
                "Setting up cache tiering directory structure" );
        try
        {
            for ( int i = 0; i < 256; i++ )
            {
                final String firstLevel = String.format( "%02x", i );
                final Path firstDirectory = Paths.get( m_cachePath.toString(), firstLevel );
                
                if ( !Files.exists( firstDirectory ) )
                {
                    createDirectory( firstDirectory );
                }
                setDirectoryPermissions( perms, firstDirectory );
                setUser.accept( firstDirectory );
                setGroup.accept( firstDirectory );
                
                for ( int j = 0; j < 256; j++ )
                {
                    final String secondLevel = String.format( "%02x", j );
                    final Path secondDirectory = Paths.get( m_cachePath.toString(), firstLevel, secondLevel );
                    
                    if ( !Files.isDirectory( secondDirectory ) )
                    {
                        createDirectory( secondDirectory );
                    }
                    setDirectoryPermissions( perms, secondDirectory );
                    setUser.accept( secondDirectory );
                    setGroup.accept( secondDirectory );
                }
            }
        }
        finally
        {
            work.completed();
        }
    }
    
    
    private Consumer< Path > getSetUserConsumer()
    {
        Consumer< Path > setUser = ( x ) -> {
        };
        try
        {
            final UserPrincipal user =
                    Files.readAttributes( m_cachePath, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS )
                         .owner();
            setUser = ( x ) -> {
                try
                {
                    Files.setOwner( x, user );
                }
                catch ( IOException e )
                {
                    LOG.error( "Failed trying to set owner of " + x.toString(), e );
                }
            };
        }
        catch ( IOException e )
        {
            LOG.error(
                    "Failed trying to find owner of " + m_cachePath.toString() + ", Continuing without setting owner",
                    e );
        }
        return setUser;
    }
    
    
    private Consumer< Path > getSetGroupConsumer()
    {
        Consumer< Path > setGroup = ( x ) -> {
        };
        try
        {
            final GroupPrincipal group =
                    Files.readAttributes( m_cachePath, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS )
                         .group();
            setGroup = ( x ) -> {
                try
                {
                    Files.getFileAttributeView( x, PosixFileAttributeView.class )
                         .setGroup( group );
                }
                catch ( IOException e )
                {
                    LOG.error( "Failed trying to set group of " + x.toString(), e );
                }
            };
        }
        catch ( IOException e )
        {
            LOG.error( "Failed trying to find group name of " + m_cachePath.toString() +
                    ", Continuing without setting group", e );
        }
        return setGroup;
    }
    
    
    private void setDirectoryPermissions( final Set< PosixFilePermission > perms, final Path directory )
    {
        try
        {
            Files.setPosixFilePermissions( directory, perms );
        }
        catch ( IOException e )
        {
            LOG.error( "Failed trying to set directory permissions on " + directory.toString(), e );
        }
    }
    
    
    private void createDirectory( final Path directory )
    {
        try
        {
            Files.createDirectory( directory );
        }
        catch ( IOException e )
        {
            LOG.error( "Failed trying to create directory " + directory.toString(), e );
        }
    }
    
    
    private static final int MOVE_THREADS = 24;
    private static final Path POISON_PILL = Paths.get( "/tmp" );
    private static final int UPDATE_INTERVAL_SECONDS = 60;
    private final static Logger LOG = Logger.getLogger( TierExistingCacheImpl.class );
    /*
     * The capacity of the blocking queue was determined experimentally.  When the ARC is fully 'charged' with a
     * cache size of 500k blobs (1M files), a queue size of 100 ended up starving 24 move threads.  A size of 200
     * hit a minimum of queue depth of 140 with an 'uncharged' cache. 200 got to a minimum of 20 with a charged
     * cache, so I chose 512 'cause it's a nice number, and we never want to starve the mover threads.
     * Update: With smaller file sizes, we've gotten down to 1/512.  65k files got down to 320.
     *
     * The cache file count used for these tests was ~21M files, with sizes from 10 bytes to 1k bytes
     */
    private final BlockingQueue< Path > m_pathQueue = new ArrayBlockingQueue<>( 512 );
    private final Path m_cachePath;
    private final String m_filesystem;
}
