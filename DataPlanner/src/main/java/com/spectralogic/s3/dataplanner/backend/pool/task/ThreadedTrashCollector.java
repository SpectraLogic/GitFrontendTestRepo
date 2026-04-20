/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class ThreadedTrashCollector extends BaseShutdownable
{
    public ThreadedTrashCollector()
    {
        initWorkPool();
    }
    
    
    /**
     * This init approach is designed to work most easily with test run thread
     * leak detection and prevention, and to do so without inducing negative
     * production impacts on this class or any classes depending on it. Do not
     * change this approach except as part of changes to test run thread leak
     * detection and prevention (see ThreadLeakHunter).
     */
    private static void initWorkPool()
    {
        synchronized ( WORK_POOL_LOCK )
        {
            if ( null == s_workPool || s_workPool.isShutdown() )
            {
                s_workPool =
                        WorkPoolFactory.createWorkPool( NUM_THREADS, ThreadedTrashCollector.class.getSimpleName() );
                
                for ( int i = 0; i < NUM_THREADS; ++i )
                {
                    s_workPool.submit( new TrashCollector() );
                }
            }
        }
    }
    
    
    public void emptyTrash( final Path toEmpty )
    {
        verifyNotShutdown();
        synchronized ( PENDING )
        {
            if ( !PENDING.contains( toEmpty ) )
            {
                PENDING.add( toEmpty );
            }
        }
    }
    
    
    private final static class TrashCollector implements Runnable
    {
        public void run()
        {
            while ( true )
            {
                Path toEmpty;
                try
                {
                    toEmpty = PENDING.take();
                }
                catch ( final InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
    
                if ( !Files.exists( toEmpty ) )
                {
                    continue;
                }
                
                final AtomicInteger filesDeleted = new AtomicInteger( 0 );
                final AtomicInteger directoriesDeleted = new AtomicInteger( 0 );
                final MonitoredWork work = new MonitoredWork( MonitoredWork.StackTraceLogging.NONE, "", x -> {
                    int elapsedSeconds = x.getElapsedSeconds();
                    if ( 0 == elapsedSeconds )
                    {
                        elapsedSeconds = 1;
                    }
                    return String.format( "So far removed %d (%d/sec) files and %d (%d/sec) directories from %s in %s",
                            filesDeleted.get(), filesDeleted.get() / elapsedSeconds, directoriesDeleted.get(),
                            directoriesDeleted.get() / elapsedSeconds, toEmpty, x.toString() );
                } );
                try
                {
                    Files.walkFileTree( toEmpty, new SimpleFileVisitor< Path >()
                    {
                        private void display()
                        {
                            if ( 2 < work.getDuration()
                                         .getElapsedMinutes() )
                            {
                                int elapsedSeconds = work.getDuration()
                                                         .getElapsedSeconds();
                                LOG.info( String.format(
                                        "Completed removing %d (%d/sec) files and %d (%d/sec) directories from %s in " +
                                                "%s",
                                        filesDeleted.get(), filesDeleted.get() / elapsedSeconds,
                                        directoriesDeleted.get(), directoriesDeleted.get() / elapsedSeconds, toEmpty,
                                        work.getDuration()
                                            .toString() ) );
                            }
                        }
                        
                        
                        @Override public FileVisitResult postVisitDirectory( final Path dir, final IOException exc )
                                throws IOException
                        {
                            if ( dir.equals( toEmpty ) )
                            {
                                display();
                                // This TERMINATE causes us to NOT delete the top level directory we're supposed to
                                // empty
                                return FileVisitResult.TERMINATE;
                            }
                            
                            if ( exc == null )
                            {
                                directoriesDeleted.incrementAndGet();
                                Files.deleteIfExists( dir );
                                return FileVisitResult.CONTINUE;
                            }
                            else
                            {
                                throw exc;
                            }
                        }
                        
                        
                        @Override public FileVisitResult visitFile( final Path file, final BasicFileAttributes attrs )
                                throws IOException
                        {
                            filesDeleted.incrementAndGet();
                            Files.deleteIfExists( file );
                            return FileVisitResult.CONTINUE;
                        }
                    } );
                }
                catch ( final IOException e )
                {
                    LOG.warn( "Failed while emptying " + toEmpty.toString() + ", ignoring.", e );
                }
                finally
                {
                    work.completed();
                }
            }
        }
    }
    
    private final static Logger LOG = Logger.getLogger( TrashCollector.class );
    private final static int NUM_THREADS = 1;
    private final static BlockingQueue< Path > PENDING = new LinkedBlockingQueue<>();
    private final static Object WORK_POOL_LOCK = new Object();
    private static WorkPool s_workPool = null;
}
