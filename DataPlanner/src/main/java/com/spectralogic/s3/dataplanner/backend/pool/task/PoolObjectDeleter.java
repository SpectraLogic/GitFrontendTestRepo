/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class PoolObjectDeleter extends BaseShutdownable
{
    public PoolObjectDeleter()
    {
        initWorkPool();
    }
    
    
    /**
     * This init approach is designed to work most easily with test run thread
     * leak detection and prevention, and to do so without inducing negative
     * production impacts on this class or any classes depending on it. Do not
     * change this approach except as part of changes to test run thread leak
     * detection and prevention (see TheadLeakHunter).
     */
    private static void initWorkPool()
    {
        synchronized ( WORK_POOL_LOCK )
        {
            if ( null == s_workPool || s_workPool.isShutdown() )
            {
                s_workPool = WorkPoolFactory.createWorkPool( NUM_THREADS, PoolObjectDeleter.class.getSimpleName() );
            }
        }
    }
    
    
    public void deleteObjects( final BeansServiceManager serviceManager, final PoolLockSupport< PoolTask > lockSupport,
            final Pool pool, final String bucketName, final Set< UUID > objectIds )
    {
        verifyNotShutdown();
        s_workPool.submit( new ObjectDeleter( serviceManager, lockSupport, pool, bucketName, objectIds ) );
    }
    
    
    private final class ObjectDeleter implements Runnable
    {
        ObjectDeleter( final BeansServiceManager serviceManager, final PoolLockSupport< PoolTask > lockSupport,
                final Pool pool, final String bucketName, final Set< UUID > objectIds )
        {
            m_serviceManager = serviceManager;
            m_lockSupport = lockSupport;
            m_pool = pool;
            m_bucketName = bucketName;
            m_objectIds.addAll( objectIds );
            m_lockHolder = InterfaceProxyFactory.getProxy( PoolTask.class,
                    MockInvocationHandler.forToString( "Delete Objects From Pool in " + bucketName ) );
            m_trash = PoolUtils.getTrashPath( pool );
        }
        
        
        public void run()
        {
            verifyNotShutdown();
            final AtomicInteger totalObjects = new AtomicInteger( m_objectIds.size() );
            final AtomicInteger processed = new AtomicInteger( 1 );
            final MonitoredWork work =
                    new MonitoredWork( MonitoredWork.StackTraceLogging.NONE, PoolObjectDeleter.class.getSimpleName(),
                            x -> "Waiting for lock to delete from " + m_bucketName + " on " + m_pool.getName() );
            try
            {
                m_lockSupport.acquireDeleteLockWait( m_pool.getId(), m_lockHolder );
                verifyNotShutdown();
                work.setCustomMessage( x -> String.format(
                        "Processed %d of %d " + "objects to remove from %s on %s in %s, ~%.0fs " + "left",
                        processed.get(), totalObjects.get(), m_bucketName, m_pool.getName(), x.toString(),
                                ( x.getElapsedSeconds() / ( double ) ( processed.get() ) ) *
                                        ( totalObjects.get() - processed.get() ) ) );
                Iterator< UUID > objectIdIterator = m_objectIds.iterator();
                while ( objectIdIterator.hasNext() )
                {
                    final UUID uuid = objectIdIterator.next();
                    objectIdIterator.remove();
                    removeObject( uuid );
                    processed.incrementAndGet();
                }
            }
            finally
            {
                work.completed();
                m_lockSupport.releaseLock( m_lockHolder );
                new ThreadedTrashCollector().emptyTrash( m_trash );
            }
        }
        
        
        private void removeObject( final UUID objectId )
        {
            final S3Object object = m_serviceManager.getRetriever( S3Object.class )
                                                    .retrieve( objectId );
            if ( null == object ) //Make sure object does not still exist in the DB
            {
                final Path objectPath = PoolUtils.getPath( m_pool, m_bucketName, objectId, null );
                if ( Files.exists( objectPath ) )
                {
                    try
                    {
                        for ( final String suffix : PoolUtils.getInfoFileSuffixes() )
                        {
                            final Path infoFile = Paths.get( objectPath.toString() + suffix );
                            Files.deleteIfExists( infoFile );
                        }
                        Files.move( objectPath, m_trash.resolve( objectPath.getFileName() ) );
                    }
                    catch ( final IOException ex )
                    {
                        LOG.info( String.format( "Failed to remove object %s from bucket %s in pool %s",
                                objectId.toString(), m_bucketName, m_pool.getName() ), ex );
                    }
                }
            }
        }
        
        
        private final PoolLockSupport< PoolTask > m_lockSupport;
        private final String m_bucketName;
        private final PoolTask m_lockHolder;
        private final Set< UUID > m_objectIds = new HashSet<>();
        private final Pool m_pool;
        private final BeansServiceManager m_serviceManager;
        private final Path m_trash;
    }
    
    
    private final static Logger LOG = Logger.getLogger( ObjectDeleter.class );
    /*
     * Ideally there would be 1 thread per pool which would allow concurrent object deletion per pool,
     * but the way to do that is not immediate obvious.
     */
    private final static int NUM_THREADS = 4;
    private final static Object WORK_POOL_LOCK = new Object();
    private static WorkPool s_workPool = null;
}
