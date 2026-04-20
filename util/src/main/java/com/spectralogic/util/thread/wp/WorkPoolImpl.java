/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.thread.wp;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.spectralogic.util.lang.Validations;

final class WorkPoolImpl implements WorkPool
{
    WorkPoolImpl( final int maxThreads, final String baseName )
    {
        this( new LinkedBlockingQueue<>(), maxThreads, baseName );
    }
    
    
    WorkPoolImpl( final BlockingQueue< Runnable > queue, final int maxThreads, final String baseName )
    {
        Validations.verifyNotNull( "Base name", baseName );
        Validations.verifyInRange( "Max threads", 1, 256, maxThreads );
        final ThreadFactory factory = new ResettableThreadFactory( true, baseName );
        m_pool = new ThreadResettingThreadPoolExecutor(
                maxThreads,
                maxThreads,
                KEEP_ALIVE_TIME_IN_SECS,
                TimeUnit.SECONDS,
                queue,
                factory );
    }


    public Future< ? > submit( final Runnable task )
    {
        return m_pool.submit( new RunnableAdapter( task ) );
    }
    
    
    public int getActiveCount()
    {
        return m_pool.getActiveCount();
    }

    
    public void shutdown()
    {
        m_pool.shutdown();
    }


    public List< Runnable > shutdownNow()
    {
        return m_pool.shutdownNow();
    }


    public boolean isShutdown()
    {
        return m_pool.isShutdown();
    }
    
    
    public boolean isTerminated()
    {
        return m_pool.isTerminated();
    }


    public boolean awaitTermination( final long timeout, final TimeUnit unit ) throws InterruptedException
    {
        return m_pool.awaitTermination( timeout, unit );
    }


    public boolean awaitEmpty( final long timeout, final TimeUnit unit ) throws InterruptedException
    {
        final long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
        while ( m_pool.getActiveCount() > 0 )
        {
            final long timeLeft = endTime - System.currentTimeMillis();
            if ( timeLeft <= 0 )
            {
                return false;
            }
            Thread.sleep( Math.min( 100, timeLeft ) );
        }
        return true;
    }
    
    
    public boolean isFull()
    {
        return ( m_pool.getActiveCount() == m_pool.getMaximumPoolSize() );
    }
    
    
    private final ThreadPoolExecutor m_pool;
    private static final int KEEP_ALIVE_TIME_IN_SECS = 10 * 60;
}
