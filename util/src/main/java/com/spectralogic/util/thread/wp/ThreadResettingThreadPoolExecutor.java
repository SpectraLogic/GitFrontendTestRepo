/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A thread pool executor that will reset its threads to default settings before re-using them.
 */
final class ThreadResettingThreadPoolExecutor extends ThreadPoolExecutor
{
    ThreadResettingThreadPoolExecutor(
            final int corePoolSize,
            final int maximumPoolSize,
            final long keepAliveTime,
            final TimeUnit unit,
            final BlockingQueue< Runnable > workQueue,
            final ThreadFactory threadFactory )
    {
        super( corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory );
    }


    @Override
    protected void afterExecute( final Runnable r, final Throwable thrw  )
    {
        super.afterExecute( r, thrw );
        ( ( ResettableThread )Thread.currentThread() ).reset();
    }
} 