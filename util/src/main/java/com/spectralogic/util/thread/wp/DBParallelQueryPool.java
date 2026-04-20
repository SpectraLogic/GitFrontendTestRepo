/*
 *
 * Copyright C 2019, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.thread.wp;

public final class DBParallelQueryPool
{
    private DBParallelQueryPool()
    {
        // singleton
    }
    
    
    public static WorkPool getInstance()
    {
        return INSTANCE;
    }
    
    
    private final static int NUM_THREADS = Math.max( 1, ( Runtime.getRuntime()
                                                                 .availableProcessors() * 3 ) / 4 );
    private final static WorkPool INSTANCE =
            WorkPoolFactory.createBoundedWorkPool( NUM_THREADS << 4, NUM_THREADS, "DBParallelQueryPool" );
}
