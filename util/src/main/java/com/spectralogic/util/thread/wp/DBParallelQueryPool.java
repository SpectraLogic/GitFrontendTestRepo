/*
 *
 * Copyright C 2019, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.thread.wp;

import com.spectralogic.util.tunables.Tunables;

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


    private final static WorkPool INSTANCE;
    static
    {
        final int numThreads = Tunables.dbParallelQueryPoolSize();
        INSTANCE = WorkPoolFactory.createBoundedWorkPool( numThreads << 4, numThreads, "DBParallelQueryPool" );
    }
}
