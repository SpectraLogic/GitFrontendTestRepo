/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

import java.util.concurrent.ArrayBlockingQueue;


public final class WorkPoolFactory
{
    public static WorkPool createWorkPool( final int maxThreads, final String baseName )
    {
        return new WorkPoolImpl( maxThreads, baseName );
    }
    
    
    /**
     * @param maxRunnablesInQueue - The max number of runnables allowed to wait in queue (runnables executing
     * do not count toward this limit)
     */
    public static WorkPool createBoundedWorkPool(
            final int maxRunnablesInQueue, 
            final int maxThreads, 
            final String baseName )
    {
        return new WorkPoolImpl(
                new BoundedQueue( maxRunnablesInQueue ),
                maxThreads, 
                baseName );
    }
    
    
    private final static class BoundedQueue extends ArrayBlockingQueue< Runnable >
    {
        private BoundedQueue( final int capacity )
        {
            super( capacity, true );
        }

        
        @Override
        public boolean offer( Runnable e )
        {
            try
            { 
                put( e );
                return true;
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
    } // end inner class def
}
