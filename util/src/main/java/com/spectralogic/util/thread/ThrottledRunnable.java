/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;


public interface ThrottledRunnable
{
    public void run( final RunnableCompletionNotifier completionNotifier );
    
    
    interface RunnableCompletionNotifier
    {
        public void completed();
    }
    
    
    /**
     * Aggregates an already-scheduled-to-run throttled runnable with one that
     * will not get executed.
     */
    interface ThrottledRunnableAggregator< T >
    {
        /**
         * @param throttledRunnableScheduledToExecute
         * @param throttledRunnableToAggregateWith
         */
        public void aggregate( 
                final T throttledRunnableScheduledToExecute, 
                final T throttledRunnableToAggregateWith );
    }
}