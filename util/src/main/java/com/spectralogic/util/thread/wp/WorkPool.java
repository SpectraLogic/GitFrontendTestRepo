/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Minimal interface for a simple work pool in terms of Java's {@link Runnable} interface.
 */
public interface WorkPool
{
    public Future< ? > submit( final Runnable task );
    
    
    /**
     * Returns the approximate number of threads that are actively executing tasks.
     *
     * @return the number of threads
     */
    public int getActiveCount();


    /**
     * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will 
     * be accepted. Invocation has no additional effect if already shut down.  <br><br>
     *
     * This method does not wait for previously submitted tasks to complete execution.  Use 
     * {@link #awaitTermination awaitTermination} to do that.
     */
    public void shutdown();


    /**
     * Attempts to stop all actively executing tasks, halts the processing of waiting tasks, and returns a 
     * list of the tasks that were awaiting execution.
     *
     * There are no guarantees beyond best-effort attempts to stop processing actively executing tasks. 
     * Cancellation is implemented via {@link Thread#interrupt}, so if any tasks mask or fail to respond to
     * interrupts, they may never terminate. Thus, tasks <strong>must</strong> respond to interrupts.
     *
     * @return list of tasks that never commenced execution
     */
    public List< Runnable > shutdownNow();


    /**
     * Is this work pool shut down?
     */
    public boolean isShutdown();
    
        
    /**
     * Returns <tt>true</tt> if all tasks have completed following shut down.  <br><br>
     * 
     * Note: Never returns <tt>true</tt> unless either {@link #shutdown()} or {@link #shutdownNow()} was 
     * called first.
     *
     * @return <tt>true</tt> if all tasks have completed following shut down
     */
    public boolean isTerminated();
    
    
    /**
     * @return <tt>true</tt> if the work pool does not have any threads free and has reached the maximum
     * number of threads permitted
     */
    public boolean isFull();


    /**
     * Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or 
     * the current thread is interrupted, whichever happens first.
     *
     * @return <tt>true</tt> if this work pool terminated and <tt>false</tt> if the timeout elapsed before 
     * termination
     */
    public boolean awaitTermination( final long timeout, final TimeUnit unit ) throws InterruptedException;

    /**
     * Blocks until the active count of the threadpool is zero, or the timeout occurs, or
     * the current thread is interrupted, whichever happens first.
     *
     * @return <tt>true</tt> if this work pool went to 0 and <tt>false</tt> if the timeout elapsed before
     * termination
     */
    public boolean awaitEmpty( final long timeout, final TimeUnit unit ) throws InterruptedException;
}