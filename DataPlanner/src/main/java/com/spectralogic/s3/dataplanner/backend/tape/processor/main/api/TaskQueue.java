/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api;

import java.util.Set;

import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;

/**
 * A queue of tasks.  <br><br>
 * 
 * Clients are required to synchronize on the queue lock while operating on it since it is rarely the case 
 * that calls made on the queue can be locked only for the duration of the queue call (it is almost always 
 * the case that the queue must be locked over a series of operations by the client code).
 */
public interface TaskQueue
{
    /**
     * @return TRUE if this queue contains the specified task; FALSE otherwise
     */
    boolean contains( final TapeTask task );
    
    
    /**
     * @return all tasks in the queue
     */
    Set< TapeTask > toSet();
    
    
    /**
     * @return TRUE if task could be found and was removed; FALSE otherwise
     */
    boolean remove( final TapeTask tapeTask, final String cause );
    
    
    /**
     * @return the number of tasks in the queue
     */
    int size();
}
