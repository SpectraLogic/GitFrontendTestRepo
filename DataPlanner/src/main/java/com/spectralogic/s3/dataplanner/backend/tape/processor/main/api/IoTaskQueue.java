/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;

/**
 * A queue of work where each task in the queue is associated with a {@link JobEntry}.  A single job chunk
 * may have at most one task associated with it.
 */
public interface IoTaskQueue extends TaskQueue
{
    /**
     * Adds the specified I/O task for the specified chunk, first removing the existing task for the specified
     * chunk if one exists
     */
    void add(final Set<JobEntry> chunks, final TapeTask task );
    
    
    /**
     * @return all chunk ids for the job specified that have tasks in the queue
     */
    Set< UUID > getChunkIds();

    /**
     * @return TRUE if task could be found and was removed; FALSE otherwise
     */
    boolean remove( final UUID chunkId, final String cause );
}
