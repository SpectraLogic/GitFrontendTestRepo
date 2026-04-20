/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Collection;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public interface JobProgressManager
{
    /**
     * Flushes any queued progress updates out to the database
     */
    public void flush();


    /**
     * Eventually updates the chunk's job with the updated cached progress
     */
    public void entriesLoadedToCache(final BeansServiceManager transaction, final Collection<JobEntry> entries );


    /**
     * Eventually updates the chunk's job with the updated cached progress
     */
    public void entryLoadedToCache(final BeansServiceManager transaction, final JobEntry chunk );


    /**
     *  Eventually updates the job with the updated cached progress
     */
    public void bytesLoadedToCache(final BeansServiceManager transaction, final long bytesLoaded, final UUID jobId);
    
    
    /**
     * Eventually updates the job with the updated cached progress
     */
    public void blobLoadedToCache( final UUID jobId, final long size );
    

    /**
     * Eventually updates the job with the updated completed progress
     */
    public void workCompleted( final UUID jobId, final long bytesOfWorkCompleted );
}
