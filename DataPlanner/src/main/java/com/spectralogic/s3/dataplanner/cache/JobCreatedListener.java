/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.cache;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;

public interface JobCreatedListener
{
    /**
     * Once a job has been fully created and committed, listeners shall be notified of its creation via a
     * call to this method.  Any job that was not fully, successfully created and committed shall not be
     * sent to any listener.
     */
    public void jobCreated( final JobRequestType type, final UUID jobId );
}
