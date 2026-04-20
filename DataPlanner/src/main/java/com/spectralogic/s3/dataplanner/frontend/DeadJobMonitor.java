/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.UUID;

public interface DeadJobMonitor
{
    boolean isDead( final UUID jobId );
    
    
    void activityOccurred( final UUID jobId, final UUID blobId );
}
