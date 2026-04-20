/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectrads3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BlobIdsSpecification extends SimpleBeanSafeToProxy
{
    String JOB_ID = "jobId";
    
    UUID getJobId();
    
    void setJobId( final UUID value );
    
    
    String BLOB_IDS = "blobIds";
    
    UUID [] getBlobIds();
    
    void setBlobIds( final UUID [] value );
}
