/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectrads3;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BlobPersistenceContainer extends SimpleBeanSafeToProxy
{
    String JOB_EXISTANT = "jobExistant";
    
    boolean isJobExistant();
    
    BlobPersistenceContainer setJobExistant( final boolean value );
    
    
    String BLOBS = "blobs";
    
    BlobPersistence [] getBlobs();
    
    BlobPersistenceContainer setBlobs( final BlobPersistence [] value );
}
