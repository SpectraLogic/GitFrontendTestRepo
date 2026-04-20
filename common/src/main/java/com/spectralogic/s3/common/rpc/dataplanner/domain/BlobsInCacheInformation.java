/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BlobsInCacheInformation extends SimpleBeanSafeToProxy
{
    String BLOBS_IN_CACHE = "blobsInCache";
    
    @Optional
    UUID [] getBlobsInCache();
    
    void setBlobsInCache( final UUID [] value );
}
