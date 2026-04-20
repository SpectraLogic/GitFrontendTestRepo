/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface CacheInformation extends SimpleBeanSafeToProxy
{
    String FILESYSTEMS = "filesystems";
    
    CacheFilesystemInformation [] getFilesystems();
    
    void setFilesystems( final CacheFilesystemInformation [] value );
}
