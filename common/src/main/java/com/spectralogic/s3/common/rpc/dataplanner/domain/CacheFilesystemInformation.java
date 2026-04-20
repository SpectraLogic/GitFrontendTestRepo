/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface CacheFilesystemInformation extends SimpleBeanSafeToProxy
{
    String USED_CAPACITY_IN_BYTES = "usedCapacityInBytes";
    
    long getUsedCapacityInBytes();
    
    void setUsedCapacityInBytes( final long value );
    
    
    String AVAILABLE_CAPACITY_IN_BYTES = "availableCapacityInBytes";
    
    long getAvailableCapacityInBytes();
    
    void setAvailableCapacityInBytes( final long value );
    
    
    String UNAVAILABLE_CAPACITY_IN_BYTES = "unavailableCapacityInBytes";
    
    long getUnavailableCapacityInBytes();
    
    void setUnavailableCapacityInBytes( final long value );
    
    
    String TOTAL_CAPACITY_IN_BYTES = "totalCapacityInBytes";
    
    long getTotalCapacityInBytes();
    
    void setTotalCapacityInBytes( final long value );


    String JOB_LOCKED_CACHE_IN_BYTES = "jobLockedCacheInBytes";

    long getJobLockedCacheInBytes();

    void setJobLockedCacheInBytes( final long value );
    
    
    String SUMMARY = "summary";
    
    String getSummary();
    
    void setSummary( final String value );
    
    
    String CACHE_FILESYSTEM = "cacheFilesystem";
    
    CacheFilesystem getCacheFilesystem();
    
    void setCacheFilesystem( final CacheFilesystem value );
    

    String ENTRIES = "entries";
    
    @Optional
    CacheEntryInformation [] getEntries();
    
    void setEntries( final CacheEntryInformation [] value );
}
