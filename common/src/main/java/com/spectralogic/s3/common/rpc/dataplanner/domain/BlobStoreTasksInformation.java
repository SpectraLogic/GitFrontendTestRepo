/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BlobStoreTasksInformation extends SimpleBeanSafeToProxy
{
    String TASKS = "tasks";
    
    @Optional
    BlobStoreTaskInformation [] getTasks();
    
    void setTasks( final BlobStoreTaskInformation [] value );
}
