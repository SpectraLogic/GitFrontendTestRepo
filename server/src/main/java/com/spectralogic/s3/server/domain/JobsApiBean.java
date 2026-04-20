/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;

@CustomMarshaledTypeName( "" )
public interface JobsApiBean extends SimpleBeanSafeToProxy
{
    String JOBS = "jobs";
    
    @CustomMarshaledName( 
            collectionValue = "Jobs",
            value = "Job",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    JobApiBean [] getJobs();
    
    void setJobs( final JobApiBean [] value );
}
