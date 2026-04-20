/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface S3ObjectsOnMedia extends SimpleBeanSafeToProxy
{
    String BUCKETS = "buckets";
    
    @Optional
    BucketOnMedia [] getBuckets();
    
    void setBuckets( final BucketOnMedia [] value );
}
