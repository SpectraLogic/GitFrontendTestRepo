/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BucketOnMedia extends SimpleBeanSafeToProxy
{
    String BUCKET_NAME = "bucketName";
    
    String getBucketName();
    
    BucketOnMedia setBucketName( final String value );
    
    
    String OBJECTS = "objects";
    
    S3ObjectOnMedia [] getObjects();
    
    BucketOnMedia setObjects( final S3ObjectOnMedia [] value );
}
