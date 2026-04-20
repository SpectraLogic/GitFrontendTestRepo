/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;


public interface S3ObjectsIoRequest extends S3ObjectsOnMedia
{
    String CACHE_ROOT_PATH = "cacheRootPath";
    
    String getCacheRootPath();
    
    void setCacheRootPath( final String value );
    

    BucketIoRequest [] getBuckets();
    
    void setBuckets( final BucketIoRequest [] value );
}
