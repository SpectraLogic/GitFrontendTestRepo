/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.util.bean.lang.Optional;

public interface S3ObjectsToVerify extends S3ObjectsOnMedia
{
    String OPTIONAL_S3_OBJECT_METADATA_KEYS = "optionalS3ObjectMetadataKeys";
    
    @Optional
    String [] getOptionalS3ObjectMetadataKeys();
    
    void setOptionalS3ObjectMetadataKeys( final String [] value );
}
