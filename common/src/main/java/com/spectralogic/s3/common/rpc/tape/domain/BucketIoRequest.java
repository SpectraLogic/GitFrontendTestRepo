/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

public interface BucketIoRequest extends BucketOnMedia
{
    S3ObjectIoRequest [] getObjects();
    
    void setObjects( final S3ObjectIoRequest [] value );
}
