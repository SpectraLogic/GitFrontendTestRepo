/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;


public interface S3ObjectIoRequest extends S3ObjectOnMedia
{
    BlobIoRequest [] getBlobs();
    
    void setBlobs( final BlobIoRequest [] value );
}
