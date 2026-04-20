/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;


/**
 * For a write blob request, the checksum value and type must be stored.  For a read blob request, the
 * checksum value and type must be validated against the stored checksum value and type.  A failure must
 * be issued if they do not match.  Additionally, as the data is read off tape into the cache, the checksum
 * must be validated against the data read.  If a mismatch occurs, a failure must be issued.
 */
public interface BlobIoRequest extends BlobOnMedia
{
    String FILE_NAME = "fileName";
    
    /**
     * @return the object chunk file name relative to the cache root directory.  For example, if the object 
     * chunk file name in cache is /usr/local/cache/object234-chunk1, then the cache root directory would be 
     * /usr/local/cache/ and the object chunk file name would be object234-chunk1
     */
    String getFileName();
    
    void setFileName( final String value );
}
