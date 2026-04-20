/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BlobIoFailure extends SimpleBeanSafeToProxy
{
    String BLOB_ID = "blobId";
    
    UUID getBlobId();
    
    BlobIoFailure setBlobId( final UUID value );
    
    
    String FAILURE = "failure";
    
    BlobIoFailureType getFailure();
    
    BlobIoFailure setFailure( final BlobIoFailureType value );
}
