/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.api;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;

public interface S3ObjectCreator
{
    Set< S3Object > getObjects();
    
    
    Set< Blob > getBlobs();


    long getTotalSize();
    
    
    void commit( final S3ObjectService objectService, final BlobService blobService );
}
