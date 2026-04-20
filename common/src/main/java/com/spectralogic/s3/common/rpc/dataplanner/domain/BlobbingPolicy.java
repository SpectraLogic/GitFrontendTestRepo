/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;

public enum BlobbingPolicy
{
    /**
     * {@link S3Object}s will be blobbed into one or more {@link Blob}s
     */
    ENABLED,
    
    
    /**
     * The {@link S3Object} will not be blobbed into more than one {@link Blob}, which has many negative side
     * effects, including but not limited to, imposing a limit on the size of the {@link S3Object}.  <br><br>
     * 
     * This policy should only be chosen when the DS3 API enhancments cannot be used and strict AWS S3 APIs
     * must be used instead.
     */
    DISABLED
}
