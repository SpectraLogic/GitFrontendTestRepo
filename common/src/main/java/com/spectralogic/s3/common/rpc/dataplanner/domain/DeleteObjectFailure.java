/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface DeleteObjectFailure extends SimpleBeanSafeToProxy
{
    String OBJECT_ID = "objectId";

    UUID getObjectId();

    void setObjectId( final UUID value );
    
    
    String REASON = "reason";

    DeleteObjectFailureReason getReason();

    void setReason( final DeleteObjectFailureReason value );
}
