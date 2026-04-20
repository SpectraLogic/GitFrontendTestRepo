/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface DeleteObjectsResult extends SimpleBeanSafeToProxy
{
    String FAILURES = "failures";

    @Optional
    DeleteObjectFailure[] getFailures();

    DeleteObjectsResult setFailures( final DeleteObjectFailure[] value );
    
    
    String DAO_MODIFIED = "daoModified";
    
    boolean isDaoModified();
    
    DeleteObjectsResult setDaoModified( final boolean value );
}
