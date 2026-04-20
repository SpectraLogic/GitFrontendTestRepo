/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.api;

import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.dataplanner.backend.api.RunnableBlobStoreTask;
import com.spectralogic.util.db.lang.DatabasePersistable;

public interface TargetTask< T extends DatabasePersistable & ReplicationTarget< T >, CF >
    extends RunnableBlobStoreTask
{
    void prepareForExecutionIfPossible();
    
    
    T getTarget();
}
