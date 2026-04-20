/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface ImportPersistenceTargetDirectiveRequest 
    extends ImportPersistenceTargetDirective< ImportPersistenceTargetDirectiveRequest >, SimpleBeanSafeToProxy
{
    String PRIORITY = "priority";
    
    BlobStoreTaskPriority getPriority();
    
    ImportPersistenceTargetDirectiveRequest setPriority( final BlobStoreTaskPriority value );
}
