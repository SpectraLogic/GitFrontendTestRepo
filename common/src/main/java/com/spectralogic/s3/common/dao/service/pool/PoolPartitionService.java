/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface PoolPartitionService
    extends BeansRetriever< PoolPartition >, 
            BeanCreator< PoolPartition >, 
            BeanUpdater< PoolPartition >,
            BeanDeleter
{
    // empty
}
