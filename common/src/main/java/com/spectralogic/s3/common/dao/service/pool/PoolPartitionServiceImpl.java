/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.util.db.service.BaseService;

final class PoolPartitionServiceImpl extends BaseService< PoolPartition > implements PoolPartitionService
{
    PoolPartitionServiceImpl()
    {
        super( PoolPartition.class );
    }
}
