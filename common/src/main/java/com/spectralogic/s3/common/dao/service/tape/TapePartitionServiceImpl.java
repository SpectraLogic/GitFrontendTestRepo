/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.util.db.service.BaseService;

final class TapePartitionServiceImpl extends BaseService< TapePartition > implements TapePartitionService
{
    TapePartitionServiceImpl()
    {
        super( TapePartition.class );
    }
}
