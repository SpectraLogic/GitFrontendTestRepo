/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.suspectblob.BaseSuspectBlobService;

final class SuspectBlobPoolServiceImpl
    extends BaseSuspectBlobService< SuspectBlobPool, Pool > implements SuspectBlobPoolService
{
    SuspectBlobPoolServiceImpl()
    {
        super( SuspectBlobPool.class, Pool.class, BlobPool.POOL_ID, PersistenceTarget.LAST_VERIFIED );
    }
}
