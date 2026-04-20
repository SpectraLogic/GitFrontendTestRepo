/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;

/**
 * Reads will be serviced in this preference order:
 * <ol>
 * <li> Local cache
 * <li> {@link PoolType#ONLINE} pool
 * <li> {@link PoolType#NEARLINE} pool 
 * <li> Non-ejectable tape
 * <li> Ejectable tape
 * </ol>
 * 
 * For example, if a {@link Blob} resides on local cache as well as tape, it should be serviced from cache.
 * <br><br>
 * 
 * Unlike the examples above, a replication target's read preference is not so black and white.  If the
 * replication target is on the customer's LAN and is very inexpensive to access, it should be strongly
 * preferenced.  If on the other hand, the replication target is in the cloud and bandwidth to access it is
 * expensive, it may make sense to pull data from tape even if it would be faster to pull data from the
 * replication target.  <br><br>
 * 
 * The minimum latency read preference should only be used when the network between the source and target is 
 * very inexpensive.  In this mode, the read preference is dynamically determined based on whether the blobs 
 * on the target are on pool or tape.  For example, if the source has a blob on pool, the blob will be 
 * serviced locally.  If however, the source only has a blob on tape and the target has the blob on pool, the
 * blob will be serviced by the target.
 */
public enum TargetReadPreferenceType
{
    MINIMUM_LATENCY,
    AFTER_ONLINE_POOL,
    AFTER_NEARLINE_POOL,
    AFTER_NON_EJECTABLE_TAPE,
    LAST_RESORT,
    NEVER
}
