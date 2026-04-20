/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.pool.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

/**
 * There is a 1:1 relationship between a pool and its volumes, or in other words, every pool has precisely 
 * one volume, and thus, has precisely one {@link PoolObservable#MOUNTPOINT}, etc.  Thus, you may think of
 * a pool as being synonymous with a volume (what we call a pool is really a combination of pool and volume 
 * attributes, which we can safely do given the 1:1 mapping between pools and volumes).  <br><br>
 * 
 * Note that there may be multiple volumes on a pool reported to Bluestorm; however, in this case, only one 
 * of those volumes may be for Bluestorm.  This is the volume that is reported to Bluestorm, and the volume 
 * from which the {@link PoolObservable#MOUNTPOINT}, etc. is derived.  If there are other volumes on a pool 
 * reported to Bluestorm, the volume reported to Bluestorm must have a reservation to guarantee that the 
 * {@link #TOTAL_CAPACITY}, etc. does not change.  In this manner, we can make the simplifying statement
 * that there is a 1:1 relationship between a pool and its volumes - as this is true as far as this software
 * stack is concerned, even if technically speaking, it is not true at lower layers.
 */
public interface PoolInformation 
    extends SimpleBeanSafeToProxy, PoolObservable< PoolInformation >
{
    String POOL_ID = "poolId";
    
    /**
     * @return the pool id last written to the pool
     * <br><br>
     * 
     * If the pool id matches the pool id in our database, that means that we own that pool.  If it does not 
     * match, that means that some other appliance took ownership over the pool and the pool is foreign and 
     * would have to be imported.  If the pool id is null, no appliance has taken ownership over the pool.
     * <br><br>
     * 
     * 
     * <b>Details applicable to implementors of this method (and not clients of it):</b><br><br>
     * 
     * There will be a custom ZFS property on every Bluestorm zpool that would be reported to Bluestorm as a
     * pool part of the Bluestorm pool environment.  We will refer to this ZFS property as "bpOwnerId".  
     * <br><br>
     * 
     * ZFS-foreign BP zpools should be ZFS-imported automatically and reported in the pool environment.  We 
     * will need to distinguish between Verde NAS pools and BP pools by checking for the existance of 
     * bpOwnerId.  If it exists, we ZFS-import automatically.  Else, we do whatever we need to do for Verde.  
     * Note that it may be necessary to temporarily import the zpool in read-only mode in order to determine 
     * if the pool is a BP zpool or a Verde zpool, which requires being able to read bpOwnerId off the zpool.
     * <br><br>
     * 
     * Finally, the poolId property on PoolInformation in the RPC response to get the pool environment shall 
     * be null if bpOwnerId does not exist on the pool, or non-null with the correct value if it does exist.  
     */
    @Optional
    UUID getPoolId();
    
    void setPoolId( final UUID value );
}
