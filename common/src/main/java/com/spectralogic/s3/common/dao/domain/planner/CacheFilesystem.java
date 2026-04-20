/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.planner;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultDoubleValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique( CacheFilesystem.NODE_ID )
})
public interface CacheFilesystem extends DatabasePersistable
{
    String NODE_ID = "nodeId";
    
    @CascadeDelete
    @References( Node.class )
    UUID getNodeId();
    
    CacheFilesystem setNodeId( final UUID value );
    
    
    String PATH = "path";
    
    String getPath();
    
    CacheFilesystem setPath( final String value );
    
    
    String MAX_CAPACITY_IN_BYTES = "maxCapacityInBytes";
    
    @Optional
    Long getMaxCapacityInBytes();
    
    CacheFilesystem setMaxCapacityInBytes( final Long value );
    
    
    String MAX_PERCENT_UTILIZATION_OF_FILESYSTEM = "maxPercentUtilizationOfFilesystem";

    @Optional
    Double getMaxPercentUtilizationOfFilesystem();
    
    CacheFilesystem setMaxPercentUtilizationOfFilesystem( final Double value );
    
    
    String AUTO_RECLAIM_INITIATE_THRESHOLD = "autoReclaimInitiateThreshold";
    
    /**
     * @return the percent utilization the cache must exceed for an auto reclaim to initiate reclaim 
     * operations
     */
    @DefaultDoubleValue( 0.82 )
    double getAutoReclaimInitiateThreshold();
    
    CacheFilesystem setAutoReclaimInitiateThreshold( final double value );
    
    
    String AUTO_RECLAIM_TERMINATE_THRESHOLD = "autoReclaimTerminateThreshold";
    
    /**
     * @return the percent utilization the cache must drop below during an auto reclaim for the auto reclaim
     * to stop reclaim operations and return early
     */
    @DefaultDoubleValue( 0.72 )
    double getAutoReclaimTerminateThreshold();
    
    CacheFilesystem setAutoReclaimTerminateThreshold( final double value );


    String BURST_THRESHOLD = "burstThreshold";

    /**
     * @return the percent utilization the cache must exceed to disable bursting (throttling is used once
     * cache utilization reaches this level to ensure jobs don't starve out other jobs)
     */
    @Deprecated
    @DefaultDoubleValue( 0.85 )
    double getBurstThreshold();

    CacheFilesystem setBurstThreshold( final double value );

    
    String CACHE_SAFETY_ENABLED = "cacheSafetyEnabled";

    /**
     * @return if we have o_sync enabled on writes to cache or not (false means the possibility of data loss)
     */
    @DefaultBooleanValue( false )
    boolean getCacheSafetyEnabled();
    
    CacheFilesystem setCacheSafetyEnabled( final boolean value );


    String NEEDS_RECONCILE = "needsReconcile";

    /**
     * @return if we require a full cache reconcile
     */
    @DefaultBooleanValue( true )
    boolean getNeedsReconcile();

    CacheFilesystem setNeedsReconcile( final boolean value );
}
