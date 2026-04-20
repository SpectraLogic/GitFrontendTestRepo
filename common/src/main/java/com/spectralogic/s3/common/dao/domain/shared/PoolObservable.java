package com.spectralogic.s3.common.dao.domain.shared;

import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;

public interface PoolObservable < T > extends NameObservable < T >
{
    String GUID = "guid";
    
    String getGuid();
    
    T setGuid( final String value );
    
    
    String MOUNTPOINT = "mountpoint";
    
    /**
     * The location in the file system where the pool is mounted.
     */
    String getMountpoint();
    
    T setMountpoint( final String value );
    
    
    String AVAILABLE_CAPACITY = "availableCapacity";
    
    /**
     * @return {@link #getTotalCapacity()} - {@link #getUsedCapacity()} - {@link #getReservedCapacity()},
     * which is the capacity available for writing data
     */
    long getAvailableCapacity();
    
    T setAvailableCapacity( final long value );
    
    
    String RESERVED_CAPACITY = "reservedCapacity";
    
    /**
     * @return the capacity reserved for filesystem overhead and performance (for example, a filesystem may
     * become non-performant if the used capacity exceeds 90% of the total capacity, in which case the 
     * reserved capacity should be at least 10% of the total capacity)
     */
    long getReservedCapacity();
    
    T setReservedCapacity( final long value );
    
    
    String USED_CAPACITY = "usedCapacity";
    
    /**
     * @return the pool's used capacity as reported by the underlying filesystem
     */
    long getUsedCapacity();
    
    T setUsedCapacity( final long value );
    
    
    String TOTAL_CAPACITY = "totalCapacity";
    
    /**
     * @return the pool's total usable capacity as reported by the underlying filesystem
     */
    long getTotalCapacity();
    
    T setTotalCapacity( final long value );
    
    
    String TYPE = "type";
    
    PoolType getType();
    
    T setType( final PoolType value );
    
    
    String HEALTH = "health";
    
    PoolHealth getHealth();
    
    T setHealth( final PoolHealth value );
    
    
    String POWERED_ON = "poweredOn";
    
    boolean isPoweredOn();
    
    T setPoweredOn( final boolean value );
}
