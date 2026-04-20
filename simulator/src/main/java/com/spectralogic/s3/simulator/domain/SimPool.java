package com.spectralogic.s3.simulator.domain;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

import java.util.UUID;

public interface SimPool extends SimpleBeanSafeToProxy, SerialNumberObservable< SimPool >,
        OnlineObservable< SimPool >{

    String STATE = "state";

    PoolState getState();

    Pool setState(final PoolState value );

    String PARTITION_ID = "partitionId";

    String getPartitionId();

    SimPool setPartitionId( final String value );

    String QUIESCED = "quiesced";

    Quiesced getQuiesced();

    SimPool setQuiesced( final Quiesced value );

    String POOL_ID = "poolId";
    String getPoolId();
    SimPool setPoolId( final String value );



    String GUID = "guid";
    String getGuid();
    SimPool setGuid( final String value );

    String NAME = "name";
    String getName();
    SimPool setName( final String value );

    String HEALTH = "health";
    String getHealth();
    SimPool setHealth( final String value );

    String MOUNT_POINT = "mountPoint";

    String getMountPoint();

    SimPool setMountPoint(final String value);

    String TOTAL_RAW_CAPACITY = "totalRawCapacity";

    long getTotalRawCapacity();

    SimPool setTotalRawCapacity( final long value );


    String AVAILABLE_RAW_CAPACITY = "availableRawCapacity";

    long getAvailableRawCapacity();

    SimPool setAvailableRawCapacity( final long value );

    String USED_RAW_CAPACITY = "usedRawCapacity";

    long getUsedRawCapacity();

    SimPool setUsedRawCapacity( final long value );

    String POWERED_ON = "poweredOn";

    boolean isPoweredOn();

    SimPool setPoweredOn( final boolean value );
}
