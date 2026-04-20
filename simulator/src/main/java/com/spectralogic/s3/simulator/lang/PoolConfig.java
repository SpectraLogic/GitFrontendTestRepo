package com.spectralogic.s3.simulator.lang;


import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface PoolConfig extends SimpleBeanSafeToProxy {
    String NUM_POOLS = "numPools";

    int getNumPools();

    PoolConfig setNumPools(final int value);

    String GET_POOL_GENERATION_NUMBER_DELAY = "getPoolGenerationNumberDelay";

    int getGetPoolGenerationNumberDelay();

    PoolConfig setGetPoolGenerationNumberDelay(final int value);

    String GET_POOL_ENVIRONMENT_DELAY = "getPoolEnvironmentDelay";
    int getGetPoolEnvironmentDelay();
    PoolConfig setGetPoolEnvironmentDelay(final int value);

    String MOUNT_POINT = "mountPoint";

    String getMountPoint();

    PoolConfig setMountPoint(final String value);

}
