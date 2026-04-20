package com.spectralogic.s3.simulator.poolresource;

import com.spectralogic.s3.simulator.domain.SimPool;
import com.spectralogic.s3.simulator.lang.PoolConfig;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.s3.simulator.poolresource.api.SimPoolResource;

import java.util.Set;

public interface SimPoolStateManager extends SimPoolResource {
    boolean hasChanged();
    SimPool getPool(final String poolGuid);
    Set<SimPool> getPools();
    PoolConfig getPoolConfig();
    void simulateDelay( final int maxMillis );
    void simulateDelay( final int maxMillis, final String action );
    void cleanup();
}
