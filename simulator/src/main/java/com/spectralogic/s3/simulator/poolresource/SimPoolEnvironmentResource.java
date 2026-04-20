package com.spectralogic.s3.simulator.poolresource;

import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.rpc.pool.domain.PoolEnvironmentInformation;
import com.spectralogic.s3.common.rpc.pool.domain.PoolInformation;
import com.spectralogic.s3.common.rpc.tape.domain.BasicTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeLibraryInformation;
import com.spectralogic.s3.simulator.domain.SimLibrary;
import com.spectralogic.s3.simulator.domain.SimPool;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class SimPoolEnvironmentResource extends BaseRpcResource implements PoolEnvironmentResource {
    private final  SimPoolStateManager m_stateManager;
    private final RpcServer m_rpcServer;
    private final static Logger LOG = Logger.getLogger( SimPoolEnvironmentResource.class );
    private final AtomicLong m_poolEnvironmentGenerationNumber = new AtomicLong();
    public SimPoolEnvironmentResource(
            final SimPoolStateManager stateManager,
            final RpcServer rpcServer )
    {
        m_stateManager = stateManager;
        m_rpcServer = rpcServer;
    }
    @Override
    public RpcFuture<PoolEnvironmentInformation> getPoolEnvironment() {
        m_stateManager.simulateDelay( m_stateManager.getPoolConfig().getGetPoolEnvironmentDelay() );
        m_stateManager.hasChanged();
        final PoolEnvironmentInformation retval = BeanFactory.newBean(PoolEnvironmentInformation.class);

        final List< PoolInformation > poolInfos = new ArrayList<>();
        for ( final SimPool s : m_stateManager.getPools() )
        {
            final PoolInformation poolInfo =
                    BeanFactory.newBean( PoolInformation.class );
            //poolInfo.setPoolId( UUID.fromString(s.getPoolId() ));
            poolInfo.setPoolId( null );
            poolInfo.setGuid( s.getGuid() );
            poolInfo.setName( s.getName() );
            poolInfo.setHealth(PoolHealth.OK);
            poolInfo.setType(PoolType.NEARLINE);
            poolInfo.setPoweredOn(true);
            poolInfo.setMountpoint(s.getMountPoint());
            poolInfo.setAvailableCapacity(s.getAvailableRawCapacity());
            poolInfo.setUsedCapacity(s.getUsedRawCapacity());
            poolInfo.setTotalCapacity(s.getTotalRawCapacity());
            poolInfos.add( poolInfo );
        }

        retval.setPools( CollectionFactory.toArray( PoolInformation.class, poolInfos ) );

        return new RpcResponse<>( retval );
    }

    @Override
    public RpcFuture<PoolInformation> getPool(String poolGuid) {
        final PoolInformation poolInfo = BeanFactory.newBean( PoolInformation.class );
        final SimPool s =  m_stateManager.getPool( poolGuid );
        poolInfo.setPoolId(UUID.fromString(s.getPoolId()));
        poolInfo.setGuid(s.getGuid());
        poolInfo.setName(s.getName());
        poolInfo.setHealth(PoolHealth.OK);
        poolInfo.setType(PoolType.NEARLINE);
        poolInfo.setMountpoint(s.getMountPoint());
        poolInfo.setAvailableCapacity(s.getAvailableRawCapacity());
        poolInfo.setUsedCapacity(s.getUsedRawCapacity());
        poolInfo.setTotalCapacity(s.getTotalRawCapacity());
        return new RpcResponse<>( poolInfo );
    }

    @Override
    public RpcFuture<?> verifyPool(String poolGuid) {
        return null;
    }

    @Override
    public RpcFuture<?> formatPool(String poolGuid) {
        return null;
    }

    @Override
    public RpcFuture<?> destroyPool(String poolGuid) {
        return null;
    }

    @Override
    public RpcFuture<Long> getPoolEnvironmentGenerationNumber() {
        m_stateManager.simulateDelay( m_stateManager.getPoolConfig().getGetPoolGenerationNumberDelay() );
        return new RpcResponse<>( Long.valueOf( m_stateManager.hasChanged() ?
                m_poolEnvironmentGenerationNumber.incrementAndGet()
                : m_poolEnvironmentGenerationNumber.get() ) );
    }

    @Override
    public RpcFuture<?> quiesceState() {
        return null;
    }

    @Override
    public RpcFuture<?> powerOn(String poolGuid) {
        return null;
    }

    @Override
    public RpcFuture<?> powerOff(String poolGuid) {
        return null;
    }

    @Override
    public RpcFuture<?> takeOwnershipOfPool(String poolGuid, UUID poolId) {
        final SimPool s =  m_stateManager.getPool( poolGuid );
        s.setPoolId(poolId.toString());
        return null;
    }
}
