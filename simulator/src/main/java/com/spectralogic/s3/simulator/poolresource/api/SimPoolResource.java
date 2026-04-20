package com.spectralogic.s3.simulator.poolresource.api;

import com.spectralogic.s3.common.rpc.pool.domain.PoolEnvironmentInformation;
import com.spectralogic.s3.common.rpc.pool.domain.PoolInformation;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimPool;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;
import com.spectralogic.util.net.rpc.server.RpcResponse;

import java.util.UUID;

@RpcResourceName( "Pool" )
public interface SimPoolResource extends RpcResource {

    @RpcMethodReturnType( SimPool.class )
    RpcFuture< SimPool > addPool( final SimPool pool );


    @RpcMethodReturnType( SimPool.class )
    RpcFuture< SimPool > updatePool(
            final String serialNumber, final String message, final boolean online );
}
