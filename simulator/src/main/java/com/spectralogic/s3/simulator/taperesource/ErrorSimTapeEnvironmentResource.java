package com.spectralogic.s3.simulator.taperesource;

import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public class ErrorSimTapeEnvironmentResource extends BaseRpcResource implements TapeEnvironmentResource {

    @Override
    public RpcFuture<TapeEnvironmentInformation> getTapeEnvironment() {
        return new RpcResponse<>(BeanFactory.newBean(TapeEnvironmentInformation.class));
    }

    @Override
    public RpcFuture<Long> getTapeEnvironmentGenerationNumber() {
        return new RpcResponse<>(999L);
    }

    @Override
    public RpcFuture<?> quiesceState() {
        return new RpcResponse<>(null);
    }
}
