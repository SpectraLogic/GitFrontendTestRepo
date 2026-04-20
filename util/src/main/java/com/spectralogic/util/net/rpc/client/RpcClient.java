/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.util.List;

import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.shutdown.Shutdownable;


/**
 * Provides client-side RPC request capabilities for an RPC resource.
 */
public interface RpcClient extends Shutdownable
{
    /**
     * @return an RPC client stub to invoke methods that exist on other software components. <br><br>
     * 
     * When RPC methods on the resource are called, if a suitable resource instance to service the request 
     * cannot be found, the RPC method call will block for several seconds waiting for one to come online
     * before throwing an {@link RpcRequestUnserviceableException}.
     */
    < T extends RpcResource > T getRpcResource( 
            final Class< T > rpcResourceApi, 
            final String resourceInstanceName,
            final ConcurrentRequestExecutionPolicy concurrentRequestExecutionPolicy );
    
    
    /**
     * @return an RPC client stub to invoke methods that exist on other software components. <br><br>
     * 
     * When RPC methods on the resource are called, if a suitable resource instance to service the request 
     * cannot be found, the RPC method call will block for up to 
     * <code>maxDelayInMillisForResourceInstanceToComeOnline</code> millis for one to come online before 
     * throwing an {@link RpcRequestUnserviceableException}.
     */
    < T extends RpcResource > T getRpcResource(
            final Class< T > rpcResourceApi,
            final String resourceInstanceName,
            final ConcurrentRequestExecutionPolicy concurrentRequestExecutionPolicy,
            final int maxDelayInMillisForResourceInstanceToComeOnline );
    
    
    /**
     * Invokes the specified remote procedure.<br><br>
     * 
     * <b><font color = red>
     * This method should never be called directly by clients.  Always use {@link #getRpcResource} and make 
     * RPC calls on the proxy interfaces instead.
     * </font></b>
     */
    RpcFuture< ? > invokeRemoteProcedureCall( 
            final String resourceTypeName,
            final String resourceInstanceName,
            final String methodName,
            final Class< ? > methodReturnType,
            final RpcMethodNullReturn rpcMethodNullReturn,
            final List< Object > methodArgs ) throws RpcRequestUnserviceableException;
    
    
    /**
     * When an RPC request completes either successfully or due to an error, the listener will be notified of 
     * that requests's future.
     */
    void addRpcCompletedListener( final RpcCompletedListener< Object > listener );
}
