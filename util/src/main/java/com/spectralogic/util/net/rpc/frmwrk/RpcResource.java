/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;

public interface RpcResource
{
    /**
     * Returns true if this resource can be serviced at this time <br><br>
     * 
     * Note: This method will result in a {@link #ping()} call unless a {@link #ping()} has occurred very
     * recently
     */
    boolean isServiceable();
    
    
    /**
     * @return the policy for concurrent request execution
     */
    ConcurrentRequestExecutionPolicy getConcurrentRequestExecutionPolicy();
    
    
    /**
     * Pings the RPC resource to ensure it is alive and able to handle requests
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > ping();
}
