/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;

/**
 * Base class for resource implementations for servicing RPC requests.
 */
public abstract class BaseRpcResource implements RpcResource
{
    public boolean isServiceable()
    {
        throw new UnsupportedOperationException(
                "Method only applies to client / proxy rpc resource instances." );
    }
    
    
    public RpcFuture< ? > ping()
    {
        return null;
    }


    public ConcurrentRequestExecutionPolicy getConcurrentRequestExecutionPolicy()
    {
        throw new UnsupportedOperationException(
                "Method only applies to client / proxy rpc resource instances." );
    }
}
