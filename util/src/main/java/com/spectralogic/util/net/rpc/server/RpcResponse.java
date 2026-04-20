/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import java.util.concurrent.TimeUnit;

import com.spectralogic.util.net.rpc.client.RpcCompletedListener;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;

/**
 * RPC future for describing the response that has been made and needs to be returned to the RPC client that
 * made the request.
 */
public final class RpcResponse< R > implements RpcFuture< R >
{
    public RpcResponse()
    {
        m_response = null;
    }
    
    
    public RpcResponse( final R response )
    {
        m_response = response;
    }
    

    public boolean isDone()
    {
        return true;
    }
    
    
    public Boolean isSuccess()
    {
        return Boolean.TRUE;
    }

    
    public long getRequestId()
    {
        throw new UnsupportedOperationException( "Not applicable." );
    }

    
    public R getWithoutBlocking()
    {
        return m_response;
    }

    
    public R get( final Timeout timeout )
    {
        return m_response;
    }

    
    public R get( long timeout, TimeUnit unit )
    {
        return m_response;
    }
    
    
    public void addRequestCompletedListener( final RpcCompletedListener< R > listener )
    {
        listener.remoteProcedureRequestCompleted( this );
    }
    
    
    public boolean isTimeoutReachedByAtLeastOneClient()
    {
        return false;
    }
    
    
    private final R m_response;
}
