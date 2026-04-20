/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.util.concurrent.TimeUnit;

import com.spectralogic.util.net.rpc.client.RpcCompletedListener;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;

public final class MockRpcFailureResponse< T > implements RpcFuture< T >
{
    public MockRpcFailureResponse( final RuntimeException ex )
    {
        m_ex = ex;
    }
    
    
    public boolean isDone()
    {
        return true;
    }

    
    public Boolean isSuccess()
    {
        return Boolean.FALSE;
    }

    
    public boolean isTimeoutReachedByAtLeastOneClient()
    {
        return false;
    }

    
    public long getRequestId()
    {
        return 0;
    }

    
    public T getWithoutBlocking()
    {
        throw m_ex;
    }

    
    public T get( final Timeout timeout )
    {
        throw m_ex;
    }

    
    public T get( final long timeout, final TimeUnit unit )
    {
        throw m_ex;
    }

    
    public void addRequestCompletedListener( final RpcCompletedListener< T > listener )
    {
        throw new UnsupportedOperationException( "No code written." );
    }

    
    private final RuntimeException m_ex;
}
