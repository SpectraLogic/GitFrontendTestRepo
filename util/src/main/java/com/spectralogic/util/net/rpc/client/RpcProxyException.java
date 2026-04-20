/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import com.spectralogic.util.exception.ExceptionUtil.IgnoreObservable;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.net.rpc.domain.Failure;

/**
 * An exception propagated over JiveResource from whatever remote software component handled the remote 
 * procedure call.  This exception originated from that remote software component and is proxied in this 
 * software component for consumption by the RPC client.
 */
public final class RpcProxyException extends RpcException implements IgnoreObservable
{
    public RpcProxyException( final String requestDescription, final Failure failure )
    {
        super( new RpcFailure( failure.getCode(), failure.getHttpResponseCode() ), 
               requestDescription + " FAILED: " + failure.getMessage() );
        m_shouldBeIgnoredWhenGeneratingReadableFailureStackTrace = false;
    }
    
    
    public RpcProxyException( final RpcProxyException ex )
    {
        super( ex );
        m_shouldBeIgnoredWhenGeneratingReadableFailureStackTrace = true;
    }
    
    
    private final static class RpcFailure implements FailureType
    {
        private RpcFailure( final String code, final int httpResponseCode )
        {
            m_code = code;
            m_httpResponseCode = httpResponseCode;
        }

        public int getHttpResponseCode()
        {
            return m_httpResponseCode;
        }

        public String getCode()
        {
            return m_code;
        }
        
        private final String m_code;
        private final int m_httpResponseCode;
    } // end inner class def


    public boolean shouldBeIgnoredWhenGeneratingReadableFailureStackTrace()
    {
        return m_shouldBeIgnoredWhenGeneratingReadableFailureStackTrace;
    }
    
    
    private final boolean m_shouldBeIgnoredWhenGeneratingReadableFailureStackTrace;
}
