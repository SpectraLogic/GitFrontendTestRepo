/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcLogger;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceUtil;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

/**
 * RPC future for clients of remote procedure calls on an RPC resource.
 */
final class ClientRpcFuture implements RpcFuture< Object >
{
    ClientRpcFuture( 
            final Class< ? > returnValueType,
            final RpcMethodNullReturn rpcMethodNullReturn,
            final long requestId,
            final String requestDescription )
    {
        m_returnValueType = returnValueType;
        m_rpcMethodNullReturn = rpcMethodNullReturn;
        m_requestId = requestId;
        m_requestDescription = requestDescription;
        m_rpcInvokerThreadName = Thread.currentThread().getName();
        
        Validations.verifyNotNull( "Return value type", m_returnValueType );
        Validations.verifyNotNull( "Return value null policy", m_rpcMethodNullReturn );
        Validations.verifyNotNull( "Request description", m_requestDescription );
        
        m_work = new MonitoredWork( 
                StackTraceLogging.NONE, m_requestDescription ).withCustomLogger( RpcLogger.CLIENT_LOG );
    }
    
    
    public Object get( final Timeout timeout )
    {
        return get( timeout.getTimeout(), timeout.getUnit() );
    }
    
    
    public Object get( final long timeout, final TimeUnit unit )
    {
        waitUntilDone( timeout, unit );
        return getWithoutBlocking();
    }
    
    
    private void waitUntilDone( final long timeout, final TimeUnit unit )
    {
        try
        {
            final Duration duration = new Duration();
            if ( !m_doneLatch.await( timeout, unit ) )
            {
                m_timeoutReachedByAtLeastOneClient = true;
                throw new RpcTimeoutException(
                        "Timeout occured after " + duration + " waiting for request " + m_requestId 
                        + " to complete." );
            }
        }
        catch ( final InterruptedException ex )
        {
            throw new RpcTimeoutException( ex );
        }
    }
    
    
    public Object getWithoutBlocking()
    {
        if ( !m_done )
        {
            throw new IllegalStateException( "Cannot get the result until the request has completed." );
        }
        
        if ( null != m_exception )
        {
            if ( RpcProxyException.class.isAssignableFrom( m_exception.getClass() ) )
            {
                throw new RpcProxyException( (RpcProxyException)m_exception );
            }
            //We set the stack trace instead of throwing a new exception here to preserve the
            //type and info in the original exception. 
            m_exception.setStackTrace( new RuntimeException( m_exception ).getStackTrace() );
            throw m_exception;
        }
        
        return m_returnValue;
    }
    
    
    public boolean isDone()
    {
        return m_done;
    }
    
    
    public Boolean isSuccess()
    {
        if ( !isDone() )
        {
            return null;
        }
        
        return Boolean.valueOf( null == m_exception );
    }
    
    
    public long getRequestId()
    {
        return m_requestId;
    }
    
    
    String getRpcInvokerThreadName()
    {
        return m_rpcInvokerThreadName;
    }
    
    
    String getRequestDescription()
    {
        return m_requestDescription;
    }
    
    
    synchronized void completedWithFailure( final RuntimeException failure )
    {
        verifyNotCompleted();
        m_exception = failure;
        completed();
    }
    
    
    synchronized void completedWithResponse( final String returnValue )
    {
        verifyNotCompleted();
        
        Object tv = null;
        try
        {
            tv = MarshalUtil.getTypedValueFromNullableString( m_returnValueType, returnValue );
            if ( null == tv && RpcMethodNullReturn.DISALLOWED == m_rpcMethodNullReturn )
            {
                throw new RuntimeException( 
                        "For " + m_requestDescription 
                        + ", null was returned by the RPC method, which is illegal." );
            }
            if ( null != tv && RpcMethodNullReturn.REQUIRED == m_rpcMethodNullReturn )
            {
                throw new RuntimeException( 
                        "For " + m_requestDescription
                        + ", null wasn't returned by the RPC method, which is illegal." );
            }
            RpcResourceUtil.validateResponse( tv, GenericFailure.INTERNAL_ERROR );
        }
        catch ( final RuntimeException ex )
        {
            RpcLogger.CLIENT_LOG.error( "The response returned to the RPC client for "
                                        + getRequestDescription() + " failed validation.", ex );
            LOG.error( "The response returned to the RPC client for " + getRequestDescription() 
                       + " failed validation." );
            m_exception = ex;
        }
        
        m_returnValue = tv;
        completed();
    }
    
    
    private void verifyNotCompleted()
    {
        if ( !m_done )
        {
            return;
        }
        
        throw new IllegalStateException( 
                "Already completed with return value '" + m_returnValue + "' and exception '" 
                 + m_exception + "'." );
    }
    
    
    private void completed()
    {
        m_work.completed();
        m_done = true;
        m_doneLatch.countDown();
        
        for ( final RpcCompletedListener< Object > listener : m_listeners )
        {
            listener.remoteProcedureRequestCompleted( this );
        }
    }
    
    
    Duration getDuration()
    {
        return m_duration;
    }
    
    
    synchronized public void addRequestCompletedListener( final RpcCompletedListener< Object > listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        if ( m_done )
        {
            listener.remoteProcedureRequestCompleted( this );
        }
        else
        {
            m_listeners.add( listener );
        }
    }
    
    
    public boolean isTimeoutReachedByAtLeastOneClient()
    {
        return m_timeoutReachedByAtLeastOneClient;
    }
    
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "@" + getRequestDescription();
    }
    
    
    private volatile boolean m_timeoutReachedByAtLeastOneClient;
    private volatile boolean m_done;
    private volatile RuntimeException m_exception;
    private volatile Object m_returnValue;
    
    private final long m_requestId;
    private final String m_requestDescription;
    private final String m_rpcInvokerThreadName;
    private final Class< ? > m_returnValueType;
    private final RpcMethodNullReturn m_rpcMethodNullReturn;
    private final CountDownLatch m_doneLatch = new CountDownLatch( 1 );
    private final Duration m_duration = new Duration();
    private final MonitoredWork m_work;
    private final List< RpcCompletedListener< Object > > m_listeners = new ArrayList<>();
    
    private final static Logger LOG = Logger.getLogger( ClientRpcFuture.class );
}
