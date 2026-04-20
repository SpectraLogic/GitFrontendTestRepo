/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.domain.RpcFrameworkErrorCode;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcLogger;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceUtil;


/**
 * Invocation handler for an RPC resource API (interface) that will make remote procedure calls onto an
 * RPC server.
 */
final class RpcResourceClientStubInvocationHandler implements InvocationHandler
{
    RpcResourceClientStubInvocationHandler( 
            final String resourceTypeName,
            final String resourceInstanceName,
            final RpcClient clientManager,
            final int maxDelayInMillisForResourceInstanceToComeOnline,
            final ConcurrentRequestExecutionPolicy concurrentRequestExecutionPolicy )
    {
        m_clientManager = clientManager;
        m_resourceTypeName = resourceTypeName;
        m_resourceInstanceName = resourceInstanceName;
        m_maxDelayInMillisForResourceInstanceToComeOnline = maxDelayInMillisForResourceInstanceToComeOnline;
        m_concurrentRequestExecutionPolicy = concurrentRequestExecutionPolicy;
        Validations.verifyNotNull( "Client manager", m_clientManager );
        Validations.verifyNotNull( "Resource type name", m_resourceTypeName );
        Validations.verifyNotNull( "Concurrent req execution policy", m_concurrentRequestExecutionPolicy );
    }


    public Object invoke(
            final Object proxy,
            final Method method,
            final Object[] params ) throws Throwable
    {
        if ( ConcurrentRequestExecutionPolicy.SERIALIZED == m_concurrentRequestExecutionPolicy )
        {
            synchronized ( this )
            {
                return invokeInternal( proxy, method, params );
            }
        }
        return invokeInternal( proxy, method, params );
    }


    private Object invokeInternal(
            final Object proxy,
            final Method method,
            final Object[] params ) throws Throwable
    {
        if ( RpcFuture.class == method.getReturnType() )
        {
            if ( ConcurrentRequestExecutionPolicy.SERIALIZED == m_concurrentRequestExecutionPolicy )
            {
                final RpcFuture< ? > requestInProgress = ( null == m_requestInProgress ) ?
                        null
                        : m_requestInProgress.get();
                if ( null != requestInProgress && !requestInProgress.isDone() )
                {
                    if ( requestInProgress.isTimeoutReachedByAtLeastOneClient() )
                    {
                        RpcLogger.CLIENT_LOG.warn( 
                                "Even though the concurrent request execution policy for "
                                + RpcLogger.getResourceName( m_resourceTypeName, m_resourceInstanceName )
                                + " is " + m_concurrentRequestExecutionPolicy + " and " + requestInProgress
                                + " has not completed yet, a client timed out waiting for it to complete, " 
                                + "so will permit this request execution to proceed." );
                    }
                    else
                    {
                        throw new IllegalStateException(
                                "Concurrent request execution is not permitted.  Request is in progress: " 
                                        + requestInProgress );
                    }
                }
            }
            
            int sleepMillis = 10;
            final Duration duration = new Duration();
            if ( 0 < m_maxDelayInMillisForResourceInstanceToComeOnline )
            {
                while ( !isServiceable() 
                        && m_maxDelayInMillisForResourceInstanceToComeOnline > duration.getElapsedMillis() )
                {
                    Thread.sleep( Math.min( 
                            m_maxDelayInMillisForResourceInstanceToComeOnline,
                            sleepMillis ) );
                    if ( 10000 > sleepMillis )
                    {
                        sleepMillis *= 2;
                    }
                }
            }
            if ( !isServiceable() )
            {
                throw new RpcRequestUnserviceableException( "Timed out after " + duration + " waiting for " 
                        + RpcLogger.getResourceName( m_resourceTypeName, m_resourceInstanceName ) 
                        + " to become serviceable.", null );
            }
            
            RpcResourceUtil.verifyRpcMethodInvocationDoesNotViolateNullParamContracts( method, params );
            
            final Class< ? > rpcReturnType = method.getAnnotation( RpcMethodReturnType.class ).value();
            @SuppressWarnings( "unchecked" )
            final RpcFuture< Object > retval = (RpcFuture< Object >)m_clientManager.invokeRemoteProcedureCall(
                    m_resourceTypeName, 
                    m_resourceInstanceName,
                    method.getName(), 
                    rpcReturnType,
                    ( void.class == rpcReturnType ) ? 
                            RpcMethodNullReturn.REQUIRED 
                            : ( null == method.getAnnotation( NullAllowed.class ) ) ? 
                                    RpcMethodNullReturn.DISALLOWED : RpcMethodNullReturn.OPTIONAL,
                    CollectionFactory.< Object >toList( params ) );
            if ( ConcurrentRequestExecutionPolicy.SERIALIZED == m_concurrentRequestExecutionPolicy )
            {
                m_requestInProgress = new WeakReference< RpcFuture<?> >( retval );
            }
            return retval;
        }
        
        if ( "hashCode".equals( method.getName() ) )
        {
            return Integer.valueOf( hashCode() );
        }
        if ( "equals".equals( method.getName() ) )
        {
            return Boolean.valueOf( proxy == params[ 0 ] );
        }
        if ( "isServiceable".equals( method.getName() ) )
        {
            return Boolean.valueOf( isServiceable() );
        }
        if ( "toString".equals( method.getName() ) )
        {
            return getClass().getSimpleName() + "@" + m_resourceTypeName + "-" + m_resourceInstanceName;
        }
        if ( "getConcurrentRequestExecutionPolicy".equals( method.getName() ) )
        {
            return m_concurrentRequestExecutionPolicy;
        }
        
        throw new UnsupportedOperationException(
                "RPC client proxy of " + m_resourceTypeName + " can only handle well-formed "
                + RpcResource.class.getSimpleName() + " methods.  The method invoked was non-compliant: " 
                + method + ".  Did you forget to have your method return an " 
                + RpcFuture.class.getSimpleName() + "?" );
    }
    
    
    private boolean isServiceable()
    {
        m_servicabilityLock.readLock().lock();
        try
        {
            if ( null != m_durationSinceLastPing && 60 * 5 >= m_durationSinceLastPing.getElapsedSeconds() )
            {
                return true;
            }
        }
        finally
        {
            m_servicabilityLock.readLock().unlock();
        }

        m_servicabilityLock.writeLock().lock();
        try
        {
            if ( null != m_durationSinceLastPing && 1 >= m_durationSinceLastPing.getElapsedSeconds() )
            {
                return true;
            }
            
            @SuppressWarnings( "unchecked" )
            final RpcFuture< Object > retval = (RpcFuture< Object >)m_clientManager.invokeRemoteProcedureCall(
                    m_resourceTypeName, 
                    m_resourceInstanceName,
                    "ping", 
                    void.class,
                    RpcMethodNullReturn.REQUIRED,
                    new ArrayList<>() );
            retval.get(
                    Math.max( 30000, m_maxDelayInMillisForResourceInstanceToComeOnline ),
                    TimeUnit.MILLISECONDS );
            m_durationSinceLastPing = new Duration();
            return true;
        }
        catch ( final Exception ex )
        {
            if (ex instanceof RpcRequestUnserviceableException
                    && ex.getCause() instanceof RpcProxyException
                    && ((RpcProxyException) ex.getCause()).getFailureType().getCode().equals(RpcFrameworkErrorCode.RESOURCE_TYPE_NOT_FOUND.toString())) {
                RpcLogger.CLIENT_LOG.info( "Resource " + m_resourceTypeName + "." + m_resourceInstanceName + " is not serviceable." );
            } else {
                RpcLogger.CLIENT_LOG.info( ExceptionUtil.getMessageWithSingleLineStackTrace(
                        "Resource " + m_resourceTypeName + "." + m_resourceInstanceName + " is not serviceable.",
                        ex ) );
            }
            return false;
        }
        finally
        {
            m_servicabilityLock.writeLock().unlock();
        }
    }
    
    
    private Duration m_durationSinceLastPing;
    private WeakReference< RpcFuture< ? > > m_requestInProgress;
    
    private final String m_resourceTypeName;
    private final String m_resourceInstanceName;
    
    private final RpcClient m_clientManager;
    private final int m_maxDelayInMillisForResourceInstanceToComeOnline;
    private final ConcurrentRequestExecutionPolicy m_concurrentRequestExecutionPolicy;
    private final ReentrantReadWriteLock m_servicabilityLock = new ReentrantReadWriteLock( true );
}
