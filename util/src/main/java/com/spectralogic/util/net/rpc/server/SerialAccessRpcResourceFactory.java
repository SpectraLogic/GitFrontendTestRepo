/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;

/**
 * Generates server-side RPC resources that ensure serial access across all public methods.
 */
public final class SerialAccessRpcResourceFactory
{
    private SerialAccessRpcResourceFactory()
    {
        // singleton
    }
    
    
    public static < T extends RpcResource > T asSerialResource( final Class< T > clazz, final T rpcResource )
    {
        Validations.verifyNotNull( "RPC resource", rpcResource );
        final T retval = InterfaceProxyFactory.getProxy(
                clazz,
                new SerialAccessRpcResourceInvocationHandler( rpcResource ) );
        return retval;
    }
    
    
    private static final class SerialAccessRpcResourceInvocationHandler implements InvocationHandler
    {
        private SerialAccessRpcResourceInvocationHandler( final Object rpcResource )
        {
            m_rpcResource = rpcResource;
        }

        
        public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
        {
            if ( Object.class == method.getDeclaringClass() )
            {
                return method.invoke( m_rpcResource, args );
            }
            
            try
            {
                if ( m_invocationInProgress.getAndSet( true ) )
                {
                    throw new IllegalStateException( 
                            "Only serial access to " + m_rpcResource 
                            + " is permitted (" + m_currentInvocation + " is still being invoked)." );
                }
                m_currentInvocation = method.toString();
                return method.invoke( m_rpcResource, args );
            }
            finally
            {
                m_invocationInProgress.set( false );
            }
        }
        
        
        private volatile String m_currentInvocation;
        private final AtomicBoolean m_invocationInProgress = new AtomicBoolean( false );
        private final Object m_rpcResource;
    } // end inner class def
}
