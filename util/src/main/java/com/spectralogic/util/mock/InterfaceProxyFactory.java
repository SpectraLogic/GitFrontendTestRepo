/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Arrays;

import com.spectralogic.util.lang.Validations;

/**
 * Creates proxies out of interfaces.
 */
public final class InterfaceProxyFactory
{
    private InterfaceProxyFactory()
    {
        // singleton
    }
    
    
    /**
     * @return proxy of the class
     */
    public static < B > B getProxy(
            final Class< B > clazz, 
            final InvocationHandler invocationHandler )
    {
        Validations.verifyNotNull( "Class", clazz );
        
        @SuppressWarnings( "unchecked" )
        final B retval = (B)Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class< ? >[] { clazz },
                ( null != invocationHandler ) ? 
                        invocationHandler 
                        : NullInvocationHandler.getInstance() );
        return retval;
    }
    
    
    public static < T > Class< T > getType( final Class< T > clazz )
    {
        Validations.verifyNotNull( "Class", clazz );

        if ( Proxy.isProxyClass( clazz ) )
        {
            if ( 1 != clazz.getInterfaces().length )
            {
                throw new UnsupportedOperationException( 
                        "Proxy doesn't implement a single interface: " 
                                + Arrays.toString( clazz.getInterfaces() ) );
            }
            @SuppressWarnings( "unchecked" )
            final Class< T > castedClass = (Class< T >)clazz.getInterfaces()[ 0 ];
            return castedClass;
        }
        return clazz;
    }
}
