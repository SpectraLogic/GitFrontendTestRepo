/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.spectralogic.util.lang.reflect.ReflectUtil;

/**
 * An invocation handler that will return null.
 */
public final class NullInvocationHandler
{
    private NullInvocationHandler()
    {
        // singleton
    }
    
    
    /**
     * @return InvocationHandler
     */
    public static InvocationHandler getInstance()
    {
        return INSTANCE;
    }
    
    
    private final static Method METHOD_EQUALS = ReflectUtil.getMethod( Object.class, "equals" );
    private final static InvocationHandler INSTANCE = new InvocationHandler()
    {
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            if ( METHOD_EQUALS.equals( method ) )
            {
                return Boolean.valueOf( proxy == args[ 0 ] );
            }
            
            if ( void.class == method.getReturnType() )
            {
                return null;
            }

            if ( method.getReturnType().isPrimitive() )
            {
                return MockObjectFactory.objectForType( method.getReturnType() );
            }

            return null;
        }
    };
}
