/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;


/**
 * An invocation handler that will return non-null objects wherever possible upon invocation.
 */
public final class NonNullInvocationHandler
{    
    private NonNullInvocationHandler()
    {
        // singleton
    }
    
    
    public static InvocationHandler getInstance()
    {
        return INSTANCE;
    }
    
    
    private static final InvocationHandler INSTANCE = new InvocationHandler()
    {
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            return MockObjectFactory.objectForType( method.getReturnType() );
        }
    };
}
