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
 * An invocation handler that returns a constant response.
 */
public final class ConstantResponseInvocationHandler implements InvocationHandler
{
    public ConstantResponseInvocationHandler( final Object constantResponse )
    {
        m_constantResponse = constantResponse;
    }


    @Override
    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
    {
        return m_constantResponse;
    }
    
    
    private final Object m_constantResponse;
}
