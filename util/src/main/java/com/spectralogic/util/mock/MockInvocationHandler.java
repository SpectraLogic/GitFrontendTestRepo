/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;

/**
 * An <code>InvocationHandler</code> for many basic needs (see static factory methods for basic
 * invocation handlers this class can produce).
 */
public final class MockInvocationHandler implements InvocationHandler
{
    /**
     * @param canHandleDeterminer - Determines whether or not this invocation handler can handle 
     *                              a given invocation
     * @param ih                  - The invocation handler to be invoked if the canHandleDeterminer
     *                              says we can handle the invocation
     * @param decoratedIh         - The invocation handler to be invoked if the canHandleDeterminer 
     *                              says we cannot handle the invocation
     */
    private MockInvocationHandler(
            final CanHandleDeterminer canHandleDeterminer,
            final InvocationHandler ih,
            final InvocationHandler decoratedIh )
    {
        m_canHandleDeterminer = canHandleDeterminer;
        m_ih = ih;
        m_decoratedIh = (null != decoratedIh) ? decoratedIh : NullInvocationHandler.getInstance();

        Validations.verifyNotNull( "Invocation handler", m_ih );
    }
    
    
    private interface CanHandleDeterminer
    {
        boolean canHandle( final Object proxy, final Method method, final Object[] args);
    } // end inner class def
    

    /**
     * The <code>ih</code> will be invoked whenever the <code>method</code> is called, and the
     * <code>decoratedIh</code> will be invoked otherwise.
     */
    public static MockInvocationHandler forMethod(
            final Method method, 
            final InvocationHandler ih,
            final InvocationHandler decoratedIh )
    {
        Validations.verifyNotNull( "Method", method );
        
        final CanHandleDeterminer determiner = new CanHandleDeterminer()
        {
            public boolean canHandle( final Object proxy, final Method m, final Object[] args )
            {
                return m.equals( method );
            }
        };
        
        return new MockInvocationHandler( determiner, ih, decoratedIh );
    }
    
    
    public static MockInvocationHandler forToString( final String response )
    {
        return forMethod( 
                ReflectUtil.getMethod( Object.class, "toString" ),
                new ConstantResponseInvocationHandler( response ),
                null );
    }
    
    
    /**
     * The <code>ih</code> will be invoked whenever the method's return type is assignable to 
     * <code>returnType</code>, and <code>decoratedIh</code> will be invoked otherwise.
     */
    public static MockInvocationHandler forReturnType(
            final Class< ? > returnType, 
            final InvocationHandler ih,
            final InvocationHandler decoratedIh )
    {
        Validations.verifyNotNull( "Return type", returnType );
        
        final CanHandleDeterminer determiner = new CanHandleDeterminer()
        {
            public boolean canHandle( final Object proxy, final Method method, final Object[] args )
            {
                return returnType.isAssignableFrom( method.getReturnType() );
            }
        };
        
        return new MockInvocationHandler( determiner, ih, decoratedIh);
    }
    
    
    @Override
    public Object invoke(
            final Object proxy, 
            final Method method, 
            final Object[] args) throws Throwable
    {
        if ( m_canHandleDeterminer.canHandle( proxy, method, args ) )
        {
            return m_ih.invoke(proxy, method, args);
        }
        if ( null == m_decoratedIh )
        {
            throw new RuntimeException( "Cannot handle method invocation." );
        }
        
        return m_decoratedIh.invoke( proxy, method, args );
    }
    
    
    private final CanHandleDeterminer m_canHandleDeterminer;
    private final InvocationHandler m_ih;
    private final InvocationHandler m_decoratedIh;
}
