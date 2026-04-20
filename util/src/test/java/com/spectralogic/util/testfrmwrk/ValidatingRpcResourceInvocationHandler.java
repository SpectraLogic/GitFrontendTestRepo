/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.mock.NullInvocationHandler;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceUtil;

public final class ValidatingRpcResourceInvocationHandler implements InvocationHandler
{
    public ValidatingRpcResourceInvocationHandler( final InvocationHandler decoratedIh )
    {
        m_decoratedIh = ( null == decoratedIh ) ? NullInvocationHandler.getInstance() : decoratedIh;
    }
    
    
    public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
    {
        if ( RpcFuture.class.isAssignableFrom( method.getReturnType() ) )
        {
            validateRpcMethodCall( method, args );
        }
        return m_decoratedIh.invoke( proxy, method, args );
    }
    
    
    private void validateRpcMethodCall( final Method method, final Object [] args )
    {
        if ( null == method.getAnnotation( RpcMethodReturnType.class ) )
        {
            throw new RuntimeException( 
                    "RPC method " + method + " is missing annotation " 
                    + RpcMethodReturnType.class.getSimpleName() + "." );
        }
        RpcResourceUtil.verifyRpcMethodInvocationDoesNotViolateNullParamContracts( method, args );
        if ( null != args )
        {
            for ( final Object arg : args )
            {
                RpcResourceUtil.validateResponse( arg, GenericFailure.INTERNAL_ERROR );
            }
        }
    }
    
    
    private final InvocationHandler m_decoratedIh;
}
