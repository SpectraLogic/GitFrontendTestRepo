/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;

public final class DeleteBucketThrowsBucketNotEmptyInvocationHandler implements InvocationHandler
{
    public Object invoke( final Object proxy, final Method method, final Object[] args )
            throws Throwable
    {
        throw new RpcProxyException( "", BeanFactory.newBean( Failure.class )
                .setCode( "BUCKET_NOT_EMPTY" )
                .setHttpResponseCode( 409 )
                .setMessage( "Tried to delete a bucket with objects without specifying the force flag." ) );
    }
}
