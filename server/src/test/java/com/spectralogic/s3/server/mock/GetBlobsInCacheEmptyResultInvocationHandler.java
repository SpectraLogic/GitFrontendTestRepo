/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;

import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobsInCacheInformation;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class GetBlobsInCacheEmptyResultInvocationHandler implements InvocationHandler
{
    public Object invoke( final Object proxy, final Method method, final Object[] args )
            throws Throwable
    {
        final BlobsInCacheInformation blobsInCacheInformation =
                BeanFactory.newBean( BlobsInCacheInformation.class );
        blobsInCacheInformation.setBlobsInCache( NO_IDS );
        return new RpcResponse<>( blobsInCacheInformation );
    }
    
    
    private static final UUID[] NO_IDS = new UUID[0];
}
