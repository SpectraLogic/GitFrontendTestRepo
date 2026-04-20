package com.spectralogic.s3.server.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;

import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobsInCacheInformation;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public class GetBlobsInCacheResultInvocationHandler implements InvocationHandler
{
    public GetBlobsInCacheResultInvocationHandler( UUID id) {
        m_id = id;
    }

    public Object invoke(final Object proxy, final Method method, final Object[] args )
            throws Throwable
    {
        final BlobsInCacheInformation blobsInCacheInformation =
                BeanFactory.newBean( BlobsInCacheInformation.class );
        blobsInCacheInformation.setBlobsInCache(new UUID[]{m_id});
        return new RpcResponse<>( blobsInCacheInformation );
    }


    private final UUID m_id;
}
