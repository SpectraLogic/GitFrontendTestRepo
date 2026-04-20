/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class ComplexPayloadResourceImpl extends BaseRpcResource implements ComplexPayloadResource
{
    public RpcFuture< ComplexPayload > getComplexPayload( final ComplexPayload payload )
    {
        return new RpcResponse<>( payload );
    }

    
    public RpcFuture< ComplexPayload > getFirstComplexPayload( final ComplexPayload[] payload )
    {
        if ( null == payload || 0 == payload.length )
        {
            return null;
        }
        return new RpcResponse<>( payload[ 0 ] );
    }
}
