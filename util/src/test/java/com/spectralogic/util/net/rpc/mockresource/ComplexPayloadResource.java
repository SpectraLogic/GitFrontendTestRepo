/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

@RpcResourceName( "ComplexPayloadResource" )
public interface ComplexPayloadResource extends RpcResource
{
    @RpcMethodReturnType( ComplexPayload.class )
    RpcFuture< ComplexPayload > getComplexPayload( final ComplexPayload payload );
    
    
    @RpcMethodReturnType( ComplexPayload.class )
    RpcFuture< ComplexPayload > getFirstComplexPayload( final ComplexPayload [] payload );
}
