/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.QuiescableRpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

@RpcResourceName( "UserResource" )
public interface UserResource extends QuiescableRpcResource
{
    @RpcMethodReturnType( void.class )
    RpcFuture< Object > createUser( final String name, final String emailAddress );
    
    
    @RpcMethodReturnType( boolean.class )
    RpcFuture< Boolean > exists( final String name );
    
    
    @RpcMethodReturnType( Integer.class )
    RpcFuture< Integer > getCount();
    

    @NullAllowed
    @RpcMethodReturnType( Integer.class )
    RpcFuture< Integer > getNullInteger( final Integer param );
    

    @RpcMethodReturnType( Integer.class )
    RpcFuture< Integer > getNullIllegally( @NullAllowed final Integer param );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > getNonNullIllegally( @NullAllowed final Integer param );
    
    
    int thisIsAnInvalidRpcMethodSinceItDoesNotReturnAnRpcFuture();
}
