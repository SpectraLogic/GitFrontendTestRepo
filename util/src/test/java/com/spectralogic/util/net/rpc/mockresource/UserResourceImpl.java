/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseQuiescableRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class UserResourceImpl extends BaseQuiescableRpcResource implements UserResource
{
    synchronized public RpcFuture< Object > createUser( final String name, final String emailAddress )
    {
        verifyNotQuiesced();
        if ( exists( name ).getWithoutBlocking().booleanValue() )
        {
            throw new IllegalArgumentException( "User already exists." );
        }
        m_users.put( name, emailAddress );
        return null;
    }
    
    
    public RpcFuture< Boolean > exists( final String name )
    {
        return new RpcResponse<>( Boolean.valueOf( m_users.containsKey( name ) ) );
    }
    
    
    public RpcFuture< Integer > getCount()
    {
        return new RpcResponse<>( Integer.valueOf( m_users.size() ) );
    }
    
    
    public int thisIsAnInvalidRpcMethodSinceItDoesNotReturnAnRpcFuture()
    {
        return 1;
    } 


    public RpcFuture< Integer > getNullInteger( final Integer param )
    {
        return null;
    }


    public RpcFuture< Integer > getNullIllegally( final Integer param )
    {
        return null;
    }
    
    
    public RpcFuture< ? > getNonNullIllegally( @NullAllowed final Integer param )
    {
        return new RpcResponse<>( Integer.valueOf( 1 ) );
    }


    @Override
    protected void forceQuiesceAndPrepareForShutdown()
    {
        m_causeForNotQuiesced = null;
    }


    @Override
    protected String getCauseForNotQuiesced()
    {
        return m_causeForNotQuiesced;
    }

    
    private volatile String m_causeForNotQuiesced = "force quiesce not received yet";
    private final Map< String, String > m_users = new ConcurrentHashMap<>();
}
