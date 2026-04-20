/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;

final class GlobalRpcCompletedEventDispatcher
{
    void addListener( final RpcCompletedListener< Object > listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        if ( m_listeners.contains( listener ) )
        {
            return;
        }
        
        m_listeners.add( listener );
    }
    
    
    void dispatchRpcCompletedEvent( final RpcFuture< ? > future )
    {
        if ( !future.isDone() )
        {
            throw new IllegalStateException( 
                    "You can only dispatch an RPC completed event for futures that are completed." );
        }
        
        for ( final RpcCompletedListener< Object > listener : m_listeners )
        {
            try
            {
                @SuppressWarnings( "unchecked" )
                final RpcFuture< Object > castedFuture = (RpcFuture< Object >)future;
                listener.remoteProcedureRequestCompleted( castedFuture );
            }
            catch ( final Exception ex )
            {
                LOG.error( listener + " failed to handle " + future + ".", ex );
            }
        }
    }
    
    
    private final List< RpcCompletedListener< Object > > m_listeners = new CopyOnWriteArrayList<>();
    private final static Logger LOG = Logger.getLogger( GlobalRpcCompletedEventDispatcher.class );
}
