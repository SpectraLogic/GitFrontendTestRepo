/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;


public abstract class BaseQuiescableRpcResource extends BaseRpcResource
{
    final protected void verifyNotQuiesced()
    {
        if ( m_quiesced.get() )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, 
                    getClass().getSimpleName() + " is quiesced (prepared to shut down) "
                    + "and unable to service the request made at this time.  Please try again after " 
                    + getClass().getSimpleName() + " has been restarted." );
        }
    }
    
    
    public final RpcFuture< ? > quiesceAndPrepareForShutdown( final boolean force )
    {
        LOG.warn( "Request received to quiesce " + getClass().getSimpleName() 
                  + " and prepare it for shutdown (force=" + force + ")." );
        synchronized ( m_quiesced )
        {
            m_quiesced.set( true );
            
            if ( force )
            {
                forceQuiesceAndPrepareForShutdown();
            }
    
            final String reasonWhyNotQuiesced = getCauseForNotQuiesced();
            if ( null == reasonWhyNotQuiesced )
            {
                LOG.warn( getClass().getSimpleName() + " is quiesced and prepared for shutdown." );
                return null;
            }
            throw new FailureTypeObservableException(
                    ( force ) ? GenericFailure.CONFLICT : GenericFailure.FORCE_FLAG_REQUIRED,
                    "Failed to quiesce and prepare for shutdown immediately since " 
                    + reasonWhyNotQuiesced + ".  Try again later." );
        }
    }
    
    
    protected abstract void forceQuiesceAndPrepareForShutdown();
    
    
    /**
     * @return null if the RPC resource is quiesced, else non-null to denote the cause for the RPC resource
     * not being quiesced
     */
    protected abstract String getCauseForNotQuiesced();
    
    
    private final AtomicBoolean m_quiesced = new AtomicBoolean( false );
    private final static Logger LOG = Logger.getLogger( BaseQuiescableRpcResource.class );
}
