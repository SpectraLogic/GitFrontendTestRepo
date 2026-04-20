/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;

import java.util.concurrent.TimeUnit;

import com.spectralogic.util.net.rpc.client.RpcCompletedListener;

public interface RpcFuture< R >
{
    /**
     * @return true if either the return value or exception has arrived and is ready for client processing
     */
    boolean isDone();
    
    
    /**
     * @return TRUE if done and successful, FALSE if done and error, and null if not done yet
     */
    Boolean isSuccess();
    
    
    /**
     * @return TRUE if at least one client of the {@link RpcFuture} has timed out waiting for a response
     */
    boolean isTimeoutReachedByAtLeastOneClient();
    
    
    /**
     * @return the id of the request that this future is for
     */
    long getRequestId();
    

    /**
     * Return the result or throw an exception if the result has not yet arrived, or if the result is an 
     * exception
     */
    R getWithoutBlocking();
    
    
    /**
     * Wait up to the timeout, then return the result (or throw an exception if the timeout is reached, or
     * if the result is an exception)
     */
    R get( Timeout timeout );
    

    /**
     * Wait up to the timeout, then return the result (or throw an exception if the timeout is reached, or
     * if the result is an exception)
     */
    R get( final long timeout, final TimeUnit unit );
    
    
    /**
     * @param listener that will be notified once the RPC request has completed, or if it already has, the
     * listener will be notified upon making this call
     */
    void addRequestCompletedListener( final RpcCompletedListener< R > listener );
    
    
    public enum Timeout
    {
        /** 10 minutes */
        DEFAULT( 10, TimeUnit.MINUTES ),
        
        /** 2 hours */
        LONG( 2, TimeUnit.HOURS ),
        
        /** 12 hours */
        VERY_LONG( 12, TimeUnit.HOURS ),
        ;
        
        private Timeout( final long timeout, final TimeUnit unit )
        {
            m_timeout = timeout;
            m_unit = unit;
        }
        
        public long getTimeout()
        {
            return m_timeout;
        }
        
        public TimeUnit getUnit()
        {
            return m_unit;
        }
        
        private final long m_timeout;
        private final TimeUnit m_unit;
    } // end inner class def
}
