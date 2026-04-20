/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.listener;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;


public abstract class BaseTapeMoveListener implements TapeMoveListener
{
    final public void validationCompleted( final UUID tapeId, final RuntimeException failure )
    {
        m_validationFailure = failure;
        m_validationLatch.countDown();
        
        if ( null == failure )
        {
            validationSucceeded( tapeId );
        }
        else
        {
            validationFailed( tapeId, failure );
        }
    }
    
    
    final public void waitUntilValidated()
    {
        try
        {
            m_validationLatch.await();
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        if ( null != m_validationFailure )
        {
            throw m_validationFailure;
        }
    }
    
    
    protected void validationSucceeded( @SuppressWarnings( "unused" ) final UUID tapeId )
    {
        // empty
    }
    
    
    protected void validationFailed( 
            @SuppressWarnings( "unused" ) final UUID tapeId, 
            @SuppressWarnings( "unused" ) final RuntimeException failure )
    {
        // empty
    }
    
    
    private volatile RuntimeException m_validationFailure;
    private final CountDownLatch m_validationLatch = new CountDownLatch( 1 );
}
