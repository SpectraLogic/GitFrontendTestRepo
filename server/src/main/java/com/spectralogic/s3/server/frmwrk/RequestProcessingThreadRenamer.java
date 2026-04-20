/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.frmwrk;

import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;

public final class RequestProcessingThreadRenamer extends BaseShutdownable implements Runnable
{
    public RequestProcessingThreadRenamer( final long requestNumber )
    {
        m_thread = Thread.currentThread();
        m_originalName = m_thread.getName();
        m_requestNumber = requestNumber;
        doNotLogWhenShutdown();
    }
    
    
    public void run()
    {
        if ( m_run.getAndSet( true ) )
        {
            throw new IllegalStateException( "Already run." );
        }
        m_thread.setName( "REQ#" + m_requestNumber );
        addShutdownListener( new RenameThreadBackToOriginalName() );
    }
    
    
    private final class RenameThreadBackToOriginalName extends CriticalShutdownListener
    {
        @Override
        public void shutdownOccurred()
        {
            m_thread.setName( m_originalName );
        }
    } // end inner class def
    
    
    private final long m_requestNumber;
    private final String m_originalName;
    private final Thread m_thread;
    private final AtomicBoolean m_run = new AtomicBoolean( false );
}
