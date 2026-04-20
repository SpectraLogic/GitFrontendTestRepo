/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.spectralogic.util.io.lang.HeapDumper;
import com.spectralogic.util.lang.Validations;

public final class MemoryHogDetector
{
    public MemoryHogDetector( final int intervalBetweenDetectionCyclesInMillis )
    {
        m_timer.scheduleAtFixedRate( 
                new MemoryHogDetectionTimerTask(),
                intervalBetweenDetectionCyclesInMillis, 
                intervalBetweenDetectionCyclesInMillis );
        LOG.info( this.getClass().getSimpleName()
                + " started and will check for memory hogs every " 
                + intervalBetweenDetectionCyclesInMillis
                + "ms." );
        HeapDumper.ensureInitialized();
    }
    
    
    private final class MemoryHogDetectionTimerTask extends TimerTask
    {
        @Override
        public void run()
        {
            try
            {
                runInternal();
            }
            catch ( final Exception ex )
            {
                LOG.error( this.getClass().getName() + " failed to execute successfully.", ex );
            }
        }
        
        private void runInternal()
        {
            for ( final MemoryHogListener listener : m_listeners )
            {
                listener.monitorMemoryUsage();
            }
        }
    } // end inner class def
    
    
    public void addMemoryHogListener( final MemoryHogListener listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        m_listeners.add( listener );
    }
    
    
    private final List< MemoryHogListener > m_listeners =
        new CopyOnWriteArrayList<>();
    private final Timer m_timer = new Timer( this.getClass().getSimpleName(), true );
    
    private final static Logger LOG = Logger.getLogger( MemoryHogDetector.class );
}
