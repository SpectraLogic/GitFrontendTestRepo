/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.strategy;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironmentManager;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeMoveStrategy;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;

public abstract class BaseTapeMoveStrategy extends BaseShutdownable implements TapeMoveStrategy
{
    protected BaseTapeMoveStrategy()
    {
        doNotLogWhenShutdown();
    }
    
    
    synchronized final public int getDest( 
            final int src,
            final Tape tape, 
            final TapeEnvironmentManager tapeEnvironmentManager )
    {
        verifyNotShutdown();
        m_tape = tape;
        m_tapeEnvironmentManager = tapeEnvironmentManager;
        
        Validations.verifyNotNull( "Tape", m_tape );
        Validations.verifyNotNull( "Tape environment manager", m_tapeEnvironmentManager );
        
        m_src = src;
        try
        { 
            m_dest = getDest();
            validationCompleted( null );
        }
        catch ( final RuntimeException ex )
        {
            validationCompleted( ex );
            throw ex;
        }
        return m_dest;
    }
    
    
    protected abstract int getDest();
    
    
    synchronized final public void addListener( final TapeMoveListener listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        if ( m_listeners.contains( listener ) )
        {
            return;
        }
        
        m_listeners.add( listener );
        
        if ( m_validationCompleted )
        {
            listener.validationCompleted( m_tape.getId(), m_validationFailure );
        }
        if ( m_moveFailed )
        {
            listener.moveFailed( m_tape.getId() );
        }
        if ( m_moveSucceeded )
        {
            listener.moveSucceeded( m_tape.getId() );
        }
    }
    
    
    synchronized final public void moveSucceeded()
    {
        verifyNotShutdown();
        shutdown();
        commitMove();
        m_moveSucceeded = true;
        for ( final TapeMoveListener listener : m_listeners )
        {
            try
            {
                listener.moveSucceeded( m_tape.getId() );
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( listener + " barfed on move succeeded event.", ex );
            }
        }
    }
    
    
    protected abstract void commitMove();


    synchronized final public void moveFailed( final Tape tape )
    {
        verifyNotShutdown();
        shutdown();
        Validations.verifyNotNull( "Tape id", tape );
        if ( null != m_tape )
        {
            rollbackMove();
            if ( !tape.getId().equals( m_tape.getId() ) )
            {
                throw new IllegalStateException( 
                        "Cannot fail move of " + tape + " when " + m_tape + " is the tape being moved." );
            }
        }
        else
        {
            m_tape = tape;
            validationCompleted( new RuntimeException( "Move failed before it could be attempted." ) );
            return;
        }
        
        m_moveFailed = true;
        for ( final TapeMoveListener listener : m_listeners )
        {
            try
            {
                listener.moveFailed( m_tape.getId() );
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( listener + " barfed on move failed event.", ex );
            }
        }
    }
    
    
    protected abstract void rollbackMove();
    
    
    synchronized private void validationCompleted( final RuntimeException failure )
    {
        m_validationCompleted = true;
        m_validationFailure = failure;
        for ( final TapeMoveListener listener : m_listeners )
        {
            try
            {
                listener.validationCompleted( m_tape.getId(), failure );
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( listener + " barfed on move begun event.", ex );
            }
        }
    }


    private boolean m_moveSucceeded;
    private boolean m_moveFailed;
    private boolean m_validationCompleted;
    private RuntimeException m_validationFailure;
    
    protected int m_src;
    protected int m_dest;
    protected Tape m_tape;
    protected TapeEnvironmentManager m_tapeEnvironmentManager;
    private final List< TapeMoveListener > m_listeners = new CopyOnWriteArrayList<>();
    
    private final static Logger LOG = Logger.getLogger( BaseTapeMoveStrategy.class );
}
