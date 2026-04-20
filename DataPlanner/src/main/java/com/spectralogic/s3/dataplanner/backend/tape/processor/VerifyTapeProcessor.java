/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.task.VerifyTapeTask;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;

public final class VerifyTapeProcessor
{
    public VerifyTapeProcessor(
            final TapeBlobStoreProcessor processor,
            final BeansServiceManager serviceManager,
            final DiskManager diskManager,
            final TapeFailureManagement tapeFailureManagement,
            final int delayInMillis)
    {
        m_processor = processor;
        m_serviceManager = serviceManager;
        m_diskManager = diskManager;
        m_tapeFailureManagement = tapeFailureManagement;
        m_executor = new ThrottledRunnableExecutor<>( delayInMillis, null );
        
        Validations.verifyNotNull( "Processor", m_processor );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
    }
    
    
    public void schedule()
    {
        m_executor.add( m_worker );
    }
    
    
    private final class TapeVerificationScheduler implements ThrottledRunnable
    {
        public void run( final RunnableCompletionNotifier completionNotifier )
        {
            try
            {
                final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                        Require.not( Require.beanPropertyEquals( Tape.VERIFY_PENDING, null ) ) ).toSet();
                if ( !tapes.isEmpty() )
                {
                    LOG.info( tapes.size() + " tapes are queued for verification." );
                    for ( final Tape tape : tapes )
                    {
                        attemptToVerify( tape );
                    }
                    LOG.info( "Finished scheduling verify tasks for " + tapes.size() 
                              + " tapes queued for verification." );
                }
            }
            finally
            {
                completionNotifier.completed();
            }
        }
    } // end inner class def
    
    
    synchronized private void attemptToVerify( final Tape tape )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            if ( null == tape.getVerifyPending() )
            {
                return;
            }
            
            scheduleVerify( tape.getVerifyPending(), tape.getId() );
        }
    }
    
    
    private void scheduleVerify( final BlobStoreTaskPriority priority, final UUID tapeId )
    {
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        if ( !tape.getType().canContainData() )
        {
            cancelVerify( tape, tape.getType() + " cannot contain data" );
            return;
        }
        
        if ( !tape.getState().isPhysicallyPresent()
                || !tape.getState().isLoadIntoDriveAllowed() )
        {
            cancelVerify( tape, "cannot verify tape in state " + tape.getState() );
            return;
        }

        if ( !m_processor.getTapeTasks().tryPriorityUpdate(
                tape.getId(), VerifyTapeTask.class, priority, false, false ) ) {
            m_processor.getTapeTasks().addStaticTask(
                    new VerifyTapeTask(priority, tape.getId(), m_diskManager, m_tapeFailureManagement, m_serviceManager));
        }
    }
    
    
    private void cancelVerify( final Tape tape, final String cause )
    {
        LOG.warn( "Cannot verify tape " + tape.getId() + " (" + tape.getBarCode() 
                  + ") since " + cause + "." );
        m_serviceManager.getService( TapeService.class ).update(
                tape.setVerifyPending( null ),
                Tape.VERIFY_PENDING );
    }
    
    
    private final TapeBlobStoreProcessor m_processor;
    private final BeansServiceManager m_serviceManager;
    private final DiskManager m_diskManager;
    private final ThrottledRunnableExecutor< TapeVerificationScheduler > m_executor;
    private final TapeVerificationScheduler m_worker = new TapeVerificationScheduler();
    private final TapeFailureManagement m_tapeFailureManagement;
    
    private final static Logger LOG = Logger.getLogger( VerifyTapeProcessor.class );
}
