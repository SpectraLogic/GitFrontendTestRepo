/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.listener.BaseTapeMoveListener;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;
import com.spectralogic.util.thread.wp.SystemWorkPool;

abstract class BaseTapeMovingProcessor extends BaseShutdownable
{
    protected BaseTapeMovingProcessor( 
            final BeansServiceManager serviceManager, 
            final TapeEnvironment processor,
            final int intervalInMillisBetweenWorkIterationRuns,
            final WhereClause tapeFilterForTapesToInitiateMoveFor,
            final BeanComparator< Tape > tapeComparatorForSelectingNextTapeToMove,
            final String actionVerbInPresentTense,
            final ElementAddressType destinationElementAddressType,
            final boolean alwaysReschedule )
    {
        m_serviceManager = serviceManager;
        m_tapeEnvironment = processor;
        m_executor = new ThrottledRunnableExecutor<>( intervalInMillisBetweenWorkIterationRuns, null );
        m_tapeFilterForTapesToInitiateMoveFor = tapeFilterForTapesToInitiateMoveFor;
        m_tapeComparatorForSelectingNextTapeToMove = tapeComparatorForSelectingNextTapeToMove;
        m_actionVerbInPresentTense = actionVerbInPresentTense;
        m_destinationElementAddressType = destinationElementAddressType;
        m_alwaysReschedule = alwaysReschedule;
        
        Validations.verifyNotNull( "brm", m_serviceManager );
        Validations.verifyNotNull( "Environment", m_tapeEnvironment);
        Validations.verifyNotNull( "Tape filter", m_tapeFilterForTapesToInitiateMoveFor );
        Validations.verifyNotNull( "Tape comparator", m_tapeComparatorForSelectingNextTapeToMove );
        Validations.verifyNotNull( "Action verb", m_actionVerbInPresentTense );
        Validations.verifyNotNull( "Destination element address type", m_destinationElementAddressType );
        
        schedule();
    }
    
    
    private final static class WorkIterationRunner implements ThrottledRunnable
    {
        private WorkIterationRunner( final BaseTapeMovingProcessor processor )
        {
            m_processor = new WeakReference<>( processor );
        }
        
        public void run( final RunnableCompletionNotifier completionNotifier )
        {
            try
            {
                run();
            }
            finally
            {
                completionNotifier.completed();
            }
        }
        
        synchronized private void run()
        {
            final BaseTapeMovingProcessor processor = m_processor.get();
            if ( null == processor )
            {
                return;
            }
            
            processor.new WorkIteration().run();
        }
        
        private final WeakReference< BaseTapeMovingProcessor > m_processor;
    } // end inner class def
    
    
    private final class WorkIteration implements Runnable
    {
        public void run()
        {
            verifyNotShutdown();
            try {
	            Thread.currentThread().setName( BaseTapeMovingProcessor.this.getClass().getSimpleName() );
	            final Set< Tape > actionableTapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
	                    m_tapeFilterForTapesToInitiateMoveFor ).toSet();
	            
	            final Map< UUID, Set< Tape > > map = new HashMap<>();
	            for ( final Tape tape : actionableTapes )
	            {
	                if ( !map.containsKey( tape.getPartitionId() ) )
	                {
	                    map.put( tape.getPartitionId(), new HashSet< Tape >() );
	                }
	                map.get( tape.getPartitionId() ).add( tape );
	            }
	            
	            for ( final Map.Entry< UUID, Set< Tape > > e : map.entrySet() )
	            {
	                runInternal( e.getKey(), e.getValue() );
	            }

            }
            finally
            {
            	if ( m_alwaysReschedule )
            	{
            		schedule();
            	}
            }
            
            cleanUpStallFailuresThatAreNoLongerApplicable(
                    m_partitionsStalledDueToMoveFailure, 
                    m_partitionsStalledDueToCannotInitiateMoveForPartition );
        }
        
        
        private void runInternal( final UUID partitionId, final Set< Tape > setOfActionableTapes )
        {
            if ( !canInitiateMoveForPartition( partitionId, setOfActionableTapes ) )
            {
                m_partitionsStalledDueToCannotInitiateMoveForPartition.add( partitionId );
                schedule();
                return;
            }
            
            final List< Tape > sortedActionableTapes = new ArrayList<>( setOfActionableTapes );
            Collections.sort( sortedActionableTapes, m_tapeComparatorForSelectingNextTapeToMove );
            
            final Map< UUID, String > lockedTapes = new HashMap<>();
            for ( final Tape tape : sortedActionableTapes )
            {
                final Object lockHolder = m_tapeEnvironment.getTapeLockHolder( tape.getId() );
                if ( null == lockHolder )
                {
                    initiateMove( tape.getId() );
                    return;
                }

                lockedTapes.put( tape.getId(), lockHolder.toString() );
            }
            
            LOG.info( "All tapes to " + m_actionVerbInPresentTense + " are locked: " + lockedTapes );
            schedule();
        }
        
        
        private void initiateMove( final UUID tapeId )
        {
            LOG.info( "Attempting to " + m_actionVerbInPresentTense + " tape " + tapeId + "..." );
            final String error = initiateMoveInternal( tapeId );
            if ( null == error )
            {
                LOG.info( "Successfully initiated "
                          + m_actionVerbInPresentTense + " for tape " + tapeId + "." );
            }
            else
            {
                LOG.info( "Failed to " + m_actionVerbInPresentTense + " tape " 
                          + tapeId + " at this time since " + error + "." );
                schedule();
            }
        }

        
        private String initiateMoveInternal( final UUID tapeId )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).retrieve( Require.all( 
                    Require.beanPropertyEquals( Identifiable.ID, tapeId ),
                    m_tapeFilterForTapesToInitiateMoveFor ) );
            if ( null == tape )
            {
                return "tape is no longer " + m_actionVerbInPresentTense + " pending";
            }

            final BaseTapeMoveListener moveListener = createMoveListener();
            try
            {
                if ( !m_tapeEnvironment.moveTapeToSlot(
                        tapeId,
                        m_destinationElementAddressType,
                        moveListener ) )
                {
                    return "retry later is required";
                }
                moveListener.waitUntilValidated();
            }
            catch ( final RuntimeException ex )
            {
                m_partitionsStalledDueToMoveFailure.add( tape.getPartitionId() );
                schedule();
                LOG.debug( "Cannot make " + m_actionVerbInPresentTense + " progress on tapes at this time.",
                           ex );
                return ex.getClass().getSimpleName() + ": " + ex.getMessage();
            }
            
            return null;
        }
        
        
        private final Set< UUID > m_partitionsStalledDueToMoveFailure = new HashSet<>();
        private final Set< UUID > m_partitionsStalledDueToCannotInitiateMoveForPartition = new HashSet<>();
    } // end inner class def
    
    
    protected boolean canInitiateMoveForPartition(
            final UUID partitionId, @SuppressWarnings( "unused" ) final Set< Tape > tapesToMove )
    {
        final TapePartition partition =
                m_serviceManager.getRetriever( TapePartition.class ).attain( partitionId );
        if ( TapePartitionState.ONLINE != partition.getState() )
        {
            LOG.info( "Cannot initiate moves for partition " + partitionId + " since " 
                      + TapePartition.STATE + "=" + partition.getState() + "." );
            return false;
        }
        if ( Quiesced.NO != partition.getQuiesced() )
        {
            LOG.info( "Cannot initiate moves for partition " + partitionId + " since " 
                      + TapePartition.QUIESCED + "=" + partition.getQuiesced() + "." );
            return false;
        }
        
        return true;
    }
    
    
    protected abstract void cleanUpStallFailuresThatAreNoLongerApplicable(
            final Set< UUID > partitionsStalledDueToMoveFailure,
            final Set< UUID > partitionsStalledDueToCannotInitiateMoveForPartition );
    
    
    protected abstract BaseTapeMoveListener createMoveListener();
    
    
    final public void schedule()
    {
        verifyNotShutdown();
        m_executor.add( m_worker );
    }
    
    
    final protected void runWithDelay( final int millis )
    {
        SystemWorkPool.getInstance().submit( new Runnable()
        {
            public void run()
            {
                try
                {
                    Thread.sleep( millis );
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
                m_worker.run();
            }
        } );
    }
    
    
    final void runNow()
    {
        SystemWorkPool.getInstance()
                      .submit( m_worker::run );
    }
    

    protected final BeansServiceManager m_serviceManager;
    protected final TapeEnvironment m_tapeEnvironment;

    private final ElementAddressType m_destinationElementAddressType;
    private final String m_actionVerbInPresentTense;
    private final WhereClause m_tapeFilterForTapesToInitiateMoveFor;
    private final BeanComparator< Tape > m_tapeComparatorForSelectingNextTapeToMove;
    private final WorkIterationRunner m_worker = new WorkIterationRunner( this );
    private final ThrottledRunnableExecutor< WorkIterationRunner > m_executor;
    private final boolean m_alwaysReschedule;
    
    protected final static Logger LOG = Logger.getLogger( BaseTapeMovingProcessor.class );
}
