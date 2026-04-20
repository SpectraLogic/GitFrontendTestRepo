/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.listener.BaseTapeMoveListener;
import com.spectralogic.s3.dataplanner.backend.tape.task.VerifyTapeTask;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class EjectTapeProcessor
    extends BaseTapeMovingProcessor
    implements BlobStoreTaskSchedulingListener
{
    public EjectTapeProcessor(
            final BeansServiceManager serviceManager,
            final TapeBlobStoreProcessor processor,
            final TapeEnvironment tapeEnvironment,
            final int delayInMillisForEject )
    {
        super( serviceManager,
               tapeEnvironment,
               delayInMillisForEject,
               Require.all(
                       Require.not( Require.beanPropertyEquals( Tape.EJECT_PENDING, null ) ),
                       Require.beanPropertyEquals( Tape.VERIFY_PENDING, null ),
                       Require.not( Require.exists( TapeDrive.class, TapeDrive.TAPE_ID, Require.nothing() ) ) ),
               new BeanComparator<>( Tape.class, Tape.EJECT_PENDING ),
               "eject",
               ElementAddressType.IMPORT_EXPORT,
               false );
        processor.addTaskSchedulingListener( this );
    }
    
    
    public void taskSchedulingRequired( final BlobStoreTask task )
    {
        //if the source of this "task scheduling required" event is a verify task, schedule ejects, because we
        //it might be due to a "verify prior to eject" configuration
        if ( null != task && VerifyTapeTask.class.isAssignableFrom( task.getClass() ) )
        {
            schedule();
        }
    }

    
    @Override
    protected boolean canInitiateMoveForPartition( final UUID partitionId, final Set< Tape > tapesToMove )
    {
        if ( !super.canInitiateMoveForPartition( partitionId, tapesToMove ) )
        {
            return false;
        }
        
        final Set< Tape > tapesNotOnline = m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals( Tape.PARTITION_ID, partitionId ),
                        Require.beanPropertyEqualsOneOf(
                                Tape.STATE,
                                TapeState.OFFLINE, TapeState.ONLINE_IN_PROGRESS ) ) ).toSet();
        if ( !tapesNotOnline.isEmpty() )
        {
            final String delimiter = ", ";
            final int maxElements = 10;
            String joinedBarcodes = tapesNotOnline.stream( )
                    .limit( maxElements )
                    .map( Tape::getBarCode )
                    .collect( Collectors.joining( delimiter ) );
            if ( tapesNotOnline.size( ) > maxElements )
            {
                joinedBarcodes += delimiter + " ...";
            }
            m_serviceManager.getService( TapePartitionFailureService.class ).create(
                    partitionId,
                    TapePartitionFailureType.EJECT_STALLED_DUE_TO_OFFLINE_TAPES,
                    "Cannot export " + tapesToMove.size() + " tapes waiting to be exported since "
                    + tapesNotOnline.size() + " tapes are waiting to be brought online.  "
                    + "These tapes must be brought online (which will move them into storage slots) "
                    + "before pending exports can be processed. Tapes " + joinedBarcodes,
                    Integer.valueOf( 60 ) );
            return false;
        }
        return true;
    }


    @Override
    protected void cleanUpStallFailuresThatAreNoLongerApplicable(
            final Set< UUID > partitionsStalledDueToMoveFailure,
            final Set< UUID > partitionsStalledDueToCannotInitiateMoveForPartition )
    {
        final Set< TapePartitionFailure > failures =
                m_serviceManager.getRetriever( TapePartitionFailure.class ).retrieveAll(
                        Failure.TYPE,
                        TapePartitionFailureType.EJECT_STALLED_DUE_TO_OFFLINE_TAPES ).toSet();
        final Set< UUID > partitionsWithFailures =
                new HashSet<>( BeanUtils.< UUID >extractPropertyValues(
                        failures, TapePartitionFailure.PARTITION_ID ) );
        partitionsWithFailures.removeAll( partitionsStalledDueToCannotInitiateMoveForPartition );
        for ( final UUID id : partitionsWithFailures )
        {
            m_serviceManager.getService( TapePartitionFailureService.class ).deleteAll(
                    id, TapePartitionFailureType.EJECT_STALLED_DUE_TO_OFFLINE_TAPES );
        }
    }


    @Override
    protected BaseTapeMoveListener createMoveListener()
    {
        return new MoveTapeToImportExportSlotListener();
    }
    
    
    private final class MoveTapeToImportExportSlotListener extends BaseTapeMoveListener
    {
        @Override
        public void validationSucceeded( final UUID tapeId )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            m_serviceManager.getService( TapeService.class ).transistState(
                    tape, TapeState.EJECT_TO_EE_IN_PROGRESS );
            m_serviceManager.getService( TapeService.class ).update(
                    tape.setEjectPending( null ),
                    Tape.EJECT_PENDING );
            m_serviceManager.getService( TapePartitionFailureService.class ).deleteAll(
                    tape.getPartitionId(),
                    TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED );
        }
        
        
        public void moveSucceeded( final UUID tapeId )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            LOG.info( "Tape " + tape.getId() + " is ready to be physically ejected." );
            m_serviceManager.getService( TapeService.class ).transistState(
                    tape, TapeState.EJECT_FROM_EE_PENDING );
            generatePhysicalEjectionRequiredTapePartitionFailureIfNecessary( tape );
            runWithDelay( 50 );
        }
        
        
        private void generatePhysicalEjectionRequiredTapePartitionFailureIfNecessary( final Tape tape )
        {
            final String reasonEjectRequired = getReasonPhysicalEjectionRequired(
                    m_tapeEnvironment.isSlotAvailable(
                            tape.getPartitionId(),
                            ElementAddressType.IMPORT_EXPORT ) );
            if ( null == reasonEjectRequired )
            {
                m_serviceManager.getService( TapePartitionFailureService.class ).deleteAll(
                        tape.getPartitionId(),
                        TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED );
            }
            else
            {
                m_serviceManager.getService( TapePartitionFailureService.class ).create(
                        tape.getPartitionId(),
                        TapePartitionFailureType.TAPE_EJECTION_BY_OPERATOR_REQUIRED,
                        reasonEjectRequired + " Tape " + tape.getBarCode( ) + ".",
                        Integer.valueOf( 60 ) );
            }
        }
        
        
        private String getReasonPhysicalEjectionRequired( final boolean moreSlotsAvailableOfDestinationType )
        {
            if ( moreSlotsAvailableOfDestinationType )
            {
                if ( m_serviceManager.getRetriever( Tape.class ).any(
                        Require.not( Require.beanPropertyEquals( Tape.EJECT_PENDING, null ) ) ) )
                {
                    return null;
                }
                return "All tapes to export have been moved to import/export slots.  "
                        + "Please remove the tapes in the import/export slots from the library.";
            }
            return "All import/export slots have been filled with tapes to export.  "
                   + "Please remove the tapes in the import/export slots from the library.";
        }

        
        public void moveFailed( final UUID tapeId )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            LOG.info( "Tape " + tape.getId() + " could not be moved to an import/export slot for ejection." );
            m_serviceManager.getService( TapeService.class ).rollbackLastStateTransition( tape );
            m_serviceManager.getService( TapeService.class ).update(
                    tape.setEjectPending( new Date( 1 ) ), Tape.EJECT_PENDING );
            schedule();
        }
    } // end inner class def
}
