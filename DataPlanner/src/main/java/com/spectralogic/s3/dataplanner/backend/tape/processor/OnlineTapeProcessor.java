/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.listener.BaseTapeMoveListener;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;

import static com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType.TAPE_MEDIA_TYPE_INCOMPATIBLE;

public final class OnlineTapeProcessor extends BaseTapeMovingProcessor
{
    public OnlineTapeProcessor( 
            final BeansServiceManager serviceManager, 
            final TapeBlobStoreProcessor processor,
            final TapeEnvironment tapeEnvironment,
            final int delayInMillisForOnline )
    {
        super( serviceManager,
               tapeEnvironment,
               delayInMillisForOnline,
               Require.beanPropertyEquals( Tape.STATE, TapeState.ONLINE_PENDING ),
               new BeanComparator<>( Tape.class, Tape.BAR_CODE ),
               "online",
               ElementAddressType.STORAGE,
               false );
    }


    @Override
    protected void cleanUpStallFailuresThatAreNoLongerApplicable(
            final Set< UUID > partitionsStalledDueToMoveFailure,
            final Set< UUID > partitionsStalledDueToCannotInitiateMoveForPartition )
    {
        final Set< TapePartitionFailure > failures = 
                m_serviceManager.getRetriever( TapePartitionFailure.class ).retrieveAll(
                        Failure.TYPE, 
                        TapePartitionFailureType.ONLINE_STALLED_DUE_TO_NO_STORAGE_SLOTS ).toSet();
        final Set< UUID > partitionsWithFailures =
                new HashSet<>( BeanUtils.< UUID >extractPropertyValues( 
                        failures, TapePartitionFailure.PARTITION_ID ) );
        partitionsWithFailures.removeAll( partitionsStalledDueToMoveFailure );
        for ( final UUID id : partitionsWithFailures )
        {
            m_serviceManager.getService( TapePartitionFailureService.class ).deleteAll(
                    id, TapePartitionFailureType.ONLINE_STALLED_DUE_TO_NO_STORAGE_SLOTS );
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
                    tape, TapeState.ONLINE_IN_PROGRESS );
            m_serviceManager.getService( TapePartitionFailureService.class ).deleteAll( 
                    tape.getPartitionId(), 
                    TapePartitionFailureType.ONLINE_STALLED_DUE_TO_NO_STORAGE_SLOTS );
        }
        
        
        @Override
        protected void validationFailed( final UUID tapeId, final RuntimeException failure )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            m_serviceManager.getService( TapePartitionFailureService.class ).create( 
                    tape.getPartitionId(),
                    TapePartitionFailureType.ONLINE_STALLED_DUE_TO_NO_STORAGE_SLOTS, 
                    failure,
                    Integer.valueOf( 60 ) );
        }
        

        public void moveSucceeded( final UUID tapeId )
        {
            final TapeRM tape = new TapeRM(tapeId, m_serviceManager);
            final TapeState newState;
            final TapeDriveType tapeDriveType = tape.getPartition().getDriveType();
            if (!tapeDriveType.getSupportedTapeTypes().contains(tape.getType())) {
                final TapePartitionFailure failure = BeanFactory.newBean(TapePartitionFailure.class)
                                .setType(TAPE_MEDIA_TYPE_INCOMPATIBLE)
                                .setPartitionId(tape.unwrap().getPartitionId())
                        .setErrorMessage("A tape drive of type " + tapeDriveType
                                + " cannot read tapes of type "
                                + tape.getType()
                                + ".  It is illegal to have a tape in a partition where the tape drives "
                                + "are unable to read the tape.  Please remove tape "
                                + tape.getBarCode() + "." );

                m_serviceManager.getService( TapePartitionFailureService.class ).create(failure, null);
                newState = TapeState.INCOMPATIBLE;
            } else if(tape.getType().canContainData()) {
                newState = TapeState.PENDING_INSPECTION;
            } else {
                newState = TapeState.NORMAL;
            }
            LOG.info( "Tape " + tape.getId() + " has been onlined." );
            m_serviceManager.getService( TapeService.class ).transistState(tape.unwrap(), newState);
            runWithDelay( 50 );
        }

        
        public void moveFailed( final UUID tapeId )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            LOG.info( "Tape " + tape.getId() + " could not be moved to a storage slot for onlining." );
            m_serviceManager.getService( TapeService.class ).transistState( tape, TapeState.ONLINE_PENDING );
            schedule();
        }
    } // end inner class def
}
