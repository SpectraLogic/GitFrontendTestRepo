/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.listener.BaseTapeMoveListener;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;


/**
 * A tape moving processor to handle tapes in drives requesting tape removal
 */
public final class ForceRemovalTapeProcessor
    extends BaseTapeMovingProcessor
{
    public ForceRemovalTapeProcessor(
        final BeansServiceManager serviceManager,
        final TapeEnvironment tapeEnvironment,
        final int delayInMillisForRemoval)
    {
        super( serviceManager,
               tapeEnvironment,
               delayInMillisForRemoval,
               Require.exists( TapeDrive.class,
                               TapeDrive.TAPE_ID,
                               Require.beanPropertyEquals( TapeDrive.FORCE_TAPE_REMOVAL,
                                                           Boolean.TRUE ) ),
               new BeanComparator<>( Tape.class, Tape.ID ),/* ordering comparator */ 
               "remove", /* verb */
               ElementAddressType.STORAGE, /* destination slot type */
               true );
    }

    
    /*
     * NB: Do not override:
     * protected boolean canInitiateMoveForPartition( final UUID partitionId,
     * final Set< Tape > tapesToMove ) 
     *
     * No other criteria than the partition not being quiesced should prevent us
     * from acting on the tape
     */

    @Override
    protected void cleanUpStallFailuresThatAreNoLongerApplicable(
            final Set< UUID > partitionsStalledDueToMoveFailure,
            final Set< UUID > partitionsStalledDueToCannotInitiateMoveForPartition )
    {
        /* We do not currently generate failures, but this is an abstract method. */
    }

    
    @Override
    protected BaseTapeMoveListener createMoveListener()
    {
        return new RemoveTapeFromDriveListener();
    }

    private final class RemoveTapeFromDriveListener extends BaseTapeMoveListener
    {
        public void moveSucceeded( final UUID tapeId )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            LOG.info( "Tape " + tape.getId() + " successfully removed from drive." );
            runWithDelay( 50 ); /* ? */
        }
        
        public void moveFailed( final UUID tapeId )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            LOG.info( "Tape " + tape.getId() + " could not be removed from drive." );
            schedule();
        }
    } /* class RemoveTapeFromDriveListener */
}
// Local Variables:
// indent-tabs-mode: nil
// End:
