/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.service.api.BeansServiceManager;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public final class CleanTapeDriveTask_Test 
{
    @Test
    public void testTaskOnlyWillingToUseDriveWeWantToClean()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "a" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "b" );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final CleanTapeDriveTask task = createTask( drive1, tape.getId(), dbSupport.getServiceManager());
        assertFalse(task.canUseDrive(drive2.getId()), "Shoulda been willing to use correct drive only.");
        assertTrue(task.canUseDrive(drive1.getId()), "Shoulda been willing to use correct drive.");
        task.prepareForExecutionIfPossible(
                tapeDriveResource, 
                new MockTapeAvailability().setDriveId( drive1.getId() ) );
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda been willing to use correct drive.");
    }
    
    
   @Test
    public void testCleanSucceedsResultsInTapeDriveUpdated()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.FORMAT_PENDING );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ), Tape.TYPE );
        assertNull(dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain( Require.nothing() )
                    .getLastCleaned(), "Should notta updated drive last cleaned date yet.");

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailGetLoadedTapeInformation( true );
        tapeDriveResource.setHasChangedSinceCheckpointException( new RuntimeException( "Uh uh uh..." ) );
        tapeDriveResource.setVerifyQuiescedToCheckpointException( new RuntimeException( "Oops." ) );
        final CleanTapeDriveTask task = createTask( drive, tape.getId(), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( drive.getId() ) );
            
        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain( Require.nothing() )
                    .getLastCleaned(), "Shoulda updated drive last cleaned date.");
        assertEquals(TapeFailureType.DRIVE_CLEANED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda recorded a failure.");
    }
    

   @Test
    public void testCleanFailsWithNon409ResultsInCleanFailureAndCleaningTapeNotMarkedBad()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ), Tape.TYPE );
        assertNull(dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain( Require.nothing() )
                    .getLastCleaned(), "Should notta updated drive last cleaned date yet.");

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailGetLoadedTapeInformation( true );
        tapeDriveResource.setHasChangedSinceCheckpointException( new RuntimeException( "Uh uh uh..." ) );
        tapeDriveResource.setVerifyQuiescedToCheckpointException( new RuntimeException( "Oops." ) );
        tapeDriveResource.setWaitForDriveCleaningToCompleteException( new RpcProxyException( 
                "blah", BeanFactory.newBean( Failure.class ) ) );
        final CleanTapeDriveTask task = createTask( drive, tape.getId(), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( drive.getId() ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertNull(dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain( Require.nothing() )
                    .getLastCleaned(), "Should notta updated drive last cleaned date due to failure.");
        assertEquals(TapeFailureType.DRIVE_CLEAN_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                    .getType(), "Shoulda recorded a failure.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        Require.nothing() ).getState(), "Should notta updated cleaning tape state.");
    }
    

   @Test
    public void testCleanFailsWith409ResultsInCleanFailureAndCleaningTapeMarkedBad()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ), Tape.TYPE );
        assertNull(dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain( Require.nothing() )
                    .getLastCleaned(), "Should notta updated drive last cleaned date yet.");

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailGetLoadedTapeInformation( true );
        tapeDriveResource.setHasChangedSinceCheckpointException( new RuntimeException( "Uh uh uh..." ) );
        tapeDriveResource.setVerifyQuiescedToCheckpointException( new RuntimeException( "Oops." ) );
        tapeDriveResource.setWaitForDriveCleaningToCompleteException( new RpcProxyException( 
                "blah",
                BeanFactory.newBean( Failure.class ).setHttpResponseCode( 409 ) ) );
        final CleanTapeDriveTask task = createTask( drive, tape.getId(), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible(
                tapeDriveResource, 
                new MockTapeAvailability().setDriveId( drive.getId() ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertNull(dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain( Require.nothing() )
                    .getLastCleaned(), "Should notta updated drive last cleaned date due to failure.");
        assertEquals(TapeFailureType.CLEANING_TAPE_EXPIRED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                    .getType(), "Shoulda recorded a failure.");
        assertEquals(TapeState.BAD, dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        Require.nothing() ).getState(), "Should updated cleaning tape state.");
    }


    private CleanTapeDriveTask createTask(TapeDrive drive, UUID tapeId, BeansServiceManager serviceManager) {
        return new CleanTapeDriveTask( drive, tapeId, new TapeFailureManagement(serviceManager), serviceManager);
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
