/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;


import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public final class TestTapeDriveTask_Test 
{
    @Test
    public void testTaskOnlyWillingToUseDriveWeWantToTest()
    {
        
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape(TapeState.NORMAL);
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "a" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "b" );

        mockDaoDriver.updateBean(tape.setRole(TapeRole.TEST), Tape.ROLE);
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final TestTapeDriveTask task = createTask( drive1, tape.getId(), dbSupport.getServiceManager());
        assertFalse(task.canUseDrive(drive2.getId()), "Shoulda been willing to use correct drive only.");
        assertTrue(
                task.canUseDrive(drive1.getId()),
                "Shoulda been willing to use correct drive."
                );
        task.prepareForExecutionIfPossible(
                tapeDriveResource, 
                new MockTapeAvailability().setDriveId( drive1.getId() ) );
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda been willing to use correct drive.");
    }

    @Test
    public void testTaskFailsIfTapeBecameIneligible()
    {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "a" );

        mockDaoDriver.updateBean(tape.setRole(TapeRole.NORMAL), Tape.ROLE);
        mockDaoDriver.updateBean(drive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        attemptPrepareForExecution(dbSupport, tape, drive, true);

        mockDaoDriver.updateBean(tape.setRole(TapeRole.TEST), Tape.ROLE);
        mockDaoDriver.updateBean(tape.setState(TapeState.FOREIGN), Tape.STATE);
        attemptPrepareForExecution(dbSupport, tape, drive, true);

        mockDaoDriver.updateBean(tape.setState(TapeState.NORMAL), Tape.STATE);
        attemptPrepareForExecution(dbSupport, tape, drive, false);
    }

    private void attemptPrepareForExecution(final DatabaseSupport dbSupport, final Tape tape, final TapeDrive drive, final boolean expectFailure) {
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final TestTapeDriveTask task = createTask(drive, tape.getId(), dbSupport.getServiceManager());

        if (expectFailure) {
            TestUtil.assertThrows(null, RuntimeException.class, () -> {
                task.prepareForExecutionIfPossible(
                        tapeDriveResource,
                        new MockTapeAvailability().setDriveId( drive.getId() ) );
            });
            assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda invalidated task and marked complete.");
        } else {
            task.prepareForExecutionIfPossible(
                    tapeDriveResource,
                    new MockTapeAvailability().setDriveId( drive.getId() ) );
            assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda marked task ready to execute.");
        }

    }


    @Test
    public void testErrorResultsInTapeFailureCreation()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ).setRole(TapeRole.TEST), Tape.TYPE, Tape.ROLE );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailTest(true);
        TestTapeDriveTask task = createTask( drive, tape.getId(), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( drive.getId() ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeFailureType.DRIVE_TEST_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                    .getType(), "Shoulda recorded a failure.");

        dbSupport.getServiceManager().getDeleter(TapeFailure.class).delete(Require.nothing());

        tapeDriveResource.setFailTest(false);
        task = createTask( drive, tape.getId(), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( drive.getId() ) );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(TapeFailureType.DRIVE_TEST_SUCCEEDED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                        .getType(), "Shoulda recorded a failure.");
    }


    private TestTapeDriveTask createTask(TapeDrive drive, UUID tapeId, BeansServiceManager serviceManager) {
        return new TestTapeDriveTask( drive, tapeId, new TapeFailureManagement(serviceManager), serviceManager);
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
