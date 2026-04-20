/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.ReservedTaskType;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;

public final class TestTapeDriveRequestHandler_Test 
{
    @Test
    public void testtestTestDriveCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "td1" );
        mockDaoDriver.updateBean(drive.setReservedTaskType(ReservedTaskType.MAINTENANCE), TapeDrive.RESERVED_TASK_TYPE);
        final UUID driveId = drive.getId();
        final Tape tape = mockDaoDriver.createTape(drive.getPartitionId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(tape.setRole(TapeRole.TEST), Tape.ROLE);

        final MediaOperationInvocationHandler testDrive = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "testDrive" ),
                        testDrive,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.TAPE_DRIVE + "/" + driveId )
                        .addParameter( "operation", RestOperationType.TEST.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( driveId );
        assertEquals(expected, testDrive.getTapeIds(), "Shoulda tested only the expected drive id.");
    }

    @Test
    public void testtestCanSpecifyTape()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "td1" );
        mockDaoDriver.updateBean(drive.setReservedTaskType(ReservedTaskType.MAINTENANCE), TapeDrive.RESERVED_TASK_TYPE);
        final UUID driveId = drive.getId();
        final Tape tape = mockDaoDriver.createTape(drive.getPartitionId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(tape.setRole(TapeRole.TEST), Tape.ROLE);

        final MediaOperationInvocationHandler testDrive = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "testDrive" ),
                        testDrive,
                        null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE_DRIVE + "/" + driveId )
                .addParameter( "operation", RestOperationType.TEST.toString() )
                .addParameter( "tapeId", tape.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( driveId );
        assertEquals(expected, testDrive.getTapeIds(), "Shoulda tested only the expected drive id.");
    }

    @Test
    public void testtestCanOnlySpecifyValidTape()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "td1" );
        mockDaoDriver.updateBean(drive.setReservedTaskType(ReservedTaskType.MAINTENANCE), TapeDrive.RESERVED_TASK_TYPE);
        final TapePartition partition2= mockDaoDriver.createTapePartition(null, "tp2");
        final UUID driveId = drive.getId();
        final Tape tape = mockDaoDriver.createTape(partition2.getId(), TapeState.FOREIGN);

        final MediaOperationInvocationHandler testDrive = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "testDrive" ),
                        testDrive,
                        null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE_DRIVE + "/" + driveId )
                .addParameter( "operation", RestOperationType.TEST.toString() )
                .addParameter( "tapeId", tape.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        mockDaoDriver.updateBean(tape.setRole(TapeRole.TEST), Tape.ROLE);
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        mockDaoDriver.updateBean(tape.setState(TapeState.NORMAL), Tape.STATE);
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        mockDaoDriver.updateBean(tape.setPartitionId(drive.getPartitionId()), Tape.PARTITION_ID);
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }

    @Test
    public void testtestMustHaveValidTape()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "td1" );
        mockDaoDriver.updateBean(drive.setReservedTaskType(ReservedTaskType.MAINTENANCE), TapeDrive.RESERVED_TASK_TYPE);
        final TapePartition partition2= mockDaoDriver.createTapePartition(null, "tp2");
        final UUID driveId = drive.getId();
        final Tape tape = mockDaoDriver.createTape(partition2.getId(), TapeState.FOREIGN);

        final MediaOperationInvocationHandler testDrive = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "testDrive" ),
                        testDrive,
                        null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE_DRIVE + "/" + driveId )
                .addParameter( "operation", RestOperationType.TEST.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        mockDaoDriver.updateBean(tape.setRole(TapeRole.TEST), Tape.ROLE);
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        mockDaoDriver.updateBean(tape.setState(TapeState.NORMAL), Tape.STATE);
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        mockDaoDriver.updateBean(tape.setPartitionId(drive.getPartitionId()), Tape.PARTITION_ID);
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
