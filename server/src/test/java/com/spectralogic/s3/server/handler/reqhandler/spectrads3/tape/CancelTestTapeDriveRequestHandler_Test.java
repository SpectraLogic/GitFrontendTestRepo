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

public final class CancelTestTapeDriveRequestHandler_Test 
{
    @Test
    public void testtestCancelTestDriveCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "td1" );
        /*mockDaoDriver.updateBean(drive.setReservedTaskType(ReservedTaskType.MAINTENANCE), TapeDrive.RESERVED_TASK_TYPE);
        final UUID driveId = drive.getId();
        final Tape tape = mockDaoDriver.createTape(drive.getPartitionId(), TapeState.NORMAL);
        mockDaoDriver.updateBean(tape.setRole(TapeRole.TEST), Tape.ROLE);*/

        final MediaOperationInvocationHandler testDrive = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "cancelTestDrive" ),
                        testDrive,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.TAPE_DRIVE + "/" + drive.getId() )
                        .addParameter( "operation", RestOperationType.CANCEL_TEST.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertEquals(
                CollectionFactory.toList( drive.getId() ),
                testDrive.getTapeIds(),
                "Shoulda tested only the expected drive id."
                 );
    }
}
