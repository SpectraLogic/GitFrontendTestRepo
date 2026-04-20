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

public final class CreateDriveDumpRequestHandler_Test 
{
    @Test
    public void testtestCreateDriveDumpCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "td1" );
        final UUID driveId = drive.getId();

        final MediaOperationInvocationHandler driveDump = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "driveDump" ),
                        driveDump,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.TAPE_DRIVE + "/" + driveId )
                        .addParameter( "operation", RestOperationType.DUMP.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertEquals(
                CollectionFactory.toList( driveId ),
                driveDump.getTapeIds(),
                "Shoulda tested only the expected drive id."
                );
    }
}
