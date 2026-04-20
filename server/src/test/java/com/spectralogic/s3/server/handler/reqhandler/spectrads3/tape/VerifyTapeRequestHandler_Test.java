/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class VerifyTapeRequestHandler_Test 
{
    @Test
    public void testtestVerifyTapeCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();

        final MediaOperationInvocationHandler verifyTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "verifyTape" ),
                        verifyTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( "operation", RestOperationType.VERIFY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( tapeId );
        assertEquals(expected, verifyTape.getTapeIds(), "Shoulda verifyted only the expected tape id.");
        assertEquals(null, verifyTape.getPriorities().get( 0 ), "Shoulda verifyted only the expected tape id.");
    }
    
    
    @Test
    public void testtestVerifyTapeWithTaskPriorityCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();

        final MediaOperationInvocationHandler verifyTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "verifyTape" ),
                        verifyTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( "operation", RestOperationType.VERIFY.toString() )
                        .addParameter( 
                                RequestParameterType.TASK_PRIORITY.toString(), 
                                BlobStoreTaskPriority.HIGH.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( tapeId );
        assertEquals(expected, verifyTape.getTapeIds(), "Shoulda verifyted only the expected tape id.");
        assertEquals(BlobStoreTaskPriority.HIGH, verifyTape.getPriorities().get( 0 ), "Shoulda verifyted only the expected tape id.");
    }
}
