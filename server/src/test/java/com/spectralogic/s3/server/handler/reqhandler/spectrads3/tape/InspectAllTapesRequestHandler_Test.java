/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
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

public final class InspectAllTapesRequestHandler_Test 
{
    @Test
    public void testtestInspectAllTapesCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler inspectTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "inspectTape" ),
                        inspectTape,
                        null ) );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.INSPECT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        final Object expected1 = CollectionFactory.toList( (UUID)null );
        assertEquals(expected1, inspectTape.getTapeIds(), "Shoulda passed null into Inspect tape exactly once.");
        assertEquals(null, inspectTape.getPriorities().get( 0 ), "Shoulda inspectted only the expected tape id.");

        inspectTape.clear();
        inspectTape.addTapeFailure( UUID.randomUUID(), "oops" );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.INSPECT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 207 );

        final Object expected = CollectionFactory.toList( (UUID)null );
        assertEquals(expected, inspectTape.getTapeIds(), "Shoulda passed null into Inspect tape exactly once.");
        assertEquals(null, inspectTape.getPriorities().get( 0 ), "Shoulda inspectted only the expected tape id.");
    }
    
    
    @Test
    public void testtestInspectAllTapesWithPriorityCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler inspectTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "inspectTape" ),
                        inspectTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.INSPECT.toString() )
                        .addParameter( 
                                RequestParameterType.TASK_PRIORITY.toString(), 
                                BlobStoreTaskPriority.HIGH.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        final Object expected = CollectionFactory.toList( (UUID)null );
        assertEquals(expected, inspectTape.getTapeIds(), "Shoulda passed null into Inspect tape exactly once.");
        assertEquals(BlobStoreTaskPriority.HIGH, inspectTape.getPriorities().get( 0 ), "Shoulda inspectted only the expected tape id.");
    }
}
