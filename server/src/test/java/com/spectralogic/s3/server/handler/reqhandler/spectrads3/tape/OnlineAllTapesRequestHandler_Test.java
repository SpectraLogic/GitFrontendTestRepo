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

import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class OnlineAllTapesRequestHandler_Test 
{
    @Test
    public void testtestOnlineAllTapesCallsDataPlanner()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler ih = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        TapeManagementResource.class.getMethod( "onlineTape", UUID.class ),
                        ih,
                        null ) );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.ONLINE.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        final Object expected1 = CollectionFactory.toList( (UUID)null );
        assertEquals(expected1, ih.getTapeIds(), "Shoulda called the online with null exactly once.");

        ih.clear();
        ih.addTapeFailure( UUID.randomUUID(), "oops" );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.ONLINE.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 207 );

        final Object expected = CollectionFactory.toList( (UUID)null );
        assertEquals(expected, ih.getTapeIds(), "Shoulda called the online with null exactly once.");
    }
}
