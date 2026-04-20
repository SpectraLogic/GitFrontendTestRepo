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

import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class CancelFormatOnAllTapesRequestHandler_Test 
{
    @Test
    public void testtestCancelFormatOnAllTapesCallsDataPlanner()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler cancelFormatTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        TapeManagementResource.class.getMethod( "cancelFormatTape", UUID.class ),
                        cancelFormatTape,
                        null ) );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.CANCEL_FORMAT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                CollectionFactory.toList( (UUID)null ),
                cancelFormatTape.getTapeIds(),
                "Shoulda called the cancel format tape with null exactly once."
                 );

        cancelFormatTape.clear();
        cancelFormatTape.addTapeFailure( UUID.randomUUID(), "oops" );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.CANCEL_FORMAT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 207 );
        
        assertEquals(
                CollectionFactory.toList( (UUID)null ),
                cancelFormatTape.getTapeIds(),
                "Shoulda called the cancel format tape with null exactly once."
                 );
    }
}
