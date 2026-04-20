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

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class FormatAllTapesRequestHandler_Test 
{
    @Test
    public void testFormatAllTapesCallsDataPlanner()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        
        final MediaOperationInvocationHandler formatTape = new MediaOperationInvocationHandler();
        formatTape.addTapeFailure( t1.getId(), "oops1" );
        formatTape.addTapeFailure( t2.getId(), "oops2" );
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        TapeManagementResource.class.getMethod( "formatTape", UUID.class, boolean.class, boolean.class ),
                        formatTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.FORMAT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 207 );
        driver.assertResponseToClientContains( t1.getId().toString() );
        driver.assertResponseToClientContains( t2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( t3.getId().toString() );
        
        driver.assertResponseToClientContains( "oops1" );
        driver.assertResponseToClientContains( "oops2" );

        final Object expected1 = CollectionFactory.toList( (UUID)null );
        assertEquals(expected1, formatTape.getTapeIds(), "Shoulda passed null into format tape exactly once.");
        final Object expected = CollectionFactory.toList( Boolean.FALSE );
        assertEquals(expected, formatTape.getForceFlags(), "Should notta provided the force flag.");
    }
    
    
    @Test
    public void testFormatAllTapesCallsDataPlannerWhenForceFlagProvided()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler formatTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        TapeManagementResource.class.getMethod( "formatTape", UUID.class, boolean.class, boolean.class),
                        formatTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.FORMAT.toString() )
                        .addParameter( "force", "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        final Object expected1 = CollectionFactory.toList( (UUID)null );
        assertEquals(expected1, formatTape.getTapeIds(), "Shoulda passed null into format tape exactly once.");
        final Object expected = CollectionFactory.toList( Boolean.TRUE );
        assertEquals(expected, formatTape.getForceFlags(), "Shoulda provided the force flag.");
    }
}
