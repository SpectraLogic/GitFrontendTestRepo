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
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class FormatTapeRequestHandler_Test 
{
    @Test
    public void testFormatTapeCallsDataPlanner()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();

        final MediaOperationInvocationHandler formatTape = new MediaOperationInvocationHandler();
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
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( "operation", RestOperationType.FORMAT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected1 = CollectionFactory.toList( tapeId );
        assertEquals(expected1, formatTape.getTapeIds(), "Shoulda formatted only the expected tape id.");
        final Object expected = CollectionFactory.toList( Boolean.FALSE );
        assertEquals(expected, formatTape.getForceFlags(), "Should notta provided the force flag.");
    }
    
    
    @Test
    public void testFormatTapeCallsDataPlannerWhenForceFlagProvided()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();

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
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( "operation", RestOperationType.FORMAT.toString() )
                        .addParameter( "force", "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected1 = CollectionFactory.toList( tapeId );
        assertEquals(expected1, formatTape.getTapeIds(), "Shoulda formatted only the expected tape id.");
        final Object expected = CollectionFactory.toList( Boolean.TRUE );
        assertEquals(expected, formatTape.getForceFlags(), "Shoulda provided the force flag.");
    }
}
