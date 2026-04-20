/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.lang.reflect.Method;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;

public final class EjectTapeRequestHandler_Test 
{
    @Test
    public void testtestEjectTapeCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();

        final Method method = ReflectUtil.getMethod( TapeManagementResource.class, "ejectTape" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        support.setTapeInterfaceIh( btih );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( "operation", RestOperationType.EJECT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(1,  btih.getMethodCallCount(method), "Shoulda sent only the expected tape id.");
        assertEquals(tapeId, btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 0 ), "Shoulda sent only the expected tape id.");
        assertEquals(null, btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 1 ), "Shoulda sent down null for non-specified eject tape attributes.");
        assertEquals(null, btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 2 ), "Shoulda sent down null for non-specified eject tape attributes.");
    }
    
    
    @Test
    public void testtestEjectTapeAllOptionalAttributesSpecifiedCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();

        final Method method = ReflectUtil.getMethod( TapeManagementResource.class, "ejectTape" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        support.setTapeInterfaceIh( btih );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( "operation", RestOperationType.EJECT.toString() )
                        .addParameter( Tape.EJECT_LABEL, "label" )
                        .addParameter( Tape.EJECT_LOCATION, "location" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(1,  btih.getMethodCallCount(method), "Shoulda sent only the expected tape id.");
        assertEquals(tapeId, btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 0 ), "Shoulda sent only the expected tape id.");
        assertEquals("label", btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 1 ), "Shoulda sent down specified eject tape attributes.");
        assertEquals("location", btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 2 ), "Shoulda sent down specified eject tape attributes.");
    }
}
