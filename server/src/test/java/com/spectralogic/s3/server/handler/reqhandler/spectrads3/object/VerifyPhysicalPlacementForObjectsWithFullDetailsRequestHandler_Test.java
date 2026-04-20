/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.nio.charset.Charset;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.common.testfrmwrk.MockObjectsOnTarget;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class VerifyPhysicalPlacementForObjectsWithFullDetailsRequestHandler_Test 
{
    @Test
    public void testVerifyDetailedPhysicalPlacementForObjectOnTapeReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final MockObjectsOnTarget moot = new MockObjectsOnTarget( mockDaoDriver, support.getDatabaseSupport() );
        moot.putObjectOnTape( "t1", "b1", "o1" );
        moot.putObjectOnTape( "t1", "b1", "o2" );
        moot.putObjectOnTape( "t2", "b1", "o3" );
        moot.putObjectOnTape( "t3", "b2", "o5" );
    
        final byte[] requestPayload = ( "<Objects>" + "<Object Name='o1'/>"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/bucket/b1/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.VERIFY_PHYSICAL_PLACEMENT.toString() )
                    .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "t1" );
        driver.assertResponseToClientDoesNotContain( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientContains( "o1" );
    }
    
    
    @Test
    public void testVerifyDetailedPhysicalPlacementForObjectThatDoesNotExistThrows404()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
    
        final MockObjectsOnTarget moot = new MockObjectsOnTarget( mockDaoDriver, support.getDatabaseSupport() );
        moot.putObjectOnTape( "t1", "b1", "o2" );
        moot.putObjectOnTape( "t2", "b1", "o3" );
        moot.putObjectOnTape( "t3", "b2", "o5" );
    
        final byte[] requestPayload =
                ( "<Objects><Object Name='o1'/></Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/bucket/b1/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.VERIFY_PHYSICAL_PLACEMENT.toString() )
                    .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testVerifyDetailedPhysicalPlacementForObjectNotOnTapeNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final MockObjectsOnTarget moot = new MockObjectsOnTarget( mockDaoDriver, support.getDatabaseSupport() );
        moot.putObjectOnTape( "t1", "b1", "o1" );
        moot.putObjectOnTape( "t1", "b1", "o2" );
        moot.putObjectOnTape( "t2", "b1", "o3" );
        moot.putObjectOnTape( "t3", "b2", "o5" );
    
        final byte[] requestPayload =
                ( "<Objects><Object Name='o4'/></Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/bucket/b1/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.VERIFY_PHYSICAL_PLACEMENT.toString() )
                    .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
        driver.assertResponseToClientDoesNotContain( "t1" );
        driver.assertResponseToClientDoesNotContain( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientContains( "o4" );
    }
    
    
    @Test
    public void testVerifyDetailedPhysicalPlacementForObjectsAllOnTapeReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
    
        final MockObjectsOnTarget moot = new MockObjectsOnTarget( mockDaoDriver, support.getDatabaseSupport() );
        moot.putObjectOnTape( "t1", "b1", "o1" );
        moot.putObjectOnTape( "t1", "b1", "o2" );
        moot.putObjectOnTape( "t2", "b1", "o3" );
        moot.putObjectOnTape( "t3", "b2", "o5" );
    
        final byte[] requestPayload = ( "<Objects><Object Name='o1'/>" + "<Object Name='o3'/></Objects>" ).getBytes(
                Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( support, true,
                new MockInternalRequestAuthorizationStrategy(), RequestType.GET, "_rest_/bucket/b1/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.VERIFY_PHYSICAL_PLACEMENT.toString() )
                    .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "t1" );
        driver.assertResponseToClientContains( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientContains( "o1" );
    }
    
    
    @Test
    public void testVerifyDetailedPhysicalPlacementForObjectsWhereSomeDontExistThrows404()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
    
        final MockObjectsOnTarget moot = new MockObjectsOnTarget( mockDaoDriver, support.getDatabaseSupport() );
        moot.putObjectOnTape( "t1", "b1", "o2" );
        moot.putObjectOnTape( "t2", "b1", "o3" );
        moot.putObjectOnTape( "t3", "b2", "o5" );
    
        final byte[] requestPayload = ( "<Objects><Object Name='o1'/><Object Name='o3'/>"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, "_rest_/bucket/b1/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.VERIFY_PHYSICAL_PLACEMENT.toString() )
                    .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testVerifyDetailedPhysicalPlacementForObjectsNotAllOnTapeNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
    
        final MockObjectsOnTarget moot = new MockObjectsOnTarget( mockDaoDriver, support.getDatabaseSupport() );
        moot.putObjectOnTape( "t1", "b1", "o1" );
        moot.putObjectOnTape( "t1", "b1", "o2" );
        moot.putObjectOnTape( "t2", "b1", "o3" );
        moot.putObjectOnTape( "t3", "b2", "o5" );
    
        final byte[] requestPayload = ( "<Objects>" + "<Object Name='o3'/>" + "<Object Name='o4'/>"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, "_rest_/bucket/b1/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.VERIFY_PHYSICAL_PLACEMENT.toString() )
                    .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
        driver.assertResponseToClientDoesNotContain( "t1" );
        driver.assertResponseToClientDoesNotContain( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o3" );
        driver.assertResponseToClientContains( "o4" );
    }
}
