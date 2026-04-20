/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.nio.charset.Charset;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class GetPhysicalPlacementForObjectsRequestHandler_Test 
{
    @Test
    public void testDistinguishesBadPayloadFromNoPayload()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Key=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectOnTapeReturnsCorrectPlacements1()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "t1" );
        driver.assertResponseToClientDoesNotContain( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectOnTapeReturnsCorrectPlacements2()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "t1" );
        driver.assertResponseToClientContains( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectThatDoesNotExistReturnsNoPlacementInfo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "t1" );
        driver.assertResponseToClientDoesNotContain( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectNotOnTapeReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "t1" );
        driver.assertResponseToClientDoesNotContain( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsAllOnTapeReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object Name=\"o3\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "t1" );
        driver.assertResponseToClientContains( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsWhereSomeObjectsDoNotExistOnTapeReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object Name=\"o3\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "t1" );
        driver.assertResponseToClientContains( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsNotAllOnTapeReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( bucket1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( bucket1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( bucket2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( bucket2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t1.setBarCode( "t1" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t2.setBarCode( "t2" ), Tape.BAR_CODE );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).update( 
                t3.setBarCode( "t3" ), Tape.BAR_CODE );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + bucket1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "t1" );
        driver.assertResponseToClientContains( "t2" );
        driver.assertResponseToClientDoesNotContain( "t3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectOnPoolReturnsCorrectPlacements1()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "p1" );
        driver.assertResponseToClientDoesNotContain( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectOnPoolReturnsCorrectPlacements2()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectNotOnPoolReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientDoesNotContain( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsAllOnPoolReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object Name=\"o3\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsWhereSomeObjectsDoNotExistOnPoolReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object Name=\"o3\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o1" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsNotAllOnPoolReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsOnDs3TargetsReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );
        
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "ds3t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "ds3t2" );
        final Ds3Target target3 = mockDaoDriver.createDs3Target( "ds3t3" );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target3.getId(), b3.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        
        driver.assertResponseToClientDoesNotContain( "ds3t1" );
        driver.assertResponseToClientContains( "ds3t2" );
        driver.assertResponseToClientContains( "ds3t3" );
    }
    
    
    @Test
    public void 
    testGetPhysicalPlacementForObjectsOnDs3TargetsReturnsCorrectPlacementsWhenFilteredByStorageDomain()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b3.getId() );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), p1.getPartitionId() );        
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), p1.getPartitionId() );
        mockDaoDriver.updateBean( 
                p3.setStorageDomainMemberId( sdm2.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "ds3t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "ds3t2" );
        final Ds3Target target3 = mockDaoDriver.createDs3Target( "ds3t3" );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnDs3Target( target3.getId(), b3.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.addParameter( RequestParameterType.STORAGE_DOMAIN.toString(), sd1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        
        driver.assertResponseToClientDoesNotContain( "ds3t1" );
        driver.assertResponseToClientDoesNotContain( "ds3t2" );
        driver.assertResponseToClientDoesNotContain( "ds3t3" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsOnAzureTargetsReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );
        
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "azuret1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "azuret2" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "azuret3" );
        mockDaoDriver.putBlobOnAzureTarget( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target3.getId(), b3.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        
        driver.assertResponseToClientDoesNotContain( "azuret1" );
        driver.assertResponseToClientContains( "azuret2" );
        driver.assertResponseToClientContains( "azuret3" );
    }
    
    
    @Test
    public void 
    testGetPhysicalPlacementForObjectsOnAzureTargetsReturnsCorrectPlacementsWhenFilteredByStorageDomain()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b3.getId() );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), p1.getPartitionId() );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), p1.getPartitionId() );
        mockDaoDriver.updateBean( 
                p3.setStorageDomainMemberId( sdm2.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "azuret1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "azuret2" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "azuret3" );
        mockDaoDriver.putBlobOnAzureTarget( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target3.getId(), b3.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.addParameter( RequestParameterType.STORAGE_DOMAIN.toString(), sd1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        
        driver.assertResponseToClientDoesNotContain( "azuret1" );
        driver.assertResponseToClientDoesNotContain( "azuret2" );
        driver.assertResponseToClientDoesNotContain( "azuret3" );
    }
    
    
    @Test
    public void testGetPhysicalPlacementForObjectsOnS3TargetsReturnsCorrectPlacements()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b5.getId() );
        
        final S3Target target1 = mockDaoDriver.createS3Target( "s3t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "s3t2" );
        final S3Target target3 = mockDaoDriver.createS3Target( "s3t3" );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnS3Target( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnS3Target( target3.getId(), b3.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        
        driver.assertResponseToClientDoesNotContain( "s3t1" );
        driver.assertResponseToClientContains( "s3t2" );
        driver.assertResponseToClientContains( "s3t3" );
    }
    
    
    @Test
    public void 
    testGetPhysicalPlacementForObjectsOnS3TargetsReturnsCorrectPlacementsWhenFilteredByStorageDomain()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Bucket buckep1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket buckep2 = mockDaoDriver.createBucket( null, "b2" );
        
        final S3Object o1 = mockDaoDriver.createObject( buckep1.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( buckep1.getId(), "o2" );
        final S3Object o3 = mockDaoDriver.createObject( buckep1.getId(), "o3" );
        final S3Object o4 = mockDaoDriver.createObject( buckep1.getId(), "o4" );
        final S3Object o5 = mockDaoDriver.createObject( buckep2.getId(), "o5" );
        final S3Object o6 = mockDaoDriver.createObject( buckep2.getId(), "o6" );
        
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.getBlobFor( o4.getId() );
        mockDaoDriver.getBlobFor( o5.getId() );
        mockDaoDriver.getBlobFor( o6.getId() );
        
        final Pool p1 = mockDaoDriver.createPool();
        final Pool p2 = mockDaoDriver.createPool();
        final Pool p3 = mockDaoDriver.createPool();
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p1.setName( "p1" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p2.setName( "p2" ), NameObservable.NAME );
        support.getDatabaseSupport().getServiceManager().getService( PoolService.class ).update( 
                p3.setName( "p3" ), NameObservable.NAME );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnPool( p3.getId(), b3.getId() );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember sdm1 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), p1.getPartitionId() );
        mockDaoDriver.updateBean( 
                p2.setStorageDomainMemberId( sdm1.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), p1.getPartitionId() );
        mockDaoDriver.updateBean( 
                p3.setStorageDomainMemberId( sdm2.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final S3Target target1 = mockDaoDriver.createS3Target( "s3t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "s3t2" );
        final S3Target target3 = mockDaoDriver.createS3Target( "s3t3" );
        mockDaoDriver.putBlobOnS3Target( target1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnS3Target( target2.getId(), b3.getId() );
        mockDaoDriver.putBlobOnS3Target( target3.getId(), b3.getId() );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" />"
                + "<Object Name=\"o4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/bucket/" + buckep1.getName() + "/" )
                    .setRequestPayload( requestPayload )
                    .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.addParameter( RequestParameterType.STORAGE_DOMAIN.toString(), sd1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "p1" );
        driver.assertResponseToClientContains( "p2" );
        driver.assertResponseToClientDoesNotContain( "p3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        
        driver.assertResponseToClientDoesNotContain( "s3t1" );
        driver.assertResponseToClientDoesNotContain( "s3t2" );
        driver.assertResponseToClientDoesNotContain( "s3t3" );
    }
}
