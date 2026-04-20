/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class EjectStorageDomainBlobsRequestHandler_Test 
{
    @Test
    public void testtestEjectWithoutRequestPayloadNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        support.setTapeInterfaceIh( btih );
        
        final UUID storageDomainId = UUID.randomUUID();
        final UUID bucketId = UUID.randomUUID();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( RequestParameterType.BLOBS.toString(), "" )
                        .addParameter( "operation", RestOperationType.EJECT.toString() )
                        .addParameter( RequestParameterType.STORAGE_DOMAIN.toString(), storageDomainId.toString() )
                        .addParameter( PersistenceTarget.BUCKET_ID, bucketId.toString() )
                        .addParameter( Tape.EJECT_LABEL, "label" )
                        .addParameter( Tape.EJECT_LOCATION, "location" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testtestEjectBucketWithBlobsSpecifiedCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final Method method = ReflectUtil.getMethod( TapeManagementResource.class, "ejectStorageDomain" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        support.setTapeInterfaceIh( btih );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "user" );
        mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.createObject( bucket.getId(), "o2" );
        mockDaoDriver.createObject( bucket.getId(), "o3" );

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final UUID storageDomainId = mockDaoDriver.createStorageDomain( "sd1" ).getId();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( RequestParameterType.BLOBS.toString(), "" )
                        .addParameter( "operation", RestOperationType.EJECT.toString() )
                        .addParameter(RequestParameterType.STORAGE_DOMAIN.toString(), storageDomainId.toString() )
                        .addParameter( PersistenceTarget.BUCKET_ID, bucket.getName() )
                        .addParameter( Tape.EJECT_LABEL, "label" )
                        .addParameter( Tape.EJECT_LOCATION, "location" )
                        .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(1,  btih.getMethodCallCount(method), "Shoulda sent only the expected storageDomain id.");
        assertEquals(storageDomainId, btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 0 ), "Shoulda sent only the expected storageDomain id.");
        final Object expected = bucket.getId();
        assertEquals(expected, btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 1 ), "Shoulda sent only the expected bucket id.");
        assertEquals("label", btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 2 ), "Shoulda sent down specified eject tape attributes.");
        assertEquals("location", btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 3 ), "Shoulda sent down specified eject tape attributes.");
        assertEquals(2,  Array.getLength(btih.getMethodInvokeData(method).get(0).getArgs().get(4)), "Should notta sent down any blobs.");
    }
}
