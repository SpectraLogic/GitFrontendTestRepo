/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class GetBlobsOnTapeRequestHandler_Test 
{
    @Test
    public void testtestGetBlobsOnTapeReturnsOnlyThoseBlobsOnThatTape()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Blob b1 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "o1" ).getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "o2" ).getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "o3" ).getId() );
        mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "o4" ).getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b3.getId() );
    
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, "_rest_/" + RestDomainType.TAPE.toString() + "/" + t1.getId()
                                                                                      .toString() ).addParameter(
                "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() )
                                                                                                   .addParameter(
                                                                                                           RequestParameterType.PAGE_LENGTH.toString(),
                                                                                                           "3" )
                                                                                                   .addParameter(
                                                                                                           RequestParameterType.PAGE_OFFSET.toString(),
                                                                                                           "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "o1" );
        driver.assertResponseToClientContains( "o2" );
        driver.assertResponseToClientDoesNotContain( "o3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
    }
}
