/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class GetBlobsOnDs3TargetRequestHandler_Test 
{
    @Test
    public void testGetBlobsOnDs3TargetReturnsOnlyThoseBlobsOnThatDs3Target()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target ds3Obj1 = mockDaoDriver.createDs3Target( "ds3Obj1" );
        final Ds3Target ds3Obj2 = mockDaoDriver.createDs3Target( "ds3Obj2" );
        
        final Blob blob1 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj1" ).getId() );
        final Blob blob2 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj2" ).getId() );
        final Blob blob3 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj3" ).getId() );
        mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj4" ).getId() );
      
        mockDaoDriver.putBlobOnDs3Target( ds3Obj1.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnDs3Target( ds3Obj1.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnDs3Target( ds3Obj2.getId(), blob3.getId() );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DS3_TARGET.toString() + "/" + ds3Obj1.getId().toString() )
        .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "obj1" );
        driver.assertResponseToClientContains( "obj2" );
        driver.assertResponseToClientDoesNotContain( "obj3" );
        driver.assertResponseToClientDoesNotContain( "obj4" );
    }
}
