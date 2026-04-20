/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class GetBlobsOnAzureTargetRequestHandler_Test 
{
    @Test
    public void testGetBlobsOnAzureTargetReturnsOnlyThoseBlobsOnThatAzureTarget()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget azureObj1 = mockDaoDriver.createAzureTarget( "azureObj1" );
        final AzureTarget azureObj2 = mockDaoDriver.createAzureTarget( "azureObj2" );
        
        final Blob blob1 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj1" ).getId() );
        final Blob blob2 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj2" ).getId() );
        final Blob blob3 = mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj3" ).getId() );
        mockDaoDriver.getBlobFor( mockDaoDriver.createObject( null, "obj4" ).getId() );
      
        mockDaoDriver.putBlobOnAzureTarget( azureObj1.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnAzureTarget( azureObj1.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnAzureTarget( azureObj2.getId(), blob3.getId() );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.AZURE_TARGET.toString() + "/" + azureObj1.getId().toString() )
        .addParameter( "operation", RestOperationType.GET_PHYSICAL_PLACEMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "obj1" );
        driver.assertResponseToClientContains( "obj2" );
        driver.assertResponseToClientDoesNotContain( "obj3" );
        driver.assertResponseToClientDoesNotContain( "obj4" );
    }
}
