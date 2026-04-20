/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class ConvertStorageDomainToDs3TargetRequestHandler_Test 
{
    @Test
    public void testConvertDelegatesToDataPolicyManagementResource()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        
        final UUID targetId = UUID.randomUUID();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
            .addParameter( RequestParameterType.CONVERT_TO_DS3_TARGET.toString(), targetId.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data policy management rpc resource."
                 );
    }
}
