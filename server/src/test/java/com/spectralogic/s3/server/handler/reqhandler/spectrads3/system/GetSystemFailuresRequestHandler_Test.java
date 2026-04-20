/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.service.ds3.SystemFailureService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetSystemFailuresRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        support.getDatabaseSupport().getServiceManager().getService(
                SystemFailureService.class ).create(
                        SystemFailureType.values()[ 0 ], 
                        "some error", 
                        null );
        final SystemFailure failure = mockDaoDriver.attainOneAndOnly( SystemFailure.class );
        support.getDatabaseSupport().getServiceManager().getService(
                SystemFailureService.class ).create(
                        SystemFailureType.values()[ 0 ], 
                        "some error 2", 
                        null );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.SYSTEM_FAILURE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( failure.getId().toString() );
    }
}
