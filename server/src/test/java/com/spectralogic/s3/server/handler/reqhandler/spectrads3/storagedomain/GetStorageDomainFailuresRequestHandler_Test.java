/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainFailureService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetStorageDomainFailuresRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        support.getDatabaseSupport().getServiceManager().getService(
                StorageDomainFailureService.class ).create(
                        storageDomain.getId(), 
                        StorageDomainFailureType.values()[ 0 ], 
                        "some error", 
                        null );
        final StorageDomainFailure failure = mockDaoDriver.attainOneAndOnly( StorageDomainFailure.class );
        support.getDatabaseSupport().getServiceManager().getService(
                StorageDomainFailureService.class ).create(
                        storageDomain.getId(), 
                        StorageDomainFailureType.values()[ 0 ], 
                        "some error 2", 
                        null );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN_FAILURE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( failure.getId().toString() );
    }
}
