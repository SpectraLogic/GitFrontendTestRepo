/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;


import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetDataPersistenceRuleRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.values()[ 0 ], storageDomain.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.values()[ 0 ], storageDomain2.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( rule.getId().toString() );
        driver.assertResponseToClientDoesNotContain( rule2.getId().toString() );
    }
}
