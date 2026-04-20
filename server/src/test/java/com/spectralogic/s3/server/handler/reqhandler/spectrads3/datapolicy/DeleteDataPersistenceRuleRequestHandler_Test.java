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
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteDataPersistenceRuleRequestHandler_Test 
{
    @Test
    public void testDeleteDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.values()[ 0 ], storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.values()[ 0 ], storageDomain2.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( DataPersistenceRule.class ).attain(
                Require.nothing() );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                );
    }
}
