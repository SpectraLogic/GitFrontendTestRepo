/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;


import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteAzureDataReplicationRuleRequestHandler_Test 
{
    @Test
    public void testDeleteDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "sd" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "sd2" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target.getId() );
        mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target2.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.AZURE_DATA_REPLICATION_RULE + "/" + rule.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( 
                AzureDataReplicationRule.class ).attain(
                        Require.nothing() );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                 );
    }
}
