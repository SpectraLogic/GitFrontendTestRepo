/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;


import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteDs3DataReplicationRuleRequestHandler_Test 
{
    @Test
    public void testDeleteDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "sd" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "sd2" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final Ds3DataReplicationRule rule = mockDaoDriver.createDs3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target.getId() );
        mockDaoDriver.createDs3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target2.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.DS3_DATA_REPLICATION_RULE + "/" + rule.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( Ds3DataReplicationRule.class ).attain(
                Require.nothing() );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                 );
    }
}
