/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
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

public final class CreateAzureDataReplicationRuleRequestHandler_Test 
{
    @Test
    public void testCreateDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy" );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "sd" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.AZURE_DATA_REPLICATION_RULE )
                .addParameter(
                        DataReplicationRule.TYPE,
                        DataReplicationRuleType.values()[ 0 ].toString() )
                .addParameter(
                        DataReplicationRule.TARGET_ID,
                        target.getName() )
                .addParameter(
                        DataPlacement.DATA_POLICY_ID,
                        dataPolicy.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( 
                AzureDataReplicationRule.class ).attain(
                        Require.nothing() );
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                 );
    }
}
