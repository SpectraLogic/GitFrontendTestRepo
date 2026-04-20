/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetS3DataReplicationRulesRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Target target = mockDaoDriver.createS3Target( "sd" );
        final S3Target target2 = mockDaoDriver.createS3Target( "sd2" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final S3DataReplicationRule rule = mockDaoDriver.createS3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target.getId() );
        final S3DataReplicationRule rule2 = mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target2.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.S3_DATA_REPLICATION_RULE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( rule.getId().toString() );
        driver.assertResponseToClientContains( rule2.getId().toString() );
    }
}
