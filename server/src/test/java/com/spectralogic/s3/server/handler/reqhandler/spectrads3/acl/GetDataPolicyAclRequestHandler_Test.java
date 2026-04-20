/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;


import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetDataPolicyAclRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy policy = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicyAcl acl = mockDaoDriver.addDataPolicyAcl( 
                policy.getId(), mockDaoDriver.createGroup( "g1" ).getId(), null );
        final DataPolicyAcl acl2 = mockDaoDriver.addDataPolicyAcl( 
                policy.getId(), mockDaoDriver.createGroup( "g2" ).getId(), null );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.DATA_POLICY_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( acl.getId().toString() );
        driver.assertResponseToClientDoesNotContain( acl2.getId().toString() );
    }
}
