/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;


import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteDataPolicyAclRequestHandler_Test
{
    @Test
    public void testDeleteDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        support.getDatabaseSupport().getDataManager().deleteBeans( DataPolicyAcl.class, Require.nothing() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final DataPolicy policy = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy policy2 = mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.addDataPolicyAcl( policy.getId(), null, user.getId() );
        mockDaoDriver.addDataPolicyAcl( policy2.getId(), null, user.getId() );
        mockDaoDriver.addDataPolicyAcl( policy2.getId(), null, user2.getId() );
        final DataPolicyAcl acl = mockDaoDriver.addDataPolicyAcl(
                policy.getId(), null, user3.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.DATA_POLICY_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                3,
                support.getDatabaseSupport().getServiceManager().getRetriever(
                        DataPolicyAcl.class ).getCount(),
                "Shoulda deleted data policy ACL."
                 );
    }
}
