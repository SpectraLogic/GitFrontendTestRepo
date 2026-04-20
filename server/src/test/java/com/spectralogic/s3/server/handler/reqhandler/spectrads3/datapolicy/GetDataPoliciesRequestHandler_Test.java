/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;


import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetDataPoliciesRequestHandler_Test
{
    @Test
    public void testGetAsInternalRequestDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.addDataPolicyAcl( dataPolicy.getId(), null, user.getId() );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.addDataPolicyAcl( dataPolicy2.getId(), null, user.getId() );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "policy3" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.DATA_POLICY );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( dataPolicy.getId().toString() );
        driver.assertResponseToClientContains( dataPolicy2.getId().toString() );
        driver.assertResponseToClientContains( dataPolicy3.getId().toString() );
    }
    
    
    @Test
    public void testGetAsUserRequestDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        mockDaoDriver.addDataPolicyAcl( dataPolicy.getId(), null, user.getId() );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "policy2" );
        mockDaoDriver.addDataPolicyAcl( dataPolicy2.getId(), null, user.getId() );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "policy3" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.DATA_POLICY );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( dataPolicy.getId().toString() );
        driver.assertResponseToClientContains( dataPolicy2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( dataPolicy3.getId().toString() );
    }
}
