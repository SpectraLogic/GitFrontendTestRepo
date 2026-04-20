/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetUserRequestHandler_Test 
{
    @Test
    public void testGetUserAsInternalRequestWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        mockDaoDriver.createUser( "the_other_user_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER + "/" + user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/AuthId", user.getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/Id", user.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", user.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/SecretKey", user.getSecretKey() );
    }
    

    @Test
    public void testGetUserForCurrentUserWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        mockDaoDriver.createUser( "the_other_user_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER + "/" + user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/AuthId", user.getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/Id", user.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", user.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/SecretKey", user.getSecretKey() );
    }
    

    @Test
    public void testGetUserThatDoesNotExistReturns404WhenAdministratorAnd403Otherwise()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        final User user2 = mockDaoDriver.createUser( "the_other_user_name" );

        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER + "/" + "oops" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user2.getId() );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER + "/" + "oops" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    

    @Test
    public void testGetUserForDifferentUserOnlyAllowedForAdministrators()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        final User user2 = mockDaoDriver.createUser( "the_other_user_name" );

        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER + "/" + user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user2.getId() );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER + "/" + user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/AuthId", user.getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/Id", user.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", user.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/SecretKey", user.getSecretKey() );
    }
}
