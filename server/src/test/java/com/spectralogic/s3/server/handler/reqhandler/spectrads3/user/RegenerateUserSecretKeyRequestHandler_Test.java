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
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class RegenerateUserSecretKeyRequestHandler_Test 
{
    @Test
    public void testRegenerateUserSecretKeyAllowedIfSameUser()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        mockDaoDriver.createUser( "the_other_user_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "_rest_/user/the_user_name" )
                        .addParameter( "operation", RestOperationType.REGENERATE_SECRET_KEY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final User updatedUser = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( UserService.class )
                .retrieve( NameObservable.NAME, user.getName() );

        driver.assertResponseToClientXPathEquals( "/Data/AuthId", user.getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/Id", user.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", user.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/SecretKey", updatedUser.getSecretKey() );
        
        assertNotEquals(
                user.getSecretKey(),
                updatedUser.getSecretKey(),
                "Should notta retained the same secret key."
                 );
        
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to data planner."
               );
    }
    
    
    @Test
    public void testRegenerateUserSecretKeyAllowedIfDifferentUserAndRequestingUserIsAdmin()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        final User otherUser = mockDaoDriver.createUser( "the_other_user_name" );
        mockDaoDriver.addUserMemberToGroup(
                mockDaoDriver.getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), 
                user.getId() );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "_rest_/user/" + otherUser.getName() )
                        .addParameter( "operation", RestOperationType.REGENERATE_SECRET_KEY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final User updatedUser = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( UserService.class )
                .retrieve( NameObservable.NAME, otherUser.getName() );

        driver.assertResponseToClientXPathEquals( "/Data/AuthId", otherUser.getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/Id", otherUser.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", otherUser.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/SecretKey", updatedUser.getSecretKey() );
        
        assertNotEquals(
                user.getSecretKey(),
                updatedUser.getSecretKey(),
                "Should notta retained the same secret key."
                 );
    }
    
    
    @Test
    public void testRegenerateUserSecretKeyAllowedIfDifferentUserAndRequestingUserIsInternalRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        final User otherUser = mockDaoDriver.createUser( "the_other_user_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/user/" + otherUser.getName() )
                        .addParameter( "operation", RestOperationType.REGENERATE_SECRET_KEY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final User updatedUser = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( UserService.class )
                .retrieve( NameObservable.NAME, otherUser.getName() );

        driver.assertResponseToClientXPathEquals( "/Data/AuthId", otherUser.getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/Id", otherUser.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", otherUser.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/SecretKey", updatedUser.getSecretKey() );
        
        assertNotEquals(
                user.getSecretKey(),
                updatedUser.getSecretKey(),
                "Should notta retained the same secret key."
                );
    }
    
    
    @Test
    public void testRegenerateUserSecretKeyNotAllowedIfDifferentUserAndRequestingUserIsNonAdmin()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        final User otherUser = mockDaoDriver.createUser( "the_other_user_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "_rest_/user/" + otherUser.getName() )
                        .addParameter( "operation", RestOperationType.REGENERATE_SECRET_KEY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        final User updatedUser = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( UserService.class )
                .retrieve( NameObservable.NAME, otherUser.getName() );

        driver.assertResponseToClientDoesNotContain( user.getAuthId() );
        driver.assertResponseToClientDoesNotContain( user.getSecretKey() );
        
        assertEquals(
                user.getSecretKey(),
                updatedUser.getSecretKey(),
                "Shoulda retained the same secret key."
                 );
    }
}
