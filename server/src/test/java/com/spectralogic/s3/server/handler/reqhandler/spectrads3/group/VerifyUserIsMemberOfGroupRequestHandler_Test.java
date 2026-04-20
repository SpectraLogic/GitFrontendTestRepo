/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class VerifyUserIsMemberOfGroupRequestHandler_Test 
{
    @Test
    public void testVerifyOnCurrentUserAllowedWhenInGroup()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        mockDaoDriver.createUser( "the_other_user_name" );

        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.EVERYONE, user.getId() );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP + "/" + BuiltInGroup.EVERYONE.getName() )
            .addParameter( "operation", RestOperationType.VERIFY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "Everyone" );
    }
    
    
    @Test
    public void testVerifyOnCurrentUserAllowedWhenNotInGroup()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        mockDaoDriver.createUser( "the_other_user_name" );

        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.EVERYONE, user.getId() );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP + "/" + BuiltInGroup.ADMINISTRATORS.getName() )
            .addParameter( "operation", RestOperationType.VERIFY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver.assertResponseToClientDoesNotContain( "Administrators" );
    }
    
    
    @Test
    public void testVerifyOnSameUserExplicitlyAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        mockDaoDriver.createUser( "the_other_user_name" );

        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.EVERYONE, user.getId() );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP + "/" + BuiltInGroup.EVERYONE.getName() )
            .addParameter( "operation", RestOperationType.VERIFY.toString() )
            .addParameter( UserIdObservable.USER_ID, user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testVerifyOnDifferentUserAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "the_user_name" );
        final User user2 = mockDaoDriver.createUser( "the_other_user_name" );

        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.EVERYONE, user.getId() );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP + "/" + BuiltInGroup.EVERYONE.getName() )
            .addParameter( "operation", RestOperationType.VERIFY.toString() )
            .addParameter( UserIdObservable.USER_ID, user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
