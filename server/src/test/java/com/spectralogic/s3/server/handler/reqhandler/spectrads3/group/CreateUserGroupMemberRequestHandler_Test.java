/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CreateUserGroupMemberRequestHandler_Test 
{

    @Test
    public void testCreateWithoutAllRequiredPropertiesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createGroup( "group" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(GroupMember.class).getCount(), "Should notta created group member.");
    }
    
    
    @Test
    public void testCreateOnEveryoneGroupNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.EVERYONE );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString() )
            .addParameter( GroupMember.GROUP_ID, group.getName() )
            .addParameter( GroupMember.MEMBER_USER_ID, user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(GroupMember.class).getCount(), "Should notta created group member.");
    }
    

    @Test
    public void testCreateOnAdministratorsGroupAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.ADMINISTRATORS );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString() )
            .addParameter( GroupMember.GROUP_ID, group.getName() )
            .addParameter( GroupMember.MEMBER_USER_ID, user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(GroupMember.class).getCount(), "Shoulda created group member.");
    }
    

    @Test
    public void testCreateWithAllRequiredPropertiesWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Group group = mockDaoDriver.createGroup( "group" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString() )
            .addParameter( GroupMember.GROUP_ID, group.getName() )
            .addParameter( GroupMember.MEMBER_USER_ID, user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(GroupMember.class).getCount(), "Shoulda created group member.");
    }
}
