/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class GetGroupMembersRequestHandler_Test 
{
    @Test
    public void testGetGroupMembersReturnsDetailedGroupMemberInformation()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        final Group mg1 = mockDaoDriver.createGroup( "mg1" );
        final Group mg2 = mockDaoDriver.createGroup( "mg2" );
        final Group mg3 = mockDaoDriver.createGroup( "mg3" );
        final Group mg4 = mockDaoDriver.createGroup( "mg4" );
        
        final GroupMember groupMember1 = mockDaoDriver.addGroupMemberToGroup( group1.getId(), mg1.getId() );
        final GroupMember groupMember2 = mockDaoDriver.addGroupMemberToGroup( group2.getId(), mg2.getId() );
        final GroupMember groupMember3 = mockDaoDriver.addGroupMemberToGroup( group3.getId(), mg3.getId() );
        final GroupMember groupMember4 = mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        final GroupMember groupMember5 = mockDaoDriver.addUserMemberToGroup( group2.getId(), user2.getId() );
        final GroupMember groupMember6 = mockDaoDriver.addUserMemberToGroup( group3.getId(), user3.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString()  );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientContains( user1.getId().toString() );
        driver.assertResponseToClientContains( user2.getId().toString() );
        driver.assertResponseToClientContains( user3.getId().toString() );
        driver.assertResponseToClientContains( group1.getId().toString() );
        driver.assertResponseToClientContains( group2.getId().toString() );
        driver.assertResponseToClientContains( group3.getId().toString() );
        driver.assertResponseToClientContains( mg1.getId().toString() );
        driver.assertResponseToClientContains( mg2.getId().toString() );
        driver.assertResponseToClientContains( mg3.getId().toString() );
        driver.assertResponseToClientContains( groupMember1.getId().toString() );
        driver.assertResponseToClientContains( groupMember2.getId().toString() );
        driver.assertResponseToClientContains( groupMember3.getId().toString() );
        driver.assertResponseToClientContains( groupMember4.getId().toString() );
        driver.assertResponseToClientContains( groupMember5.getId().toString() );
        driver.assertResponseToClientContains( groupMember6.getId().toString() );
        
        driver.assertResponseToClientDoesNotContain( user4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( group4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( mg4.getId().toString() );
    }
    
    
    @Test
    public void testGetGroupMembersWorksWithMemberUserIdlParameter()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group mg1 = mockDaoDriver.createGroup( "mg1" );
        final Group mg2 = mockDaoDriver.createGroup( "mg2" );
        final Group mg3 = mockDaoDriver.createGroup( "mg3" );
        
        final GroupMember groupMember1 = mockDaoDriver.addGroupMemberToGroup( group1.getId(), mg1.getId() );
        final GroupMember groupMember2 = mockDaoDriver.addGroupMemberToGroup( group2.getId(), mg2.getId() );
        final GroupMember groupMember3 = mockDaoDriver.addGroupMemberToGroup( group3.getId(), mg3.getId() );
        final GroupMember groupMember4 = mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        final GroupMember groupMember5 = mockDaoDriver.addUserMemberToGroup( group2.getId(), user2.getId() );
        final GroupMember groupMember6 = mockDaoDriver.addUserMemberToGroup( group3.getId(), user3.getId() );
        final GroupMember groupMember7 = mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString()  )
                        .addParameter( GroupMember.MEMBER_USER_ID, user1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientContains( user1.getId().toString() );
        driver.assertResponseToClientContains( groupMember4.getId().toString() );
        driver.assertResponseToClientContains( groupMember7.getId().toString() );
        driver.assertResponseToClientContains( group1.getId().toString() );
        driver.assertResponseToClientContains( group2.getId().toString() );
        
        driver.assertResponseToClientDoesNotContain( group3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember5.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember6.getId().toString() );
    }
    
        
    @Test
    public void testGetGroupMembersWorksWithMemberGroupIdlParameter()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group mg1 = mockDaoDriver.createGroup( "mg1" );
        final Group mg2 = mockDaoDriver.createGroup( "mg2" );
        final Group mg3 = mockDaoDriver.createGroup( "mg3" );
        
        final GroupMember groupMember1 = mockDaoDriver.addGroupMemberToGroup( group1.getId(), mg1.getId() );
        final GroupMember groupMember2 = mockDaoDriver.addGroupMemberToGroup( group2.getId(), mg2.getId() );
        final GroupMember groupMember3 = mockDaoDriver.addGroupMemberToGroup( group3.getId(), mg3.getId() );
        final GroupMember groupMember4 = mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        final GroupMember groupMember5 = mockDaoDriver.addUserMemberToGroup( group2.getId(), user2.getId() );
        final GroupMember groupMember6 = mockDaoDriver.addUserMemberToGroup( group3.getId(), user3.getId() );
        final GroupMember groupMember7 = mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        final GroupMember groupMember8 = mockDaoDriver.addGroupMemberToGroup( group2.getId(), mg1.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString()  )
                        .addParameter( GroupMember.MEMBER_GROUP_ID, mg1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientContains( group1.getId().toString() );
        driver.assertResponseToClientContains( groupMember1.getId().toString() );
        driver.assertResponseToClientContains( groupMember8.getId().toString() );
        driver.assertResponseToClientContains( mg1.getId().toString() );
        
        driver.assertResponseToClientDoesNotContain( mg3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember5.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember6.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember7.getId().toString() );
    }
    
    
    @Test
    public void testGetGroupMembersWorksWithGroupIdlParameter()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group mg1 = mockDaoDriver.createGroup( "mg1" );
        final Group mg2 = mockDaoDriver.createGroup( "mg2" );
        final Group mg3 = mockDaoDriver.createGroup( "mg3" );
        
        final GroupMember groupMember1 = mockDaoDriver.addGroupMemberToGroup( group1.getId(), mg1.getId() );
        final GroupMember groupMember2 = mockDaoDriver.addGroupMemberToGroup( group2.getId(), mg2.getId() );
        final GroupMember groupMember3 = mockDaoDriver.addGroupMemberToGroup( group3.getId(), mg3.getId() );
        final GroupMember groupMember4 = mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        final GroupMember groupMember5 = mockDaoDriver.addUserMemberToGroup( group2.getId(), user2.getId() );
        final GroupMember groupMember6 = mockDaoDriver.addUserMemberToGroup( group3.getId(), user3.getId() );
        final GroupMember groupMember7 = mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        final GroupMember groupMember8 = mockDaoDriver.addGroupMemberToGroup( group2.getId(), mg1.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString()  )
                        .addParameter( GroupMember.GROUP_ID, group1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientContains( group1.getId().toString() );
        driver.assertResponseToClientContains( user1.getId().toString() );
        driver.assertResponseToClientContains( groupMember1.getId().toString() );
        driver.assertResponseToClientContains( groupMember4.getId().toString() );
        driver.assertResponseToClientContains( mg1.getId().toString() );
        
        driver.assertResponseToClientDoesNotContain( mg2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( mg3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( group2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( group3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember5.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember6.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember7.getId().toString() );
        driver.assertResponseToClientDoesNotContain( groupMember8.getId().toString() );
    }
}
