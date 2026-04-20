/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class GetGroupsRequestHandler_Test 
{
    @Test
    public void testGetGroupReturnsDetailedGroupInformation()
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
        
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), mg1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), mg2.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), mg3.getId() );
        mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user3.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.GROUP.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientContains( "group1" );
        driver.assertResponseToClientDoesNotContain( "user1" );
        driver.assertResponseToClientContains( "mg1" );
        driver.assertResponseToClientContains( "group2" );
        driver.assertResponseToClientDoesNotContain( "user2" );
        driver.assertResponseToClientContains( "mg2" );
        driver.assertResponseToClientContains( "group3" );
        driver.assertResponseToClientDoesNotContain( "user3" );
        driver.assertResponseToClientContains( "mg3" );
    }
}
