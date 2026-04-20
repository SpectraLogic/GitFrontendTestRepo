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
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteGroupMemberRequestHandler_Test 
{
    @Test
    public void testDeleteOnEveryoneGroupNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.EVERYONE );
        final Group mg = mockDaoDriver.createGroup( "membergroup" );
        final GroupMember member = mockDaoDriver.addGroupMemberToGroup( group.getId(), mg.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString() + "/" + member.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        assertEquals(1,  dbSupport.getServiceManager().getRetriever(GroupMember.class).getCount(), "Should notta deleted group member.");
    }
    

    @Test
    public void testDeleteOnAdministratorsGroupAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.ADMINISTRATORS );
        final Group mg = mockDaoDriver.createGroup( "membergroup" );
        final GroupMember member = mockDaoDriver.addGroupMemberToGroup( group.getId(), mg.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString() + "/" + member.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(GroupMember.class).getCount(), "Shoulda deleted group member.");
    }
    

    @Test
    public void testDeleteOnNonBuiltInGroupWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group group = mockDaoDriver.createGroup( "abc" );
        final Group mg = mockDaoDriver.createGroup( "membergroup" );
        final GroupMember member = mockDaoDriver.addGroupMemberToGroup( group.getId(), mg.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.GROUP_MEMBER.toString() + "/" + member.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(GroupMember.class).getCount(), "Shoulda deleted group member.");
    }
}
