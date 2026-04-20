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
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteGroupRequestHandler_Test 
{
    @Test
    public void testDeleteOnAdministratorsGroupNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final int numBuiltInGroups = dbSupport.getServiceManager().getRetriever( Group.class ).getCount();
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.ADMINISTRATORS );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        assertEquals(numBuiltInGroups,  dbSupport.getServiceManager().getRetriever(Group.class).getCount(), "Should notta deleted group.");
    }
    
    
    @Test
    public void testDeleteOnEveryoneGroupNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final int numBuiltInGroups = dbSupport.getServiceManager().getRetriever( Group.class ).getCount();
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.EVERYONE );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        assertEquals(numBuiltInGroups,  dbSupport.getServiceManager().getRetriever(Group.class).getCount(), "Should notta deleted group.");
    }
    

    @Test
    public void testDeleteOnNonBuiltInGroupAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final int numBuiltInGroups = dbSupport.getServiceManager().getRetriever( Group.class ).getCount();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group group = mockDaoDriver.createGroup( "customgr" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(numBuiltInGroups,  dbSupport.getServiceManager().getRetriever(Group.class).getCount(), "Shoulda deleted group.");
    }
}
