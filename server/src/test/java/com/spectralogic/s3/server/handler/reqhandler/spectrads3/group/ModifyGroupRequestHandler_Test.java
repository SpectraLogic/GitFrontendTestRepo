/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ModifyGroupRequestHandler_Test 
{
    @Test
    public void testModifyOnAdministratorsGroupNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.ADMINISTRATORS );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() )
                        .addParameter( NameObservable.NAME, "newname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        final Object expected = group.getName();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Group.class ).attain( group.getId() ).getName(), "Should notta updated group.");
    }
    
    
    @Test
    public void testModifyOnEveryoneGroupNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final Group group = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                BuiltInGroup.EVERYONE );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() )
                        .addParameter( NameObservable.NAME, "newname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        final Object expected = group.getName();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Group.class ).attain( group.getId() ).getName(), "Should notta updated group.");
    }
    

    @Test
    public void testModifyOnNonBuiltInGroupAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group group = mockDaoDriver.createGroup( "customgr" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() )
                    .addParameter( NameObservable.NAME, "newname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals("newname", dbSupport.getServiceManager().getRetriever( Group.class ).attain( group.getId() ).getName(), "Shoulda updated group.");
    }
    
    
    @Test
    public void testPropOptionalForModsButReqAtBeanDefNullValueShouldThrow400()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final String initialGroupname = "blank-group";
        final String updateGroupNameTo = "";
        assertEquals(0,  updateGroupNameTo.length(), "updateGroupNameTo is zero chars.");
        final Group group = mockDaoDriver.createGroup( initialGroupname );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() )
                    .addParameter( NameObservable.NAME, updateGroupNameTo );
        driver.run();
        driver.assertHttpResponseCodeEquals( GenericFailure.BAD_REQUEST.getHttpResponseCode() );
        
        final String updatedName = dbSupport.getServiceManager().getRetriever( 
                Group.class ).attain( group.getId() ).getName();
        assertEquals(initialGroupname, updatedName, "Shoulda NOT have updated group.");
    }
    
    
    @Test
    public void testPropOptionalForModsButReqAtBeanDefOneSpaceShouldWork()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final String initialGroupname = "blank-group";
        final String updateGroupNameTo = " ";
        assertEquals(1,  updateGroupNameTo.length(), "updateGroupNameTo is one space, legal but dumb.");
        final Group group = mockDaoDriver.createGroup( initialGroupname );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.GROUP.toString() + "/" + group.getName() )
                    .addParameter( NameObservable.NAME, updateGroupNameTo );
        driver.run();
        driver.assertHttpResponseCodeEquals( HttpServletResponse.SC_OK );
        
        final String updatedName = dbSupport.getServiceManager().getRetriever( 
                Group.class ).attain( group.getId() ).getName();
        assertEquals(updateGroupNameTo, updatedName, "Shoulda NOT have updated group.");
    }
}
