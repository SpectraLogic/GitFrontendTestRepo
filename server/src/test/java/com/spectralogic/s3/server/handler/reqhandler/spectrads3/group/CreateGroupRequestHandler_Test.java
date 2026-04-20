/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CreateGroupRequestHandler_Test 
{
    @Test
    public void testCreateWithoutAllRequiredPropertiesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final int numBuiltInGroups = dbSupport.getServiceManager().getRetriever( Group.class ).getCount();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.GROUP.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(numBuiltInGroups,  dbSupport.getServiceManager().getRetriever(Group.class).getCount(), "Should notta created group.");
    }
    
    
    @Test
    public void testCreateWithAllRequiredPropertiesWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final int numBuiltInGroups = dbSupport.getServiceManager().getRetriever( Group.class ).getCount();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.GROUP.toString() )
            .addParameter( NameObservable.NAME, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );

        final Object actual = dbSupport.getServiceManager().getRetriever( Group.class ).getCount();
        assertEquals(numBuiltInGroups + 1, actual, "Shoulda created group.");
    }
}
