/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.spectraview.SpectraViewRestRequest;
import com.spectralogic.s3.common.platform.spectraview.SpectraViewRestRequest.SpectraViewRestRequestOverrideRunner;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class DelegateDeleteUserRequestHandler_Test 
{
    @Test
    public void testRequestHandlerFailsIfFailsToDeleteUser()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        
        SpectraViewRestRequest.setOverrideRunner( new SpectraViewRestRequestOverrideRunner()
        {
            public String run()
            {
                return "";
            }
        } );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.USER + "/" + user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 500 );
    }
    
    
    @Test
    public void testRequestHandlerSuccessfulWhenSucceedsToDeleteUser()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        
        SpectraViewRestRequest.setOverrideRunner( new SpectraViewRestRequestOverrideRunner()
        {
            public String run()
            {
                mockDaoDriver.delete( User.class, user );
                return "";
            }
        } );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.USER + "/" + user.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
}
