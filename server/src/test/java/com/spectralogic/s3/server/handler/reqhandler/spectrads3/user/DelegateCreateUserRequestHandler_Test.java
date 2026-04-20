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
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.spectraview.SpectraViewRestRequest;
import com.spectralogic.s3.common.platform.spectraview.SpectraViewRestRequest.SpectraViewRestRequestOverrideRunner;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.http.RequestType;

public final class DelegateCreateUserRequestHandler_Test 
{
    @Test
    public void testRequestHandlerFailsIfFailsToCreateUser()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        SpectraViewRestRequest.setOverrideRunReturnValue( "" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER ).addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 500 );
    }
    
    
    @Test
    public void testRequestHandlerSuccessfulWhenSucceedsToCreateUser()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        SpectraViewRestRequest.setOverrideRunner( new SpectraViewRestRequestOverrideRunner()
        {
            public String run()
            {
                mockDaoDriver.createUser( "jason" );
                return "";
            }
        } );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        driver.assertResponseToClientContains( "jason" );
    }
    
    
    @Test
    public void testRequestHandlerFailsWhenUserAlreadyExistsById()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "barry" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER )
                    .addParameter( NameObservable.NAME, "jason" )
                    .addParameter( Identifiable.ID, user.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }
    
    
    @Test
    public void testRequestHandlerFailsWhenUserAlreadyExistsByName()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "jason" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER ).addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }
}
