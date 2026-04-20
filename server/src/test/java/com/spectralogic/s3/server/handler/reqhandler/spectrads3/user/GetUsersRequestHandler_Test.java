/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetUsersRequestHandler_Test 
{
    @Test
    public void testGetUsers()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final List< User > users = CollectionFactory.toList(
                mockDaoDriver.createUser( "the_other_user_name" ),
                mockDaoDriver.createUser( "the_user_name" ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver.assertResponseToClientXPathEquals( "/Data/User[1]/AuthId", users.get( 0 ).getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/User[1]/Id", users.get( 0 ).getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/User[1]/Name", users.get( 0 ).getName() );
        driver.assertResponseToClientXPathEquals( "/Data/User[1]/SecretKey", users.get( 0 ).getSecretKey() );

        driver.assertResponseToClientXPathEquals( "/Data/User[2]/AuthId", users.get( 1 ).getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/User[2]/Id", users.get( 1 ).getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/User[2]/Name", users.get( 1 ).getName() );
        driver.assertResponseToClientXPathEquals( "/Data/User[2]/SecretKey", users.get( 1 ).getSecretKey() );
    }
}
