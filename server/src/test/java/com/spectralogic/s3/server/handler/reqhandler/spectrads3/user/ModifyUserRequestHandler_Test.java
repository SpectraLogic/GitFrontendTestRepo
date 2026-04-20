/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;

public final class ModifyUserRequestHandler_Test 
{
    @Test
    public void testModifyUser()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final User user = mockDaoDriver.createUser( "starting_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/user/starting_name" )
                        .addParameter( NameObservable.NAME, "foobar" )
                        .addParameter( User.MAX_BUCKETS, "5" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final User updatedUser = mockDaoDriver.attain( user );
        assertNotEquals(
                user.getAuthId(),
                updatedUser.getAuthId(),
                "Should notta had the same authid as we had before."
                 );
        
        assertEquals(
                "foobar",
                updatedUser.getName(),
                "Shoulda had the updated name." );
        assertEquals(
                5,
                updatedUser.getMaxBuckets(),
                "Shoulda had the updated max buckets." );

        driver.assertResponseToClientXPathEquals( "/Data/Id", user.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/AuthId", updatedUser.getAuthId() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", updatedUser.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/SecretKey", "mySecretKey" );
        
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to data planner."
                 );
    }
    
    
    @Test
    public void testModifyUserFailsNegativeBuckets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final User user = mockDaoDriver.createUser( "starting_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/user/starting_name" )
                        .addParameter( NameObservable.NAME, "foobar" )
                        .addParameter( User.MAX_BUCKETS, "-5" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        }
    
    
    @Test
    public void testModifyUserSecretKeyAllowedIfValid()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final User user = mockDaoDriver.createUser( "starting_name" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/user/starting_name" )
                        .addParameter( User.SECRET_KEY, "inv-alid" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(
                user.getSecretKey(),
                mockDaoDriver.attain( user ).getSecretKey(),
                "Should notta updated secret key."
                 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/user/starting_name" )
                        .addParameter( User.SECRET_KEY, "valid123" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(
                "valid123",
                mockDaoDriver.attain( user ).getSecretKey(),
                "Shoulda updated secret key."
                 );
    }
}
