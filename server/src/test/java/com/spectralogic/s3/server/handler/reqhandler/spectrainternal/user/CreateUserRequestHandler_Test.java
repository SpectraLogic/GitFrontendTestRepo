/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.user;

import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class CreateUserRequestHandler_Test
{
    
    @Test
    public void testCreateUserWithoutNameNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertFalse(
                driver.getResponseToClientAsString().contains( "jason" ),
                "Response payload should notta included user name, since it wasn't created."
                 );
    }
    
    
    @Test
    public void testCreateUserWithoutSecretKeyWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        assertTrue(driver.getResponseToClientAsString().contains( "jason" ), "Response payload shoulda included user just created.");
    }
    
    
    @Test
    public void testCreateUserWithExplicitIdWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID id = UUID.randomUUID();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" )
                .addParameter( User.SECRET_KEY, "secretkey" )
                .addParameter( Identifiable.ID, id.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        assertTrue(driver.getResponseToClientAsString().contains( "jason" ), "Response payload shoulda included user just created.");
        assertTrue(driver.getResponseToClientAsString().contains( "secret" ), "Response payload shoulda included user just created.");
        assertTrue(driver.getResponseToClientAsString().contains( id.toString() ), "Response payload shoulda included user just created.");
    }
    
    
    @Test
    public void testCreateUserWithExplicitMaxBucketsWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID id = UUID.randomUUID();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" )
                .addParameter( User.MAX_BUCKETS, "42" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        assertTrue(driver.getResponseToClientAsString().contains( "jason" ), "Response payload shoulda included user just created.");
        assertTrue(driver.getResponseToClientAsString().contains( "MaxBuckets" ), "Response payload shoulda included max buckets.");
        assertTrue(driver.getResponseToClientAsString().contains( "42" ), "Response payload shoulda included max bucket value.");
    }
    
    
    @Test
    public void testCreateUserWithToFewMaxBucketsFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID id = UUID.randomUUID();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" )
                .addParameter( User.MAX_BUCKETS, "-42" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateUserWithSecretKeyWorksIfValid()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" )
                .addParameter( User.SECRET_KEY, "secret" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" )
                .addParameter( User.SECRET_KEY, "secretkey" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        assertTrue(driver.getResponseToClientAsString().contains( "jason" ), "Response payload shoulda included user just created.");
        assertTrue(driver.getResponseToClientAsString().contains( "secret" ), "Response payload shoulda included user just created.");
    }
    
    
    @Test
    public void testCreateUserAsAdministratorNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user" );
        final Group group = support.getDatabaseSupport().getServiceManager().getService( 
                GroupService.class ).getBuiltInGroup( BuiltInGroup.ADMINISTRATORS );
        mockDaoDriver.addUserMemberToGroup( group.getId(), user.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreateUserAsNonAdministratorNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
