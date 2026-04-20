/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.JobCompletedNotificationRegistration;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CreateJobCompletedNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testCreateRequestWithoutAllRequiredPropertiesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.JOB_COMPLETED_NOTIFICATION_REGISTRATION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever(
                        JobCompletedNotificationRegistration.class ).getCount(),
                "Should notta created notification registration."
                 );
    }
    
    
    @Test
    public void testCreateRequestWithAllRequiredPropertiesCreatesRegistration()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.JOB_COMPLETED_NOTIFICATION_REGISTRATION.toString() )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_END_POINT, "a" )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD, 
                           RequestType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever(
                        JobCompletedNotificationRegistration.class ).getCount(),
                "Shoulda created notification registration."
                 );
        dbSupport.getServiceManager().getRetriever( JobCompletedNotificationRegistration.class ).attain( 
                UserIdObservable.USER_ID, null );
    }
    
    
    @Test
    public void testCreateRequestWithUserIdSpecifiedNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final User user2 = mockDaoDriver.createUser( "jason" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getId().toString() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.JOB_COMPLETED_NOTIFICATION_REGISTRATION.toString() )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_END_POINT, "a" )
            .addParameter( UserIdObservable.USER_ID, user2.getName() )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD, 
                           RequestType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever(
                        JobCompletedNotificationRegistration.class ).getCount(),
                "Should notta created notification registration."
                 );
    }
    
    
    @Test
    public void testCreateRequestWithUserIdImpliedUserDoesNotHaveAccessNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "jason" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getId().toString() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.JOB_COMPLETED_NOTIFICATION_REGISTRATION.toString() )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_END_POINT, "a" )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD, 
                           RequestType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreateRequestWithUserIdImpliedCreatesRegistration()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "jason" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getId().toString() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.JOB_COMPLETED_NOTIFICATION_REGISTRATION.toString() )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_END_POINT, "a" )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD, 
                           RequestType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever(
                        JobCompletedNotificationRegistration.class ).getCount(),
                "Shoulda created notification registration."
                 );
        dbSupport.getServiceManager().getRetriever( JobCompletedNotificationRegistration.class ).attain( 
                UserIdObservable.USER_ID, user.getId() );
    }
}
