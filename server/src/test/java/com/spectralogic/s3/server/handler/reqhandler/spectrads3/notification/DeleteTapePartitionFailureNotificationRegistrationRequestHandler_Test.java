/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.notification.TapePartitionFailureNotificationRegistration;
import com.spectralogic.s3.common.dao.service.notification.TapePartitionFailureNotificationRegistrationService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteTapePartitionFailureNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testDeleteRegistrationThatExistsDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID userId = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( MockDaoDriver.DEFAULT_USER_NAME )
                .getId();
        NotificationRegistrationTestUtil.saveRegistrations(
                support.getDatabaseSupport(),
                NotificationRegistrationTestUtil.createRegistrations(
                        TapePartitionFailureNotificationRegistration.class,
                        userId ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/"
                    + RestDomainType.TAPE_PARTITION_FAILURE_NOTIFICATION_REGISTRATION.toString()
                    + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                1,
                support
                        .getDatabaseSupport()
                        .getServiceManager()
                        .getService( TapePartitionFailureNotificationRegistrationService.class )
                        .getCount(),
                "Shoulda deleted the registration."
                 );
    }
    
    
    @Test
    public void testDeleteNotificationCreatedBySameUserAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID userId = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( MockDaoDriver.DEFAULT_USER_NAME )
                .getId();
        NotificationRegistrationTestUtil.saveRegistrations(
                support.getDatabaseSupport(),
                NotificationRegistrationTestUtil.createRegistrations(
                        TapePartitionFailureNotificationRegistration.class,
                        userId ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userId.toString() ),
                RequestType.DELETE, 
                "_rest_/"
                    + RestDomainType.TAPE_PARTITION_FAILURE_NOTIFICATION_REGISTRATION.toString()
                    + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    
    
    @Test
    public void testDeleteNotificationCreatedByDifferentUserAllowedIfUserIsAdministrator()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID userId = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( MockDaoDriver.DEFAULT_USER_NAME )
                .getId();
        NotificationRegistrationTestUtil.saveRegistrations(
                support.getDatabaseSupport(),
                NotificationRegistrationTestUtil.createRegistrations(
                        TapePartitionFailureNotificationRegistration.class,
                        userId ) );
        
        final User user2 = new MockDaoDriver( support.getDatabaseSupport() ).createUser( "user2" );
        new MockDaoDriver( support.getDatabaseSupport() ).addUserMemberToGroup( 
                BuiltInGroup.ADMINISTRATORS, user2.getId() );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.DELETE, 
                "_rest_/"
                    + RestDomainType.TAPE_PARTITION_FAILURE_NOTIFICATION_REGISTRATION.toString()
                    + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    
    
    @Test
    public void testDeleteNotificationCreatedByDifferentUserNotAllowedIfUserNotAdministrator()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID userId = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( MockDaoDriver.DEFAULT_USER_NAME )
                .getId();
        NotificationRegistrationTestUtil.saveRegistrations(
                support.getDatabaseSupport(),
                NotificationRegistrationTestUtil.createRegistrations(
                        TapePartitionFailureNotificationRegistration.class,
                        userId ) );
        
        final User user2 = new MockDaoDriver( support.getDatabaseSupport() ).createUser( "user2" );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.DELETE, 
                "_rest_/"
                    + RestDomainType.TAPE_PARTITION_FAILURE_NOTIFICATION_REGISTRATION.toString()
                    + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
