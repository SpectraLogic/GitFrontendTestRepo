/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.notification.Ds3TargetFailureNotificationRegistration;
import com.spectralogic.s3.common.dao.service.notification.Ds3TargetFailureNotificationRegistrationService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteDs3TargetFailureNotificationRegistrationRequestHandler_Test 
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
                        Ds3TargetFailureNotificationRegistration.class,
                        userId ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.DS3_TARGET_FAILURE_NOTIFICATION_REGISTRATION.toString() + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                1,
                support
                        .getDatabaseSupport()
                        .getServiceManager()
                        .getService( Ds3TargetFailureNotificationRegistrationService.class )
                        .getCount(),

                "Shoulda deleted the registration."
                 );
    }
}
