/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.notification.S3TargetFailureNotificationRegistration;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;

public final class GetS3TargetFailureNotificationRegistrationsRequestHandler_Test 
{
    @Test
    public void testGetS3TargetFailureNotificationRegistrationsReturnsRegistrations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final List< S3TargetFailureNotificationRegistration > registrations =
                NotificationRegistrationTestUtil.createRegistrations(
                        S3TargetFailureNotificationRegistration.class,
                        userId );
        NotificationRegistrationTestUtil.saveRegistrations( support.getDatabaseSupport(), registrations );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.S3_TARGET_FAILURE_NOTIFICATION_REGISTRATION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        new HttpResponseContentVerifier( driver )
                .verifyNotificationRegistrationNode(
                        "/Data/S3TargetFailureNotificationRegistration[1]/",
                        registrations.get( 0 ) )
                .verifyNotificationRegistrationNode(
                        "/Data/S3TargetFailureNotificationRegistration[2]/",
                        registrations.get( 1 ) );
    }
}
