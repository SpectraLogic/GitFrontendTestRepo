/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetObjectPersistedNotificationRegistrationsRequestHandler_Test 
{
    @Test
    public void testGetObjectPersistedNotificationRegistrationsReturnsRegistrations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final UUID jobId = mockDaoDriver.createJob( null, userId, JobRequestType.values()[0] ).getId();
        final List< S3ObjectPersistedNotificationRegistration > registrations =
                NotificationRegistrationTestUtil.createRegistrations(
                        S3ObjectPersistedNotificationRegistration.class,
                        userId );
        registrations.get( 0 ).setJobId( jobId );
        NotificationRegistrationTestUtil.saveRegistrations( support.getDatabaseSupport(), registrations );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.OBJECT_PERSISTED_NOTIFICATION_REGISTRATION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        new HttpResponseContentVerifier( driver )
                .verifyNotificationRegistrationNode(
                        "/Data/S3ObjectPersistedNotificationRegistration[1]/",
                        registrations.get( 0 ) )
                .verifyNotificationRegistrationNode(
                        "/Data/S3ObjectPersistedNotificationRegistration[2]/",
                        registrations.get( 1 ) );
        driver.assertResponseToClientXPathEquals(
                "/Data/S3ObjectPersistedNotificationRegistration[1]/JobId",
                jobId.toString() );
        driver.assertResponseToClientXPathEquals(
                "/Data/S3ObjectPersistedNotificationRegistration[2]/JobId",
                "" );
    }
}
