/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.notification.StorageDomainFailureNotificationRegistration;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetStorageDomainFailureNotificationRegistrationsRequestHandler_Test 
{
    @Test
    public void testGetObjectPersistedNotificationRegistrationsReturnsRegistrations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final List< StorageDomainFailureNotificationRegistration > registrations =
                NotificationRegistrationTestUtil.createRegistrations(
                        StorageDomainFailureNotificationRegistration.class,
                        userId );
        NotificationRegistrationTestUtil.saveRegistrations( support.getDatabaseSupport(), registrations );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN_FAILURE_NOTIFICATION_REGISTRATION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        new HttpResponseContentVerifier( driver )
                .verifyNotificationRegistrationNode(
                        "/Data/StorageDomainFailureNotificationRegistration[1]/",
                        registrations.get( 0 ) )
                .verifyNotificationRegistrationNode(
                        "/Data/StorageDomainFailureNotificationRegistration[2]/",
                        registrations.get( 1 ) );
    }
}
