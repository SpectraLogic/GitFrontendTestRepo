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

import com.spectralogic.s3.common.dao.domain.notification.JobCreatedNotificationRegistration;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetJobCreatedNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testGetJobCreatedNotificationRegistrationByEndPoint()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID userId = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( MockDaoDriver.DEFAULT_USER_NAME )
                .getId();
        final List< JobCreatedNotificationRegistration > registrations =
                NotificationRegistrationTestUtil.createRegistrations(
                        JobCreatedNotificationRegistration.class,
                        userId );
        NotificationRegistrationTestUtil.saveRegistrations( support.getDatabaseSupport(), registrations );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.JOB_CREATED_NOTIFICATION_REGISTRATION.toString() + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        new HttpResponseContentVerifier( driver )
                .verifyNotificationRegistrationNode( "/Data/", registrations.get( 0 ) );
    }
}
