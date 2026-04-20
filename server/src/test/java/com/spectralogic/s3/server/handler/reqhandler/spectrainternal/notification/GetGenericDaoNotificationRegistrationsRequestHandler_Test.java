/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.notification;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.notification.GenericDaoNotificationRegistration;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification.NotificationRegistrationTestUtil;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetGenericDaoNotificationRegistrationsRequestHandler_Test 
{
    @Test
    public void testGetGenericDaoNotificationRegistrationsReturnsRegistrations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String daoType1 = Bucket.class.getCanonicalName();
        final String daoType2 = S3Object.class.getCanonicalName();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final List< GenericDaoNotificationRegistration > registrations =
                NotificationRegistrationTestUtil.createRegistrations(
                        GenericDaoNotificationRegistration.class,
                        userId );
        registrations.get( 0 ).setDaoType( daoType1 );
        registrations.get( 1 ).setDaoType( daoType2 );
        NotificationRegistrationTestUtil.saveRegistrations( support.getDatabaseSupport(), registrations );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.GENERIC_DAO_NOTIFICATION_REGISTRATION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        new HttpResponseContentVerifier( driver )
                .verifyNotificationRegistrationNode(
                        "/Data/GenericDaoNotificationRegistration[1]/",
                        registrations.get( 0 ) )
                .verifyNotificationRegistrationNode(
                        "/Data/GenericDaoNotificationRegistration[2]/",
                        registrations.get( 1 ) );
        driver.assertResponseToClientXPathEquals(
                "/Data/GenericDaoNotificationRegistration[1]/DaoType",
                daoType1 );
        driver.assertResponseToClientXPathEquals(
                "/Data/GenericDaoNotificationRegistration[2]/DaoType",
                daoType2 );
    }
}
