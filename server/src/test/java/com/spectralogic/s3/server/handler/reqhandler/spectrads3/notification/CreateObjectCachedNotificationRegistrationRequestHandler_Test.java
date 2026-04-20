/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CreateObjectCachedNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testCreateObjectCachedNotificationRegistrationCreatesBean()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.OBJECT_CACHED_NOTIFICATION_REGISTRATION.toString() )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_END_POINT, "a" )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD, 
                           RequestType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        assertEquals(
                1,
                support.getDatabaseSupport().getServiceManager().getRetriever(
                        S3ObjectCachedNotificationRegistration.class ).getCount(),
                "Shoulda created notification registration."
                );
    }
}
