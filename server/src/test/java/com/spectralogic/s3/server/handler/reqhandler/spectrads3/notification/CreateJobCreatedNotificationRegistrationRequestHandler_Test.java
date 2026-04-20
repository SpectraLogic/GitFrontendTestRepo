/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.notification.JobCreatedNotificationRegistration;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CreateJobCreatedNotificationRegistrationRequestHandler_Test 
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
                "_rest_/" + RestDomainType.JOB_CREATED_NOTIFICATION_REGISTRATION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever(
                        JobCreatedNotificationRegistration.class ).getCount(),
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
                "_rest_/" + RestDomainType.JOB_CREATED_NOTIFICATION_REGISTRATION.toString() )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_END_POINT, "a" )
            .addParameter( HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD, 
                           RequestType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever(
                        JobCreatedNotificationRegistration.class ).getCount(),
                "Shoulda created notification registration."
                 );
    }
}
