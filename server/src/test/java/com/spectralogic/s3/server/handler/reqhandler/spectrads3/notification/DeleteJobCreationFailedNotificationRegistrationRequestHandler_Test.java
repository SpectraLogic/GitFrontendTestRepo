/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.JobCreationFailedNotificationRegistration;
import com.spectralogic.s3.common.dao.service.notification.JobCreationFailedNotificationRegistrationService;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteJobCreationFailedNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testDeleteRegistrationThatExistsDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final JobCreationFailedNotificationRegistrationService service = 
                dbSupport.getServiceManager().getService(
                        JobCreationFailedNotificationRegistrationService.class );
        service.create(
                BeanFactory.newBean( JobCreationFailedNotificationRegistration.class )
                .setNotificationEndPoint( "a" ).setNotificationHttpMethod( RequestType.values()[ 0 ] ) );
        service.create(
                BeanFactory.newBean( JobCreationFailedNotificationRegistration.class )
                .setNotificationEndPoint( "b" ).setNotificationHttpMethod( RequestType.values()[ 0 ] ) );
        assertEquals(
                2,
                service.getCount(),
                "Shoulda been the initial registration."
                );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.JOB_CREATION_FAILED_NOTIFICATION_REGISTRATION.toString() + "/" 
                + service.retrieveAll().getFirst().getNotificationEndPoint() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                1,
                service.getCount(),
                "Shoulda deleted the registration."
                 );
    }
}
