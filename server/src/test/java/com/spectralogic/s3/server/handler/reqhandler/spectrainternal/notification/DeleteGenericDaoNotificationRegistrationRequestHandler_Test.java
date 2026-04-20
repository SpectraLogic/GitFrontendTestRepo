/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.notification;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.notification.GenericDaoNotificationRegistration;
import com.spectralogic.s3.common.dao.service.notification.GenericDaoNotificationRegistrationService;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class DeleteGenericDaoNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testDeleteRegistrationThatExistsDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final GenericDaoNotificationRegistrationService service = 
                dbSupport.getServiceManager().getService(
                        GenericDaoNotificationRegistrationService.class );
        service.create(
                BeanFactory.newBean( GenericDaoNotificationRegistration.class )
                .setDaoType( "aaa" )
                .setNotificationEndPoint( "a" ).setNotificationHttpMethod( RequestType.values()[ 0 ] ) );
        service.create(
                BeanFactory.newBean( GenericDaoNotificationRegistration.class )
                .setDaoType( "bbb" )
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
                "_rest_/" + RestDomainType.GENERIC_DAO_NOTIFICATION_REGISTRATION.toString() + "/" 
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
