/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.List;

import org.junit.jupiter.api.Test;



import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.service.notification.S3ObjectCachedNotificationRegistrationService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public final class DeleteObjectCachedNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testDeleteObjectCachedNotificationRegistrationDeletesBean()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final S3ObjectCachedNotificationRegistrationService service = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( S3ObjectCachedNotificationRegistrationService.class );
        final S3ObjectCachedNotificationRegistration beanToDelete = newBean( "a" );
        service.create( beanToDelete );
        service.create( newBean( "b" ) );
        
        new MockDaoDriver( support.getDatabaseSupport() ).createUser( MockDaoDriver.DEFAULT_USER_NAME );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.OBJECT_CACHED_NOTIFICATION_REGISTRATION + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        final List< S3ObjectCachedNotificationRegistration > beans = service.retrieveAll().toList();
        assertEquals(
                1,
                beans.size() ,
                "Shoulda only had one bean left.");
        assertNotEquals(
                beanToDelete.getId(),
                beans.get( 0 ).getId(),
                "The deleted id should notta remained in the database."
                 );
    }
    
    
    @Test
    public void testDeleteObjectCachedNotificationRegistrationReturns404WhenBeanDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final S3ObjectCachedNotificationRegistrationService service = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( S3ObjectCachedNotificationRegistrationService.class );
        
        new MockDaoDriver( support.getDatabaseSupport() ).createUser( MockDaoDriver.DEFAULT_USER_NAME );
        service.create( newBean( "b" ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.OBJECT_CACHED_NOTIFICATION_REGISTRATION + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
        
        assertEquals(
                1,
                service.getCount(),
                "Shoulda had one bean left." );
    }
    
    
    private static S3ObjectCachedNotificationRegistration newBean( final String endPoint )
    {
        final S3ObjectCachedNotificationRegistration notificationRegistration =
                BeanFactory.newBean( S3ObjectCachedNotificationRegistration.class );
        notificationRegistration.setNotificationEndPoint( endPoint );
        notificationRegistration.setNumberOfFailuresSinceLastSuccess( 0 );
        return notificationRegistration;
    }
}
