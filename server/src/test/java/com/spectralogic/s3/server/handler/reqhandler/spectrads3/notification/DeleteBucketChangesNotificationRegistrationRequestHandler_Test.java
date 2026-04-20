/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import java.util.List;

import org.junit.jupiter.api.Test;



import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.s3.common.dao.service.notification.BucketChangesNotificationRegistrationService;
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

public final class DeleteBucketChangesNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testDeleteBucketChangesNotificationRegistrationDeletesBean()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final BucketChangesNotificationRegistrationService service = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( BucketChangesNotificationRegistrationService.class );
        final BucketChangesNotificationRegistration beanToDelete = newBean( "a" );
        service.create( beanToDelete );
        service.create( newBean( "b" ) );
        
        new MockDaoDriver( support.getDatabaseSupport() ).createUser( MockDaoDriver.DEFAULT_USER_NAME );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.BUCKET_CHANGES_NOTIFICATION_REGISTRATION + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        final List< BucketChangesNotificationRegistration > beans = service.retrieveAll().toList();
        assertEquals(
                1,
                beans.size(),
                "Shoulda only had one bean left."  );
        assertNotEquals(
                beanToDelete.getId(),
                beans.get( 0 ).getId(),
                "The deleted id should notta remained in the database."
                 );
    }
    
    
    @Test
    public void testDeleteBucketChangesNotificationRegistrationReturns404WhenBeanDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final BucketChangesNotificationRegistrationService service = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( BucketChangesNotificationRegistrationService.class );
        
        new MockDaoDriver( support.getDatabaseSupport() ).createUser( MockDaoDriver.DEFAULT_USER_NAME );
        service.create( newBean( "b" ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.BUCKET_CHANGES_NOTIFICATION_REGISTRATION + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
        
        assertEquals(
                1,
                service.getCount(),
                "Shoulda had one bean left." );
    }
    
    
    private static BucketChangesNotificationRegistration newBean( final String endPoint )
    {
        final BucketChangesNotificationRegistration notificationRegistration =
                BeanFactory.newBean( BucketChangesNotificationRegistration.class );
        notificationRegistration.setNotificationEndPoint( endPoint );
        notificationRegistration.setNumberOfFailuresSinceLastSuccess( 0 );
        return notificationRegistration;
    }
}
