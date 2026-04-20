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
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.http.RequestType;

public final class GetStorageDomainFailureNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testGetStorageDomainFailureNotificationRegistrationCreatesRegistration()
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
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN_FAILURE_NOTIFICATION_REGISTRATION.toString() 
                + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        new HttpResponseContentVerifier( driver )
                .verifyNotificationRegistrationNode( "/Data/", registrations.get( 0 ) );
    }
    
    
    @Test
    public void testGetStorageDomainFailureNotificationRegistrationReturns404WhenRegistrationDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final DataManager dataManager = support.getDatabaseSupport().getDataManager();
        final StorageDomainFailureNotificationRegistration registration2 =
                BeanFactory.newBean( StorageDomainFailureNotificationRegistration.class );
        registration2
                .setNotificationEndPoint( "b" )
                .setUserId( userId );
        dataManager.createBean( registration2 );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN_FAILURE_NOTIFICATION_REGISTRATION.toString() 
                + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
}
