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

import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.http.RequestType;

public final class GetObjectPersistedNotificationRegistrationRequestHandler_Test 
{
    @Test
    public void testGetObjectPersistedNotificationRegistrationCreatesRegistration()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final UUID jobId = mockDaoDriver.createJob( null, userId, JobRequestType.values()[0] ).getId();
        final List< S3ObjectPersistedNotificationRegistration > registrations =
                NotificationRegistrationTestUtil.createRegistrations(
                        S3ObjectPersistedNotificationRegistration.class,
                        userId );
        registrations.get( 0 ).setJobId( jobId );
        NotificationRegistrationTestUtil.saveRegistrations( support.getDatabaseSupport(), registrations );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.OBJECT_PERSISTED_NOTIFICATION_REGISTRATION.toString() + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        new HttpResponseContentVerifier( driver )
                .verifyNotificationRegistrationNode( "/Data/", registrations.get( 0 ) );
        driver.assertResponseToClientXPathEquals( "/Data/JobId", jobId.toString() );
    }
    
    
    @Test
    public void testGetObjectPersistedNotificationRegistrationReturns404WhenRegistrationDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final DataManager dataManager = support.getDatabaseSupport().getDataManager();
        final S3ObjectPersistedNotificationRegistration registration2 =
                BeanFactory.newBean( S3ObjectPersistedNotificationRegistration.class );
        registration2
                .setNotificationEndPoint( "b" )
                .setUserId( userId );
        dataManager.createBean( registration2 );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.OBJECT_PERSISTED_NOTIFICATION_REGISTRATION.toString() + "/a" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
}
