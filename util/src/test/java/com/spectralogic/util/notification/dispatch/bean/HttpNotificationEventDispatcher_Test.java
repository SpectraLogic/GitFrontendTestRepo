/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.dispatch.bean;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.TestNotificationRegistration;
import com.spectralogic.util.db.mockservice.TestNotificationRegistrationService;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.notification.domain.NotificationEvent;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class HttpNotificationEventDispatcher_Test
{

    @Test
    public void testSimpleDispatchNullNotificationEventGeneratorNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() ).fire(
                        new MockBeansServiceManager().getRetriever(
                                HttpNotificationRegistration.class ),
                                new ArrayList< HttpNotificationRegistration< ? > >(),
                        null,
                        TestNotificationRegistration.class );
            }
        } );
    }
    

    @Test
    public void testSimpleDispatchNullRegistrationsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() ).fire(
                        new MockBeansServiceManager().getRetriever(
                                HttpNotificationRegistration.class ),
                        null,
                        new MockNotificationPayloadGenerator(),
                        TestNotificationRegistration.class );
            }
        } );
    }
    

    @Test
    public void testSimpleDispatchNoNotificationRegistrationsResultsInNoCallToGenerateEvent()
    {
        final MockNotificationPayloadGenerator generator = new MockNotificationPayloadGenerator();
        new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() ).fire(
                InterfaceProxyFactory.getProxy( TestNotificationRegistrationService.class, null ),
                new ArrayList< HttpNotificationRegistration< ? > >(),
                generator,
                TestNotificationRegistration.class );
        assertEquals(0,  generator.m_callCount.get(), "Should notta asked generator to generate the notification since no listeners.");
    }
    

    @Test
    public void testSimpleDispatchNotificationRegistrationsResultsInCallToGenerateEvent()
    {
        // This test has an external dependency on getting to spectralogic.com, so don't let it fail on the
        // build server for not being able to get to that URL.
        if ( !System.getProperty( "os.name" ).contains( "Windows" ) )
        {
            return;
        }
        
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( 
                        TestNotificationRegistration.class, TestNotificationRegistrationService.class );
        final MockNotificationPayloadGenerator generator = new MockNotificationPayloadGenerator();
        
        final TestNotificationRegistrationService service = 
                dbSupport.getServiceManager().getService( TestNotificationRegistrationService.class );
        final TestNotificationRegistration registration = 
                BeanFactory.newBean( TestNotificationRegistration.class )
                .setNotificationHttpMethod( RequestType.POST )
                .setNamingConvention( NamingConventionType.values()[ 0 ] )
                .setNotificationEndPoint( "https://www.spectralogic.com/" );
        service.create( registration );
        
        final List< HttpNotificationRegistration< ? > > registrations = new ArrayList<>();
        registrations.add( registration );
        new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() ).fire(
                dbSupport.getServiceManager().getService( TestNotificationRegistrationService.class ),
                registrations,
                generator,
                TestNotificationRegistration.class );
        assertEquals(1,  generator.m_callCount.get(), "Shoulda asked generator to generate the notification since there was a listener.");

        int i = 1000;
        while ( --i > 0 && null == service.attain( registration.getId() ).getLastHttpResponseCode() )
        {
            TestUtil.sleep( 10 );
        }
        final Object actual = service.attain( registration.getId() ).getLastHttpResponseCode();
        assertEquals(Integer.valueOf( 403 ),
                actual,
                "Shoulda recorded result of notification firing.");
        assertEquals(0,  service.attain(registration.getId()).getNumberOfFailuresSinceLastSuccess(), "Shoulda recorded result of notification firing.");
    }
    
    
    @Test
    public void testDispatchUnsupportedNotificationTypeNotAllowed()
    {
        final NotificationEventDispatcher dispatcher =
                new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() );
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                dispatcher.fire( InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
            }
        } );
    }
    
    
    @Test
    public void testFireWithoutUsingTransactionFiresImmediately()
    {
        final DatabaseSupport databaseSupport = DatabaseSupportFactory.getSupport(
                TestNotificationRegistration.class, TestNotificationRegistrationService.class );
        databaseSupport.getDataManager().createBean( BeanFactory.newBean( TestNotificationRegistration.class )
                .setNotificationHttpMethod( RequestType.values()[ 0 ] )
                .setNamingConvention( NamingConventionType.values()[ 0 ] )
                .setNotificationEndPoint( "localhost" ) );
        final MockNotificationPayloadGenerator generator = new MockNotificationPayloadGenerator();
        final NotificationEventDispatcher dispatcher = 
                new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() );
        final NotificationEventDispatcher transaction = dispatcher.startTransaction();
        assertNull(
                databaseSupport.getServiceManager().getRetriever( TestNotificationRegistration.class )
                        .retrieveAll().toList().get( 0 ).getLastNotification(),
                "Notification should notta come in yet."
                 );
        final Date date = new Date();
        dispatcher.fire( new HttpNotificationEvent( 
                databaseSupport.getServiceManager().getRetriever( TestNotificationRegistration.class ),
                generator ) );
        assertEquals(1,  generator.m_callCount.get(), "Shoulda asked generator to generate the notification since there was a listener.");
        transaction.commitTransaction();
        assertEquals(1,  generator.m_callCount.get(), "Shoulda asked generator to generate the notification since there was a listener.");

        int i = 1000;
        while ( --i > 0 )
        {
            final TestNotificationRegistration registration = 
                    databaseSupport.getServiceManager().getRetriever( TestNotificationRegistration.class )
                    .retrieveAll().toList().get( 0 );
            if ( null == registration.getLastNotification() )
            {
                TestUtil.sleep( 10 );
                continue;
            }
            assertTrue(date.getTime() <= registration.getLastNotification().getTime(), "Notification shoulda come in after firing.");
            assertEquals(1,  registration.getNumberOfFailuresSinceLastSuccess(), "Notification shoulda come in after firing.");
            assertNotNull(
                    "Shoulda recorded failure.",
                    registration.getLastFailure() );
            return;
        }
        
        fail( "Shoulda updated registration with notification fired at it." );
    }
    
    
    @Test
    public void testStartTransactionReturnsDispatcherThatDoesNotDispatchUntilCommitted()
    {
        final DatabaseSupport databaseSupport = DatabaseSupportFactory.getSupport(
                TestNotificationRegistration.class, TestNotificationRegistrationService.class );
        databaseSupport.getDataManager().createBean( BeanFactory.newBean( TestNotificationRegistration.class )
                .setNotificationHttpMethod( RequestType.values()[ 0 ] )
                .setNamingConvention( NamingConventionType.values()[ 0 ] )
                .setNotificationEndPoint( "localhost" ) );
        final MockNotificationPayloadGenerator generator = new MockNotificationPayloadGenerator();
        final NotificationEventDispatcher dispatcher = 
                new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() );
        final NotificationEventDispatcher transaction = dispatcher.startTransaction();
        transaction.fire( new HttpNotificationEvent( 
                databaseSupport.getServiceManager().getRetriever( TestNotificationRegistration.class ),
                generator ) );
        assertEquals(0,  generator.m_callCount.get(), "Should notta asked generator to generate the notification yet.");
        transaction.commitTransaction();
        assertEquals(1,  generator.m_callCount.get(), "Shoulda asked generator to generate the notification since there was a listener.");
    }
    
    
    @Test
    public void testCommitTransactionNotAllowed()
    {
        final NotificationEventDispatcher dispatcher = 
                new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                dispatcher.commitTransaction();
            }
        } );
    }
    
    
    private final static class MockNotificationPayloadGenerator implements NotificationPayloadGenerator
    {
        public NotificationPayload generateNotificationPayload()
        {
            m_callCount.incrementAndGet();
            return BeanFactory.newBean( TestBean.class ).setIntProp( 999 );
        }
        
        private final AtomicInteger m_callCount = new AtomicInteger();
    } // end inner class def
}
