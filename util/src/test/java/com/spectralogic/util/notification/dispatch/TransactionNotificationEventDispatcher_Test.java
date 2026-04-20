/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.dispatch;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.TestNotificationRegistration;
import com.spectralogic.util.db.mockservice.TestNotificationRegistrationService;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.notification.dispatch.bean.HttpNotificationEventDispatcher;
import com.spectralogic.util.notification.domain.NotificationEvent;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class TransactionNotificationEventDispatcher_Test
{
    @Test
    public void testConstructorNullDecoratedDispatcherNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TransactionNotificationEventDispatcher( null );
            }
        } );
    }
    
    
    @Test
    public void testStartTransactionNotAllowed()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final NotificationEventDispatcher decoratedDispatcher = 
                InterfaceProxyFactory.getProxy( NotificationEventDispatcher.class, btih );
        final TransactionNotificationEventDispatcher transaction =
                new TransactionNotificationEventDispatcher( decoratedDispatcher );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
             public void test() throws Throwable
            {
                transaction.startTransaction();
            }
        } );
    }
    
    
    @Test
    public void testFireNullEventNotAllowed()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final NotificationEventDispatcher decoratedDispatcher = 
                InterfaceProxyFactory.getProxy( NotificationEventDispatcher.class, btih );
        final TransactionNotificationEventDispatcher transaction =
                new TransactionNotificationEventDispatcher( decoratedDispatcher );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
             public void test() throws Throwable
            {
                transaction.fire( null );
            }
        } );
    }
    
    
    @Test
    public void testCommitTransactionCallsFireOnDecoratedDispatcherForEveryEventFiredOnTransaction()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final NotificationEventDispatcher decoratedDispatcher = 
                InterfaceProxyFactory.getProxy( NotificationEventDispatcher.class, btih );
        final TransactionNotificationEventDispatcher transaction =
                new TransactionNotificationEventDispatcher( decoratedDispatcher );
        
        final List< NotificationEvent< ? > > events = new ArrayList<>();
        events.add( InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
        events.add( InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
        events.add( InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
        transaction.fire( events.get( 0 ) );
        transaction.fire( events.get( 1 ) );
        transaction.fire( events.get( 2 ) );
        
        final Method fireMethod = ReflectUtil.getMethod( NotificationEventDispatcher.class, "fire" );
        assertEquals(0,  btih.getMethodCallCount(fireMethod), "Should notta made any fire calls yet.");
        transaction.commitTransaction();
        assertEquals(3,  btih.getMethodCallCount(fireMethod), "Shoulda fired events in the order in which they were fired on the transaction.");
        assertEquals(events.get( 0 ), btih.getMethodInvokeData( fireMethod ).get( 0 ).getArgs().get( 0 ), "Shoulda fired events in the order in which they were fired on the transaction.");
        assertEquals(events.get( 1 ), btih.getMethodInvokeData( fireMethod ).get( 1 ).getArgs().get( 0 ), "Shoulda fired events in the order in which they were fired on the transaction.");
        assertEquals(events.get( 2 ), btih.getMethodInvokeData( fireMethod ).get( 2 ).getArgs().get( 0 ), "Shoulda fired events in the order in which they were fired on the transaction.");
    }
    
    
    @Test
    public void testCommitTransactionWhenDecoratingHttpNotificationDispatcherDoesNotBlowUp()
    {
        final DatabaseSupport databaseSupport = DatabaseSupportFactory.getSupport(
                TestNotificationRegistration.class, TestNotificationRegistrationService.class );
        final NotificationEventDispatcher decoratedDispatcher = 
                new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() );
        final TransactionNotificationEventDispatcher transaction =
                new TransactionNotificationEventDispatcher( decoratedDispatcher );
        
        final BeansServiceManager t = databaseSupport.getServiceManager().startTransaction();
        final List< NotificationEvent< ? > > events = new ArrayList<>();
        events.add( new HttpNotificationEvent(
                t.getRetriever( TestNotificationRegistration.class ),
                new MockNotificationPayloadGenerator() ) );
        transaction.fire( events.get( 0 ) );
        
        transaction.commitTransaction();
        t.closeTransaction();
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
    
    
    @Test
    public void testFireEventsAfterTransactionCommittedNotAllowed()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final NotificationEventDispatcher decoratedDispatcher = 
                InterfaceProxyFactory.getProxy( NotificationEventDispatcher.class, btih );
        final TransactionNotificationEventDispatcher transaction =
                new TransactionNotificationEventDispatcher( decoratedDispatcher );
        
        final List< NotificationEvent< ? > > events = new ArrayList<>();
        events.add( InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
        events.add( InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
        events.add( InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
        transaction.fire( events.get( 0 ) );
        transaction.fire( events.get( 1 ) );
        
        transaction.commitTransaction();
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
             public void test() throws Throwable
            {
                transaction.fire( events.get( 2 ) );
            }
        } );
    }
}
