/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.notification.dispatch.NotificationListener;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.notification.dispatch.TransactionNotificationEventDispatcher;
import com.spectralogic.util.notification.domain.NotificationEvent;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BeansServiceManagerImpl_Test 
{
    @Test
    public void testBeansServiceManagerThrowsIllegalStateExceptionGettingTransactionSourceOnNonTransaction()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        final Set< Class< ? > > seedClassesInPackagesToSearchForServices = new HashSet<>();
        final BeansServiceManager bsm = BeansServiceManagerImpl.create(
                dbSupport.getServiceManager().getNotificationEventDispatcher(), 
                dbSupport.getDataManager(), 
                seedClassesInPackagesToSearchForServices );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                bsm.getTransactionSource();
            }
        });
    }
    
    
    @Test
    public void testGetBeanCreatorOnlyAllowedIfTypeHasBeanCreator()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();

        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                serviceManager.getCreator( null );
            }
        });
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                serviceManager.getCreator( School.class );
            }
        });
        serviceManager.getCreator( Teacher.class ).create( 
                BeanFactory.newBean( Teacher.class ).setName( "a" )
                .setDateOfBirth( new Date() ).setType( TeacherType.values()[ 0 ] ) );
        assertEquals(1,  serviceManager.getRetriever(Teacher.class).getCount(), "Shoulda modified dao as expected.");
    }
    
    
    @Test
    public void testGetBeanUpdaterOnlyAllowedIfTypeHasBeanUpdater()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();

        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {

            public void test() throws Throwable
            {
                serviceManager.getUpdater( null );
            }
        });
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                serviceManager.getUpdater( School.class );
            }
        });

        final Teacher teacher = BeanFactory.newBean( Teacher.class ).setName( "a" )
                .setDateOfBirth( new Date() ).setType( TeacherType.values()[ 0 ] );
        serviceManager.getService( TeacherService.class ).create( teacher );
        serviceManager.getUpdater( Teacher.class ).update( 
                teacher.setName( "b" ),
                Teacher.NAME );
        assertEquals("b", serviceManager.getRetriever( Teacher.class ).attain( Require.nothing() ).getName(), "Shoulda modified dao as expected.");
    }
    
    
    @Test
    public void testGetBeanDeleterOnlyAllowedIfTypeHasBeanDeleter()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();

        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                serviceManager.getDeleter( null );
            }
        });
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                serviceManager.getDeleter( School.class );
            }
        });
        
        final Teacher teacher = BeanFactory.newBean( Teacher.class ).setName( "a" )
                .setDateOfBirth( new Date() ).setType( TeacherType.values()[ 0 ] );
        serviceManager.getService( TeacherService.class ).create( teacher );
        serviceManager.getDeleter( Teacher.class ).delete( teacher.getId() );
        assertEquals(0,  serviceManager.getRetriever(Teacher.class).getCount(), "Shoulda modified dao as expected.");
    }
    
    
    @Test
    public void testCommitCommitsNotificationsDispatcher()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final MockNotificationDispatcher dispatcher = new MockNotificationDispatcher();
        final BeansServiceManager serviceManager = BeansServiceManagerImpl.create(
                dispatcher,
                dbSupport.getDataManager(),
                CollectionFactory.< Class< ? > >toSet( CountyService.class ) );
        
        BeansServiceManager transaction = serviceManager.startTransaction();
        try
        {
            transaction.getNotificationEventDispatcher().fire(
                    InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
            assertEquals(0,  dispatcher.m_events.size(), "Should notta fired anything yet.");
            transaction.commitTransaction();
            assertEquals(1,  dispatcher.m_events.size(), "Shoulda fired from commit.");
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        transaction = serviceManager.startTransaction();
        try
        {
            transaction.getNotificationEventDispatcher().fire(
                    InterfaceProxyFactory.getProxy( NotificationEvent.class, null ) );
            assertEquals(1,  dispatcher.m_events.size(), "Should notta fired anything yet.");
        }
        finally
        {
            transaction.closeTransaction();
        }
        assertEquals(1,  dispatcher.m_events.size(), "Should notta fired anything since not committed.");
    }
    
    
    private final static class MockNotificationDispatcher implements NotificationEventDispatcher
    {
        public void fire( final NotificationEvent< ? > event )
        {
            m_events.add( event );
        }

        @Override
        public void queueFire(NotificationEvent<?> event) {
            throw new IllegalStateException( "Mock dispatcher does not support queueing." );
        }


        public NotificationEventDispatcher startTransaction()
        {
            return new TransactionNotificationEventDispatcher( this );
        }

        
        public void commitTransaction()
        {
            throw new UnsupportedOperationException( "Not supported." );
        }
        
        
        private final List< NotificationEvent< ? > > m_events = new CopyOnWriteArrayList<>();


        public void registerListener(NotificationListener listener,
                                     Class<? extends HttpNotificationRegistration<?>> notificationRegistrationType)
        {
            throw new UnsupportedOperationException( "Not supported." );
            
        }


        public void unregisterListener(NotificationListener listener,
                                       Class<? extends HttpNotificationRegistration<?>> notificationRegistrationType)
        {
            throw new UnsupportedOperationException( "Not supported." );
            
        }
    } // end inner class def
}
