/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class MutexServiceImpl_Test 
{
    @Test
    public void testRunWithNullLockNameNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final MutexService service = dbSupport.getServiceManager().getService( MutexService.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                service.run( null, InterfaceProxyFactory.getProxy( Runnable.class, null ) );
            }
            } );
    }
    

    @Test
    public void testRunWithNullRunnableNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final MutexService service = dbSupport.getServiceManager().getService( MutexService.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                service.run( "lock", null );
            }
            } );
    }
    

    @Test
    public void testRunRunsRunnable()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final MutexService service = dbSupport.getServiceManager().getService( MutexService.class );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        service.run( "lock", InterfaceProxyFactory.getProxy( Runnable.class, btih ) );
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda called run."
                 );
    }
    
    
    @Test
    public void testAcquireEventuallyDeletesStaleLockIfNeverReleased()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final MutexServiceImpl service =
                (MutexServiceImpl)dbSupport.getServiceManager().getService( MutexService.class );

        UUID mutexId = service.acquireLock( "lock1" );
        service.releaseLock( mutexId );
        
        Duration duration = new Duration();
        mutexId = service.acquireLock( "lock1" );
        service.releaseLock( mutexId );
        assertTrue(
                1 > duration.getElapsedSeconds(),
                "Shoulda been able to acquire lock very quickly.");

        mutexId = service.acquireLock( "lock1" );
        duration = new Duration();
        mutexId = service.acquireLock( "lock1", 150 );
        service.releaseLock( mutexId );
        assertTrue(
                100 < duration.getElapsedMillis(),
                "Shoulda been able to acquire lock very quickly.");
    }
    
    
    @Test
    public void testConcurrentLockAcquireReleaseWorks() throws InterruptedException
    {
        DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final int numberOfLocks = 4;
        final int numberOfThreadsPerLock = 6;
        final int numberOfThreads = numberOfLocks * numberOfThreadsPerLock;
        final int numberOfAcquisitionsPerThread = 8;
        final int sleepInMillisBetweenAcquisitions = 1;
        final WorkPool wp = WorkPoolFactory.createWorkPool( numberOfThreads, getClass().getSimpleName() );
        
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final Set< LockAcquirer > runnables = new HashSet<>();
        for ( int i = 0; i < numberOfThreadsPerLock; ++i )
        {
            dbSupport = DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
            
            final MutexServiceImpl service = new MutexServiceImpl();
            service.setInitParams( 
                    dbSupport.getServiceManager(), 
                    dbSupport.getDataManager(),
                    dbSupport.getServiceManager().getNotificationEventDispatcher() );
            
            for ( int j = 0; j < numberOfLocks; ++j )
            {
                final String lockName = "Lock" + String.valueOf( j );
                final LockAcquirer runnable = new LockAcquirer(
                        startLatch,
                        lockName,
                        numberOfAcquisitionsPerThread, 
                        sleepInMillisBetweenAcquisitions,
                        service );
                runnables.add( runnable );
                wp.submit( runnable );
            }
        }
        
        startLatch.countDown();
        for ( final LockAcquirer runnable : runnables )
        {
            runnable.m_doneLatch.await();
            if ( null != runnable.m_ex )
            {
                throw new RuntimeException( runnable.m_ex );
            }
        }
    }
    
    
    @Test
    public void testCannotBeUsedAsPartOfTransaction()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final BeansServiceManager transaction =
                dbSupport.getServiceManager().startTransaction();
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                transaction.getService( MutexService.class );
            }
        } );
        transaction.closeTransaction();
    }
    
    
    private final static class LockAcquirer implements Runnable
    {
        private LockAcquirer( 
                final CountDownLatch startLatch,
                final String lockName, 
                final int numberOfAcquisitions, 
                final int sleepInMillisBetweenAcquisitions,
                final MutexServiceImpl service )
        {
            m_startLatch = startLatch;
            m_lockName = lockName;
            m_numberOfAcquisitions = numberOfAcquisitions;
            m_sleepInMillisBetweenAcquisitions = sleepInMillisBetweenAcquisitions;
            m_service = service;
        }
        
        
        public void run()
        {
            try
            {
                m_startLatch.await();
                for ( int i = 0; i < m_numberOfAcquisitions; ++i )
                {
                    runIteration();
                }
            }
            catch ( final Throwable ex )
            {
                Validations.verifyNotNull( "Shut up CodePro.", ex );
                m_ex = ex;
            }
            finally
            {
                m_doneLatch.countDown();
            }
        }
        
        
        private void runIteration()
        {
            final UUID id = m_service.acquireLock( m_lockName );
            TestUtil.sleep( m_sleepInMillisBetweenAcquisitions );
            m_service.releaseLock( id );
        }
        
        
        private final String m_lockName;
        private final int m_numberOfAcquisitions;
        private final int m_sleepInMillisBetweenAcquisitions;
        private final MutexServiceImpl m_service;
        private final CountDownLatch m_startLatch;
        private final CountDownLatch m_doneLatch = new CountDownLatch( 1 );
        private volatile Throwable m_ex;
    } // end inner class def
}
