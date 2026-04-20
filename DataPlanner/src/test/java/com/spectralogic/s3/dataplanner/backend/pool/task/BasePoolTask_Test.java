/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;



import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public final class BasePoolTask_Test
{
    @Test
    public void testSimpleConstructorHappyConstruction()
    {
        new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ] );
    }
    
    
    @Test
    public void testPrepareForStartWhenNotInReadyStateNotAllowed()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH );

        
        task.m_poolId = null;
        task.prepareForExecutionIfPossible();
        task.prepareForExecutionIfPossible();
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Should notta advanced state to pending execution since no pool selected.");

        task.m_poolId = UUID.randomUUID();
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda advanced state to pending execution since pool selected.");

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
    public void test()
            {
                task.prepareForExecutionIfPossible();
            }
        } );
    }
    
    
    @Test
    public void testRunBeforePrepareToStartNotAllowed()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
    }
    
    
    @Test
    public void testRunReturnsInProgressNotAllowed()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH );
        task.prepareForExecutionIfPossible();

        task.m_runReturnValue = BlobStoreTaskState.IN_PROGRESS;
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
    }
    
    
    @Test
    public void testRunReturnsPendingExecutionNotAllowed()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH );
        task.prepareForExecutionIfPossible();

        task.m_runReturnValue = BlobStoreTaskState.PENDING_EXECUTION;
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
    }
    
    
    @Test
    public void testRunReturnsLegalReturnCodeResultsInLocksNotReleased()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, getLockSupport( btih ) );
        task.prepareForExecutionIfPossible();

        task.m_runReturnValue = BlobStoreTaskState.READY;
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        final Method method = ReflectUtil.getMethod( PoolLockSupport.class, "releaseLock" );
        assertEquals(0,  btih.getMethodCallCount(method), "Should notta released locks upon task run termination.");
    }
    
    
    @Test
    public void testRunThrowsExceptionResultsInLocksNotReleased()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, getLockSupport( btih ) );
        task.prepareForExecutionIfPossible();

        task.m_runReturnValue = null;
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        
        final Method method = ReflectUtil.getMethod( PoolLockSupport.class, "releaseLock" );
        assertEquals(0,  btih.getMethodCallCount(method), "Should notta released locks upon task run termination.");
    }
    
    
    @Test
    public void testGetStateReturnsRealStateUnlessTooManyRetriesInWhichCaseNotReadyShouldBeReturned()
            throws IllegalArgumentException, SecurityException
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH );
        task.m_runReturnValue = BlobStoreTaskState.READY;
        for ( int i = 0; i < 3; ++i )
        {
            assertEquals(BlobStoreTaskState.READY, task.getState(), "Task shoulda reported real state.");
            task.prepareForExecutionIfPossible();
            TestUtil.invokeAndWaitUnchecked( task );
        }

        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Task shoulda reported not ready.");
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Task shoulda reported not ready.");
        TestUtil.sleep( 1 );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Task shoulda reported not ready.");
    }
    
    
    @Test
    public void testGetStateReturnsTaskState()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ] );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported correct task state.");
        task.m_runReturnValue = BlobStoreTaskState.READY;
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported correct task state.");
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda reported correct task state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported correct task state.");

        task.m_runReturnValue = BlobStoreTaskState.COMPLETED;
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported correct task state.");
    }
    
    
    @Test
    public void testGetCacheManagerAndGetServiceManagerAndGetPoolDriveResourceReturnValuesSet()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ] );
        task.m_runReturnValue = BlobStoreTaskState.READY;
        task.prepareForExecutionIfPossible();
        assertNotNull(task.getServiceManager(), "Shoulda returned service manager.");
        assertNotNull(task.getDiskManager(), "Shoulda returned cache manager.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        task.m_runReturnValue = BlobStoreTaskState.COMPLETED;
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(task.getServiceManager(), "Shoulda returned service manager.");
        assertNotNull(task.getDiskManager(), "Shoulda returned cache manager.");
    }
    
    
    @Test
    public void testTaskSchedulingListenersNotifiedWhenTaskCompletes() 
    {
        final Method methodTaskSchedulingRequired = 
                ReflectUtil.getMethod( BlobStoreTaskSchedulingListener.class, "taskSchedulingRequired" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final BlobStoreTaskSchedulingListener listener = 
                InterfaceProxyFactory.getProxy( BlobStoreTaskSchedulingListener.class, btih );
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ] );
        task.addSchedulingListener( listener );
        
        task.m_runReturnValue = BlobStoreTaskState.READY;
        task.prepareForExecutionIfPossible();

        assertEquals(0,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Should notta sent any events yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(1,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Task completed needing retry shoulda resulted in listener notification.");

        task.addSchedulingListener( listener );
        task.prepareForExecutionIfPossible();

        assertEquals(1,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Should notta sent any events yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(2,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Shoulda been single listener notification, even though listener was added twice.");

        task.m_runReturnValue = BlobStoreTaskState.COMPLETED;
        task.prepareForExecutionIfPossible();

        assertEquals(2,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Should notta sent any events yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(3,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Task completed successfully shoulda resulted in listener notification.");
    }
    
    
    @Test
    public void testSelectPoolThrowsExceptionResultsInTaskInvalidation()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, getLockSupport( btih ) );
        task.m_selectPoolEx = new RuntimeException( "oops" );
        task.prepareForExecutionIfPossible();
        
        final Method method = ReflectUtil.getMethod( PoolLockSupport.class, "releaseLock" );
        assertEquals(1,  btih.getMethodCallCount(method), "Shoulda released locks upon task run termination.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as completed since it's no longer valid.");
    }
    
    
    private final static class ConcreteTask extends BasePoolTask
    {
        private ConcreteTask( final BlobStoreTaskPriority priority )
        {
            super( priority,
                    new MockBeansServiceManager(),
                    InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, null ),
                    InterfaceProxyFactory.getProxy( PoolLockSupport.class, null ),
                    InterfaceProxyFactory.getProxy( DiskManager.class, null ),
                    InterfaceProxyFactory.getProxy( JobProgressManager.class, null ) );
        }

        private ConcreteTask( final BlobStoreTaskPriority priority, final PoolLockSupport lockSupport )
        {
            super( priority,
                    new MockBeansServiceManager(),
                    InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, null ),
                    lockSupport,
                    InterfaceProxyFactory.getProxy( DiskManager.class, null ),
                    InterfaceProxyFactory.getProxy( JobProgressManager.class, null ) );
        }

        @Override
        protected BlobStoreTaskState runInternal()
        {
            m_runCallCount.incrementAndGet();
            if ( null == m_runReturnValue )
            {
                throw new RuntimeException( "Oops." );
            }
            return m_runReturnValue;
        }

        @Override
        public UUID selectPool()
        {
            if ( null != m_selectPoolEx )
            {
                throw m_selectPoolEx;
            }
            return m_poolId;
        }
        
        public String getDescription()
        {
            return "mocked";
        }
        
        @Override
        public void performPreRunValidations()
        {
            // empty
        }
        
        @Override
        protected void taskCancelledDueToTooManyRunFailures()
        {
            // empty
        }

        private volatile UUID m_poolId = UUID.randomUUID();
        private volatile RuntimeException m_selectPoolEx;
        private volatile BlobStoreTaskState m_runReturnValue = BlobStoreTaskState.COMPLETED;
        private final AtomicInteger m_runCallCount = new AtomicInteger( 0 );
    } // end inner class def
    
    
    @SuppressWarnings( "unchecked" )
    private PoolLockSupport< PoolTask > getLockSupport( final BasicTestsInvocationHandler btih )
    {
        return InterfaceProxyFactory.getProxy( PoolLockSupport.class, btih );
    }
}
