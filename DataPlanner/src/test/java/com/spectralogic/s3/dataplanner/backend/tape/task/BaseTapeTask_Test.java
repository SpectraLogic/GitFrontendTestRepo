/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;


import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class BaseTapeTask_Test 
{

    @Test
    public void testSimpleConstructorHappyConstruction()
    {
        new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ] );
    }
    
    
    @Test
    public void testConstructorNullTapeIdAllowed()
    {
        new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ], null );
    }

    
    @Test
    public void testPrepareForStartWhenNotInReadyStateNotAllowed()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, UUID.randomUUID() );

        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                new MockTapeAvailability() );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                task.prepareForExecutionIfPossible(
                        InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                        new MockTapeAvailability() );
            }
        } );
    }
    
    
    @Test
    public void testRunBeforePrepareToStartNotAllowed()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, UUID.randomUUID() );
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
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, UUID.randomUUID() );
        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                new MockTapeAvailability() );

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
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, UUID.randomUUID() );
        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                new MockTapeAvailability() );

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
    public void testGetStateReturnsRealStateUnlessTooManyRetriesInWhichCaseNotReadyShouldBeReturned()
            throws IllegalArgumentException, SecurityException
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.HIGH, UUID.randomUUID() );
        task.m_runReturnValue = BlobStoreTaskState.READY;
        for ( int i = 0; i < 3; ++i )
        {
            assertEquals(BlobStoreTaskState.READY, task.getState(), "Task shoulda reported real state.");
            task.prepareForExecutionIfPossible(
                    InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                    new MockTapeAvailability() );
            
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
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ], UUID.randomUUID() );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported correct task state.");
        task.m_runReturnValue = BlobStoreTaskState.READY;
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported correct task state.");
        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                new MockTapeAvailability() );
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda reported correct task state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported correct task state.");

        task.m_runReturnValue = BlobStoreTaskState.COMPLETED;
        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ), 
                new MockTapeAvailability() );
            
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported correct task state.");
    }
    
    
    @Test
    public void testGetCacheManagerAndGetServiceManagerAndGetTapeDriveResourceReturnValuesSet()
    {
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ], UUID.randomUUID() );
        task.m_runReturnValue = BlobStoreTaskState.READY;
        final TapeDriveResource resource1 = InterfaceProxyFactory.getProxy( TapeDriveResource.class, null );
        task.prepareForExecutionIfPossible( resource1, new MockTapeAvailability() );
        assertNotNull(task.getServiceManager(), "Shoulda returned service manager.");
        assertEquals(resource1, task.getDriveResource(), "Shoulda returned current resource.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNull(task.getDriveResource(), "Resource shoulda been cleared upon completion.");

        task.m_runReturnValue = BlobStoreTaskState.COMPLETED;
        final TapeDriveResource resource2 = InterfaceProxyFactory.getProxy( TapeDriveResource.class, null );
        task.prepareForExecutionIfPossible( resource2, new MockTapeAvailability() );
        assertEquals(resource2, task.getDriveResource(), "Shoulda returned current resource.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(task.getServiceManager(), "Shoulda returned service manager.");
        assertNull(task.getDriveResource(), "Resource shoulda been cleared upon completion.");
    }
    
    
    @Test
    public void testTaskSchedulingListenersNotifiedWhenTaskCompletes()
    {
        final Method methodTaskSchedulingRequired = 
                ReflectUtil.getMethod( BlobStoreTaskSchedulingListener.class, "taskSchedulingRequired" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final BlobStoreTaskSchedulingListener listener = 
                InterfaceProxyFactory.getProxy( BlobStoreTaskSchedulingListener.class, btih );
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ], UUID.randomUUID() );
        task.addSchedulingListener( listener );
        
        task.m_runReturnValue = BlobStoreTaskState.READY;
        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                new MockTapeAvailability() );

        assertEquals(0,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Should notta sent any events yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(1,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Task completed needing retry shoulda resulted in listener notification.");

        task.addSchedulingListener( listener );
        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ), 
                new MockTapeAvailability() );

        assertEquals(1,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Should notta sent any events yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(2,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Shoulda been single listener notification, even though listener was added twice.");

        task.m_runReturnValue = BlobStoreTaskState.COMPLETED;
        task.prepareForExecutionIfPossible(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                new MockTapeAvailability() );

        assertEquals(2, btih.getMethodCallCount(methodTaskSchedulingRequired), "Should notta sent any events yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(3,  btih.getMethodCallCount(methodTaskSchedulingRequired), "Task completed successfully shoulda resulted in listener notification.");
    }


    @Test
    public void testTapeIdManagedCorrectlyWhenTapeTemporarilyUnavailable()
    {
        
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final UUID tapeId = UUID.randomUUID();
        final NoOpTapeTask task = new NoOpTapeTask( "task", BlobStoreTaskPriority.values()[ 0 ], tapeId, tapeFailureManagement, dbSupport.getServiceManager());

        final Throwable ex = TestUtil.assertThrows(
                "Should notta successfully prepared for execution using unavailable tape id.",
                RuntimeException.class,
                () -> task.prepareForExecutionIfPossible(
                        InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                        new MockTapeAvailability().addUnavailableTape( tapeId )
                                .setVerifyAvailableReturnsError( true ) ));
        assertFalse(ex instanceof BlobStoreTaskNoLongerValidException, "Task should not be considered invalid due to temporarily unavailable tape.");
        final Object expected = task.getState();
        assertEquals(expected, BlobStoreTaskState.READY, "Task should still be instate ready");
    }


    @Test
    public void testTapeIdManagedCorrectlyWhenTapePermanentlyUnavailable()
    {
        final UUID initialTapeId = UUID.randomUUID();
        final ConcreteTask task = new ConcreteTask( BlobStoreTaskPriority.values()[ 0 ], initialTapeId );

        task.m_runReturnValue = BlobStoreTaskState.READY;
        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                task.prepareForExecutionIfPossible(
                        InterfaceProxyFactory.getProxy( TapeDriveResource.class, null ),
                        new MockTapeAvailability().addUnavailableTape( initialTapeId )
                        .setVerifyAvailableException( new RuntimeException( "oops" ) ) );
            }
        } );
        final Object expected = task.getState();
        assertEquals(expected, BlobStoreTaskState.COMPLETED, "Task should be considered completed now since it is invalid.");
    }
    
    
    private final static class ConcreteTask extends BaseTapeTask
    {
        private ConcreteTask( final BlobStoreTaskPriority priority )
        {
            super( priority, null, new TapeFailureManagement(new MockBeansServiceManager()), new MockBeansServiceManager() );
        }
        
        private ConcreteTask( final BlobStoreTaskPriority priority, final UUID tapeId )
        {
            super( priority, tapeId, new TapeFailureManagement(new MockBeansServiceManager()), new MockBeansServiceManager() );
        }

        @Override
        protected BlobStoreTaskState runInternal()
        {
            m_runCallCount.incrementAndGet();
            return m_runReturnValue;
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

        @Override
        public boolean canUseTapeAlreadyInDrive(TapeAvailability tapeAvailability) {
            return true;
        }

        @Override
        public boolean canUseAvailableTape(TapeAvailability tapeAvailability) { return true; }

        private volatile boolean m_overrideSelectTapeIdDynamically;
        private volatile UUID m_tapeIdToSelect;
        private volatile BlobStoreTaskState m_runReturnValue = BlobStoreTaskState.COMPLETED;
        private final AtomicInteger m_runCallCount = new AtomicInteger( 0 );
    } // end inner class def

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
