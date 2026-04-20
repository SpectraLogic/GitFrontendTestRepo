/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.ThrottledRunnable.ThrottledRunnableAggregator;
import com.spectralogic.util.thread.ThrottledRunnableExecutor.WhenAggregating;




public final class ThrottledRunnableExecutor_Test
{
    @Test
    public void testConstructorNegativeDelayNotAllowed()
    {
        try
        {
            new ThrottledRunnableExecutor<>(
                    -1,
                    null );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }
    }
    

    @Test
    public void testConstructorZeroDelayNotAllowed()
    {
        try
        {
            new ThrottledRunnableExecutor<>(
                    0,
                    null );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }
    }
    

    @Test
    public void testConstructorPositiveDelaysAllowed()
    {
        new ThrottledRunnableExecutor<>(
                1,
                null );
        new ThrottledRunnableExecutor<>(
                100,
                null );
        new ThrottledRunnableExecutor<>(
                100000,
                null );
    }
    
    
    @Test
    public void testIsIdleReturnsTrueIffNothingScheduledAndNothingRunning()
    {
        final ThrottledRunnableExecutor< MyThrottledRunnable > executor =
            new ThrottledRunnableExecutor<>( 10, null );
        final MyThrottledRunnable r = new MyThrottledRunnable();
        
        executor.add( r );
        
        final Duration duration = new Duration();
        for ( int i = 0; i <= 10000; ++i )
        {
            assertFalse(
                    executor.isIdle(),
                    "Should notta reported idle, since running or scheduled.");
            if ( 10000 == i )
            {
                if ( duration.getElapsedMillis() > 10000 )
                {
                    fail( "Timed out." ); 
                }
                if ( r.m_completionNotifiers.isEmpty() )
                {
                    i = 0;
                }
            }
        }
        
        r.m_completionNotifiers.get( 0 ).completed();
        assertTrue(
                executor.isIdle(),
                "Nothing running or scheduled, so shoulda been idle.");
    }
    

    @Test
    public void testIsDelayedRunnableScheduledToRunInTheFutureReturnsTrueIffSuchRunnableExists()
    {
        final ThrottledRunnableExecutor< MyThrottledRunnable > executor =
            new ThrottledRunnableExecutor<>( 10, null );
        final MyThrottledRunnable r1 = new MyThrottledRunnable();
        final MyThrottledRunnable r2 = new MyThrottledRunnable();
        
        assertFalse(
                executor.isDelayedRunnableScheduledToRunInTheFuture(),
                "Should notta been anything scheduled yet.");
        
        executor.add( r1 );
        r1.assertCompletionNotifiersCount( 1 );
        assertFalse(
                executor.isDelayedRunnableScheduledToRunInTheFuture(),
                "Should notta been anything scheduled anymore.");
        
        executor.add( r2 );
        assertTrue(
                executor.isDelayedRunnableScheduledToRunInTheFuture(),
                "Shoulda been scheduled runnable.");
        
        r1.m_completionNotifiers.get( 0 ).completed();
        r2.assertCompletionNotifiersCount( 1 );
        assertFalse(
                executor.isDelayedRunnableScheduledToRunInTheFuture(),
                "Should notta been anything scheduled anymore.");
        
        r2.m_completionNotifiers.get( 0 ).completed();
        assertFalse(
                executor.isDelayedRunnableScheduledToRunInTheFuture(),
                "Should notta been anything scheduled anymore.");
    }
    
    
    @Test
    public void testAddNullNotAllowed()
    {
        final ThrottledRunnableExecutor< ThrottledRunnable > executor =
                new ThrottledRunnableExecutor<>(
                        850,
                        null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                executor.add( null );
            }
        } );
    }
    
    
    @Test
    public void testAddOnceResultsInEventualExecution()
    {
        final ThrottledRunnableExecutor< ThrottledRunnable > executor =
            new ThrottledRunnableExecutor<>(
                    850,
                    null );
        assertTrue(executor.isIdle(), "Executor is idle.");
        
        final MyThrottledRunnable runnable = new MyThrottledRunnable();
        executor.add( runnable );
        assertFalse(
                executor.isExecutingRunnable(),
                "Shoulda reported not running runnable initially.");
        final Duration duration = new Duration();
        assertFalse(executor.isIdle(), "Executor not idle.");
        runnable.assertCompletionNotifiersCount( 1 );
        assertTrue(
                executor.isExecutingRunnable(),
                "Shoulda been running runnable until we say we're completed.");

        assertTimeElapsed( 850, duration.getElapsedMillis() );
        assertFalse(executor.isIdle(), "Executor not idle.");
        
        runnable.m_completionNotifiers.get( 0 ).completed();
        assertFalse(
                executor.isExecutingRunnable(),
                "Shoulda reported not running runnable once completed.");
        assertTrue(executor.isIdle(), "Executor idle.");
    }
    
    
    @Test
    public void testAddMultipleTimesWithDefaultAggregationResultsInEventualExecution()
    {
        final ThrottledRunnableExecutor< ThrottledRunnable > executor =
                new ThrottledRunnableExecutor<>(
                        50,
                        null );
                
        final MyThrottledRunnable runnable = new MyThrottledRunnable();
        executor.add( runnable );
        runnable.assertCompletionNotifiersCount( 1 );
        executor.add( runnable );
        executor.add( runnable );
        
        runnable.m_completionNotifiers.get( 0 ).completed();
        assertFalse(executor.isIdle(), "Executor not idle.");

        runnable.assertCompletionNotifiersCount( 2 );
        runnable.m_completionNotifiers.get( 1 ).completed();
        
        TestUtil.sleep( 75 );
        assertEquals(
                2,
                runnable.m_completionNotifiers.size(),
                "Shoulda executed 2 runnables, aggregating the second and third calls together."
                );
    }
    
    
    @Test
    public void testAddMultipleTimesWithCustomAggregationResultsInEventualExecution()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final ThrottledRunnableAggregator< MyThrottledRunnable > aggregator =
                InterfaceProxyFactory.getProxy( ThrottledRunnableAggregator.class, btih );
        final ThrottledRunnableExecutor< MyThrottledRunnable > executor =
                new ThrottledRunnableExecutor<>(
                        50,
                        aggregator );
                
        final MyThrottledRunnable runnable1 = new MyThrottledRunnable();
        final MyThrottledRunnable runnable2 = new MyThrottledRunnable();
        final MyThrottledRunnable runnable3 = new MyThrottledRunnable();
        executor.add( runnable1 );
        runnable1.assertCompletionNotifiersCount( 1 );
        executor.add( runnable2 );
        executor.add( runnable3 );
        
        runnable1.m_completionNotifiers.get( 0 ).completed();
        assertFalse(executor.isIdle(), "Executor not idle.");

        runnable2.assertCompletionNotifiersCount( 1 );
        runnable2.m_completionNotifiers.get( 0 ).completed();
        
        TestUtil.sleep( 75 );
        assertEquals(
                0,
                runnable3.m_completionNotifiers.size(),
                "Shoulda executed 2 runnables, aggregating the second and third calls together."
               );
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda made single call to aggregator."
                );
        assertEquals(
                runnable2,
                btih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ),
                "Shoulda made single call to aggregator."
                 );
        assertEquals(
                runnable3,
                btih.getMethodInvokeData().get( 0 ).getArgs().get( 1 ),
                "Shoulda made single call to aggregator."
               );
    }
    
    
    @Test
    public void testCompletedNotificationSentMultipleTimesNotAllowed()
    {
        final ThrottledRunnableExecutor< ThrottledRunnable > executor =
                new ThrottledRunnableExecutor<>(
                        10,
                        null );
        
        final MyThrottledRunnable runnable = new MyThrottledRunnable();
        executor.add( runnable ); 
        runnable.assertCompletionNotifiersCount( 1 );
        runnable.m_completionNotifiers.get( 0 ).completed();
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                runnable.m_completionNotifiers.get( 0 ).completed();
            }
        } );
    }
    
    
    @Test
    public void testAggregationExecutionModeOnTimeDoesNotDelayUntilQuietPeriodElapses()
    {
        final ThrottledRunnableExecutor< ThrottledRunnable > executor =
            new ThrottledRunnableExecutor<>(
                    100,
                    null,
                    WhenAggregating.EXECUTE_ON_TIME );
        
        final MyThrottledRunnable runnable = new MyThrottledRunnable();
        executor.add( runnable );
        
        for ( int i = 0; i < 15; ++i )
        {
            TestUtil.sleep( 10 );
            executor.add( runnable );
        }
        
        assertEquals(
                1,
                runnable.m_completionNotifiers.size(),
                "Shoulda executed once already."
                );
    }
    
    
    @Test
    public void testAggregationExecutionModeDelayDelaysUntilQuietPeriodElapses()
    {
        final ThrottledRunnableExecutor< ThrottledRunnable > executor =
            new ThrottledRunnableExecutor<>(
                    100,
                    null,
                    WhenAggregating.DELAY_EXECUTION );
        
        final MyThrottledRunnable runnable = new MyThrottledRunnable();
        executor.add( runnable );
        
        for ( int i = 0; i < 15; ++i )
        {
            TestUtil.sleep( 10 );
            executor.add( runnable );
        }
        
        assertEquals(
                0,
                runnable.m_completionNotifiers.size(),
                "Should notta executed yet."
                 );
        runnable.assertCompletionNotifiersCount( 1 );
    }
    
    
    private void assertTimeElapsed( final long expected, final long actual )
    {
        if ( expected - 400 > actual
                || expected * 3 < actual )
        {
            fail( new StringBuilder( 100 )
                .append( "Expected duration to be ~" ) 
                .append( expected )
                .append( "ms, but was " ) 
                .append( actual )
                .append( '.' )
                .toString() );
        }
    }
    
    
    private final static class MyThrottledRunnable implements ThrottledRunnable
    {
        @Override
        public void run( final RunnableCompletionNotifier completionNotifier )
        {
            m_completionNotifiers.add( completionNotifier );
        }
        
        private void assertCompletionNotifiersCount( final int expectedCount )
        {
            int i = 1000;
            while ( --i > 0 && expectedCount != m_completionNotifiers.size() )
            {
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
            
            assertEquals(
                    expectedCount,
                    m_completionNotifiers.size(),
                    "Run calls not as expected."
                     );
        }
        
        private final List< RunnableCompletionNotifier > m_completionNotifiers = new CopyOnWriteArrayList<>();
    }
}
