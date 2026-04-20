/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Future;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ThreadedInitializable_Test
{
    @Test
    public void testWaitUntilInitializedProbablyWaitsUntilInitialized() throws InterruptedException
    {
        final ThreadedInitializableTester threadedInitializableTester = new ThreadedInitializableTester();
        
        final Future< ? > future = SystemWorkPool.getInstance().submit( new Runnable()
        {
            public void run()
            {
                threadedInitializableTester.methodThatWaitsUntilInitialized();
            }
        } );
        
        Thread.sleep( 10 );
        assertFalse(
                future.isDone(),
                "Shoulda not returned yet because initialization should have taken longer.");

        int i = 100;
        while ( --i > 0 && !future.isDone() )
        {
            TestUtil.sleep( 10 );
        }
        
        assertTrue(
                future.isDone(),
                "Shoulda unblocked eventually since initialization should have finished.");

        assertTrue(
                threadedInitializableTester.isInitialized(),
                "Shoulda been initialized since we tried to wait for initialization.");
    }
    
    
    @Test
    public void testWaitUntilInitializedReturnsImmediatelyWhenAlreadyInitialized()
    {
        final ThreadedInitializableTester threadedInitializableTester = new ThreadedInitializableTester();
        final InitializeTwiceWaiter initializeTwiceWaiter =
                new InitializeTwiceWaiter( threadedInitializableTester );
        final Future< ? > future = SystemWorkPool.getInstance().submit( initializeTwiceWaiter );

        int i = 100;
        while ( --i > 0 && !future.isDone() )
        {
            TestUtil.sleep( 10 );
        }

        assertTrue(future.isDone(), "Shoulda finished initializing eventually.");
        
        assertTrue(
                initializeTwiceWaiter.getTimeSpentInSecondInitializeWait() < 10,
                "Shoulda returned immediately since we're supposedly already initialized.");
    }
    
    
    private final class InitializeTwiceWaiter implements Runnable
    {
        private InitializeTwiceWaiter( final ThreadedInitializableTester threadedInitializableTester )
        {
            m_threadedInitializableTester = threadedInitializableTester;
        }


        public void run()
        {
            m_threadedInitializableTester.methodThatWaitsUntilInitialized();
            
            m_startTime = getTime();
            m_threadedInitializableTester.methodThatWaitsUntilInitialized();
            m_endTime = getTime();
        }
        
        
        public long getTimeSpentInSecondInitializeWait()
        {
            return m_endTime - m_startTime;
        }


        private long getTime()
        {
            return Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) ).getTimeInMillis();
        }
        

        private long m_startTime;
        private long m_endTime;
        private final ThreadedInitializableTester m_threadedInitializableTester;
    }// end inner class
    
    
    @Test
    public void testStartInitializationTwiceNotAllowed()
    {
        final ThreadedInitializableTester threadedInitializableTester = new ThreadedInitializableTester();
        TestUtil.assertThrows(
                "Shoulda thrown a duplicate initialization exception.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        threadedInitializableTester.startInitializationAgain();
                    }
                } );
    }
    
    
    @Test
    public void testInitializerFailurePropagatesException()
    {
        final ThreadedInitializableTester threadedInitializableTester =
                new ThreadedInitializableTester( true );
        TestUtil.assertThrows(
                "Shoulda thrown an initialization failed exception.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        threadedInitializableTester.methodThatWaitsUntilInitialized();
                    }
                } );
    }


    private static class ThreadedInitializableTester extends ThreadedInitializable
    {
        ThreadedInitializableTester()
        {
            this( false );
        }
        
        
        ThreadedInitializableTester( final boolean shouldFail )
        {
            m_shouldFail = shouldFail;
            this.startInitialization();
        }
        
        
        public void startInitializationAgain()
        {
            this.startInitialization();
        }
        
        
        public void methodThatWaitsUntilInitialized()
        {
            this.waitUntilInitialized();
        }
        
        
        @Override
        protected Set< Runnable > getInitializers()
        {
            final Set< Runnable > initializers = new HashSet<>();
            initializers.add( new Runnable()
            {
                @Override
                public void run()
                {
                    if ( m_shouldFail )
                    {
                        throw new RuntimeException( "The initializer failed intentionally." );
                    }
                    
                    try
                    {
                        Thread.sleep( 50 );
                        m_initialized = true;
                    }
                    catch ( final InterruptedException ex )
                    {
                        throw new RuntimeException( ex );
                    }
                }
            } );
            return initializers;
        }
        
        
        public boolean isInitialized()
        {
            return m_initialized;
        }


        private volatile boolean m_initialized = false;
        private final boolean m_shouldFail;
    }// end inner class
}
