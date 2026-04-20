/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.mock.InterfaceProxyFactory;

public final class WorkPoolImpl_Test 
{
    @Test
    public void testThreadsThatHaveTheirPriorityChangedAreResetToDefaultPriorityBeforeBeingReUsed()
        throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 10, "baseName" ); 
        try
        {
            final int [] completedIterations = new int[ 1 ];
            final Set< Integer > initialPriorities = new HashSet<>();
            final Runnable priorityChangingRunnable = new Runnable()
            {
                public void run()
                {
                    synchronized ( initialPriorities )
                    {
                        initialPriorities.add( Integer.valueOf(
                                        Thread.currentThread().getPriority() ) );
                        Thread.currentThread().setPriority( Thread.MIN_PRIORITY );
                        ++completedIterations[ 0 ];
                    }
                }
            };
    
            for ( int i = 0; i < 30; ++i )
            {
                wpi.submit( priorityChangingRunnable );
            }
    
            int i = 1000;
            while ( --i > 0 && 30 != completedIterations[ 0 ] )
            {
                Thread.sleep( 10 );
            }
    
            if ( 1 != initialPriorities.size() )
            {
                final StringBuilder sb = new StringBuilder( 200 )
                    .append( "Initial thread priority when running a new runnable " ) 
                    .append( "should always be the same - " ) 
                    .append( "even when a runnable changes its thread priority " ) 
                    .append( "and never changes it back.  Found multiple initial " ) 
                    .append( "thread priorities: " ); 
                for ( Integer priority : initialPriorities )
                {
                    sb.append( priority ).append( ", " );
                }
                sb.deleteCharAt( sb.length() - 1 );
                sb.deleteCharAt( sb.length() - 1 );
                fail( sb.toString() );
            }
        }
        finally
        {
            wpi.shutdownNow();
        }
    }


    @Test
    public void testThreadsThatHaveTheirNameChangedAreResetToDefaultNameBeforeBeingReUsed()
        throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 10, "baseName" ); 
        try
        {
            final int [] completedIterations = new int[ 1 ];
            final Set< String > initialValues = new HashSet<>();
            final Runnable priorityChangingRunnable = new Runnable()
            {
                public void run()
                {
                    synchronized ( initialValues )
                    {
                        initialValues.add( Thread.currentThread().getName() );
                        Thread.currentThread().setName( "hello" ); 
                        ++completedIterations[ 0 ];
                    }
                }
            };
    
            for ( int i = 0; i < 30; ++i )
            {
                wpi.submit( priorityChangingRunnable );
            }
    
            int i = 1000;
            while ( --i > 0 && 30 != completedIterations[ 0 ] )
            {
                Thread.sleep( 10 );
            }
    
            for ( String value : initialValues )
            {
                if ( ( "hello".equals( value ) ) )
                {
                    assertNull( "Initial thread name was leftover from a previous runnable.", value ); 
                }
            }
        }
        finally
        {
            wpi.shutdownNow();
        }
    }


    @Test
    public void testThreadsThatHaveTheirDaemonChangedAreResetToDefaultDaemonBeforeBeingReUsed()
        throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 10, "baseName" ); 
        try
        {
            final int [] completedIterations = new int[ 1 ];
            final Set< Boolean > initialValues = new HashSet<>();
            final Runnable priorityChangingRunnable = new Runnable()
            {
                public void run()
                {
                    synchronized ( initialValues )
                    {
                        initialValues.add( Boolean.valueOf( Thread.currentThread().isDaemon() ) );
                        ++completedIterations[ 0 ];
                        Thread.currentThread().setDaemon( false );
                    }
                }
            };
    
            for ( int i = 0; i < 30; ++i )
            {
                wpi.submit( priorityChangingRunnable );
            }
    
            int i = 1000;
            while ( --i > 0 && 30 != completedIterations[ 0 ] )
            {
                Thread.sleep( 10 );
            }
    
            if ( initialValues.contains( Boolean.FALSE ) )
            {
                fail( "Thread not reset to be a daemon thread." ); 
            }
        }
        finally
        {
            wpi.shutdownNow();
        }
    }


    /**
     * @throws InterruptedException
     */
    @Test
    public void testThreadsThatHaveTheirUehChangedAreResetToDefaultUehBeforeBeingReUsed()
        throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 10, "baseName" ); 
        try
        {
            final int [] completedIterations = new int[ 1 ];
            final Set< UncaughtExceptionHandler > initialValues =
                new HashSet<>();
            final Runnable priorityChangingRunnable = new Runnable()
            {
                public void run()
                {
                    synchronized ( initialValues )
                    {
                        initialValues.add( Thread.currentThread().getUncaughtExceptionHandler() );
                        Thread.currentThread().setUncaughtExceptionHandler(
                                InterfaceProxyFactory.getProxy( UncaughtExceptionHandler.class, null ) );
                        ++completedIterations[ 0 ];
                    }
                }
            };
    
            for ( int i = 0; i < 30; ++i )
            {
                wpi.submit( priorityChangingRunnable );
            }
    
            int i = 1000;
            while ( --i > 0 && 30 != completedIterations[ 0 ] )
            {
                Thread.sleep( 10 );
            }
    
            if ( 1 != initialValues.size() )
            {
                fail( "Thread not reset to use default exception handler." ); 
            }
        }
        finally
        {
            wpi.shutdownNow();
        }
    }


    @Test
    public void testSubmitRunnable()
    {
        class StringHolder
        {
            public void setStringData( final String s )
            {
                m_data = s;
            }

            private String m_data = null;
        }
        final StringHolder holder = new StringHolder();
        final String boo = "boo"; 
        final Runnable task = new Runnable()
        {
            public void run()
            {
                holder.setStringData( boo );
            }
        };

        final WorkPoolImpl wpi = new WorkPoolImpl( 4, "baseName" ); 
        try
        {
            final Future< ? > future = wpi.submit( task );
            assertNull( future.get(),
                    "Result should be null: " );
            assertEquals(  boo,
                    holder.m_data,
                    "Result should be 'boo': " );
        }
        catch ( final Exception e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            wpi.shutdownNow();
        }
    }


    /**
     * Test method to ensure that no more than max treads are created
     *
     * @throws InterruptedException
     */
    @Test
    public void testMaxThreadsDoesntCreateMore() throws InterruptedException
    {
        final int maxThreads = 4;
        final CountDownLatch countDownLatch = new CountDownLatch( maxThreads );


        class WaitRunnable implements Runnable
        {
            private boolean m_waitingToStart = true;


            public void run()
            {
                m_waitingToStart = false;
                countDownLatch.countDown();
                
                int i = 1000;
                while ( --i > 0 )
                {
                    try
                    {
                        Thread.sleep( 10 );
                    }
                    catch ( final InterruptedException ex )
                    {
                        throw new RuntimeException( ex );
                    }
                }
            }
        }
        final WorkPoolImpl wpi =
            new WorkPoolImpl( maxThreads, "maxThreadsTest" ); 
        try
        {
            final WaitRunnable wr1 = new WaitRunnable();
            final WaitRunnable wr2 = new WaitRunnable();
            final WaitRunnable wr3 = new WaitRunnable();
            final WaitRunnable wr4 = new WaitRunnable();
            final WaitRunnable wr5 = new WaitRunnable();
            wpi.submit( wr1 );
            wpi.submit( wr2 );
            wpi.submit( wr3 );
            wpi.submit( wr4 );
            wpi.submit( wr5 );
    
            countDownLatch.await( 30, TimeUnit.SECONDS );
    
            List< Runnable > notRun = null;
            try
            {
                assertTrue(!wr1.m_waitingToStart || !wr2.m_waitingToStart || !wr3.m_waitingToStart
                                                                || !wr4.m_waitingToStart || !wr5.m_waitingToStart,
                        "At least one thread should have started.");
    
                assertTrue(wr1.m_waitingToStart || wr2.m_waitingToStart || wr3.m_waitingToStart
                                                                || wr4.m_waitingToStart || wr5.m_waitingToStart,
                        "At least one thread should not have started: ");
            }
            finally
            {
                notRun = wpi.shutdownNow();
            }
            assertEquals( 1,
                    notRun.size(),
                    "One Task should not have run: " );
        }
        finally
        {
            wpi.shutdownNow();
        }
    }


    @Test
    public void testJobsAreQueuedAndRunWithBoundedPool() throws InterruptedException,
                    ExecutionException
    {
        final int jobs = 200;
        final int maxThreads = 2;
        final CountDownLatch latch = new CountDownLatch( jobs );

        final WorkPoolImpl wpi = new WorkPoolImpl( maxThreads, "boundedPoolTest" ); 
        final List< Future< ? > > tasks = new ArrayList<>( jobs );
        final Runnable task = new Runnable()
        {
            public void run()
            {
                latch.countDown();
            }
        };
        for ( int count = 0; count < jobs; ++count )
        {
            tasks.add( wpi.submit( task ) );
        }

        List< Runnable > notRun = null;
        try
        {
            assertTrue(latch.await( 30, TimeUnit.SECONDS ),
                    "All jobs should have run.");


            for ( Future< ? > result : tasks )
            {
                try
                {
                    result.get( 2, TimeUnit.SECONDS );
                }
                catch ( final TimeoutException e )
                {
                    if ( LOG.isDebugEnabled() )
                    {
                        LOG.debug( e );
                    }
                    fail( "Job should have completed." ); 
                }
            }
        }
        finally
        {
            notRun = wpi.shutdownNow();
        }
        assertEquals(
                0,
                notRun.size(),
                "All tasks should have run: " );

    }


    @Test
    public void testShutdownAllowsJobsToComplete() throws InterruptedException
    {
        final int maxThreads = 4;
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final CountDownLatch finishLatch = new CountDownLatch( 1 );

        class WaitRunnable implements Runnable
        {
            public void run()
            {
                startLatch.countDown();

                try
                {
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException e )
                {
                    if (LOG.isDebugEnabled())
                    {
                        LOG.info( "Received unexpected interruption.", e ); 
                    }
                    else
                    {
                        LOG.info( "Received unexpected interruption." ); 
                    }
                }

                finishLatch.countDown();
            }
        }
        final WorkPoolImpl wpi =
            new WorkPoolImpl( maxThreads, "allowTaskToFinish" ); 
        final WaitRunnable wr1 = new WaitRunnable();
        final WaitRunnable wr2 = new WaitRunnable();
        final WaitRunnable wr3 = new WaitRunnable();
        final WaitRunnable wr4 = new WaitRunnable();
        wpi.submit( wr1 );
        wpi.submit( wr2 );
        wpi.submit( wr3 );
        wpi.submit( wr4 );

        startLatch.await( 30, TimeUnit.SECONDS );

        wpi.shutdownNow();

        assertTrue(finishLatch.await( 30, TimeUnit.SECONDS ),
                "All jobs submitted should have completed.");
    }


    @Test
    public void testShutdownRejectsNewJobsWhenPoolEmpty()
    {
        final int maxThreads = 1;
        final WorkPoolImpl wpi = new WorkPoolImpl( maxThreads, "" );
        wpi.shutdown();
        try
        {
            wpi.submit( new Runnable()
            {
                public void run()
                {
                    // do nothing, who cares?
                }
            } );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }
    }


    @Test
    public void testShutdownRejectsNewJobsWhenNothingQueued() throws InterruptedException
    {
        final int maxThreads = 1;
        final CountDownLatch startLatch = new CountDownLatch( maxThreads );
        final CountDownLatch finishLatch = new CountDownLatch( maxThreads );

        class WaitRunnable implements Runnable
        {
            public void run()
            {

                startLatch.countDown();

                finishLatch.countDown();
            }
        }


        WorkPoolImpl wpi = new WorkPoolImpl( maxThreads, "shutdownTest" ); 
        wpi.submit( new WaitRunnable() );
        try
        {
            assertTrue(startLatch.await( 30, TimeUnit.SECONDS ),
                    "One job should have been started.");

        }
        finally
        {
            wpi.shutdown();
        }

        final Runnable task = new WaitRunnable();

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }
        assertTrue(finishLatch.await( 30, TimeUnit.SECONDS ),
                "Job should have finished.");

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }
    }


    @Test
    public void testShutdownRejectsNewJobsWhenJobsAreQueued() throws InterruptedException
    {
        final int maxThreads = 1;
        final Object sentinel = new Object();
        final CountDownLatch startLatch = new CountDownLatch( maxThreads );
        final CountDownLatch finishLatch = new CountDownLatch( 2 );

        class WaitRunnable implements Runnable
        {
            public void run()
            {
                synchronized ( sentinel )
                {
                    startLatch.countDown();
                    try
                    {
                        sentinel.wait();
                    }
                    catch ( final InterruptedException e )
                    {
                        if ( LOG.isDebugEnabled() )
                        {
                            LOG.debug( e );
                        }
                    }
                }
                finishLatch.countDown();
            }
        }


        WorkPoolImpl wpi = new WorkPoolImpl( maxThreads, "ShutdownTest" ); 
        wpi.submit( new WaitRunnable() );
        try
        {
            assertTrue(startLatch.await( 30, TimeUnit.SECONDS ),
                    "One job should have been started.");

            wpi.submit( new Runnable()
            {
                public void run()
                {
                    finishLatch.countDown();
                }
            } );
        }
        finally
        {
            wpi.shutdown();
        }

        final Runnable task = new WaitRunnable();

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }

        synchronized ( sentinel )
        {
            sentinel.notifyAll();
        }
        assertTrue(finishLatch.await( 30, TimeUnit.SECONDS ),
                "Jobs should have finished.");

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }
    }


    @Test
    public void testShutdownNowInterruptsRunningJobs() throws InterruptedException
    {
        final int maxThreads = 1;
        final CountDownLatch startLatch = new CountDownLatch( maxThreads );
        final CountDownLatch finishLatch = new CountDownLatch( maxThreads );

        class WaitRunnable implements Runnable
        {
            private boolean m_interrupted = false;

            public void run()
            {
                startLatch.countDown();
                synchronized ( this )
                {
                    try
                    {
                        wait();
                    }
                    catch ( final InterruptedException e )
                    {
                        m_interrupted = true;
                        if ( LOG.isDebugEnabled() )
                        {
                            LOG.debug( e );
                        }
                    }
                }
                finishLatch.countDown();
            }
        }
        final WorkPoolImpl wpi =
            new WorkPoolImpl( maxThreads, "shutdownTest" ); 
        final WaitRunnable task = new WaitRunnable();
        wpi.submit( task );

        try
        {
            assertTrue(startLatch.await( 30, TimeUnit.SECONDS ),
                    "Job should have been started.");
        }
        finally
        {
            wpi.shutdownNow();
        }
        assertTrue(finishLatch.await( 30, TimeUnit.SECONDS ),
                "Job should have finished.");
        assertTrue(task.m_interrupted, "Job should have been m_interrupted.");
    }


    @Test
    public void testShutdownNowRejectsNewJobsWhenPoolEmpty()
    {
        final int maxThreads = 1;
        final WorkPoolImpl wpi = new WorkPoolImpl( maxThreads, "" );
        wpi.shutdownNow();
        try
        {
            wpi.submit( new Runnable()
            {
                public void run()
                {
                    // do nothing, who cares?
                }
            } );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }
    }


    @Test
    public void testShutdownNowRejectsNewJobsWhenNothingQueued() throws InterruptedException
    {
        final int maxThreads = 1;
        final Object sentinel = new Object();
        final CountDownLatch startLatch = new CountDownLatch( maxThreads );
        final CountDownLatch finishLatch = new CountDownLatch( maxThreads );

        class WaitRunnable implements Runnable
        {
            public void run()
            {
                startLatch.countDown();
                finishLatch.countDown();
            }
        }


        final WorkPoolImpl wpi =
            new WorkPoolImpl( maxThreads, "shutdownTest" ); 
        wpi.submit( new WaitRunnable() );
        try
        {
            assertTrue(startLatch.await( 30, TimeUnit.SECONDS ),
                    "One job should have been started.");

        }
        finally
        {
            wpi.shutdownNow();
            wpi.awaitTermination( 30, TimeUnit.SECONDS );
        }

        final Runnable task = new WaitRunnable();

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }

        synchronized ( sentinel )
        {
            sentinel.notifyAll();
        }
        // I changed this to not finished because it should have been canceled by shutdownNow().
        assertTrue(finishLatch.await( 30, TimeUnit.SECONDS ),
                "Job should have  finished.");

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }
    }


    @Test
    public void testShutdownNowRejectsNewJobsWhenJobsAreQueued() throws InterruptedException
    {
        final int maxThreads = 1;
        final Object sentinel = new Object();
        final CountDownLatch startLatch = new CountDownLatch( maxThreads );
        final CountDownLatch finishLatch = new CountDownLatch( maxThreads );

        class WaitRunnable implements Runnable
        {
            public void run()
            {
                synchronized ( sentinel )
                {
                    startLatch.countDown();
                    try
                    {
                        sentinel.wait();
                    }
                    catch ( final InterruptedException e )
                    {
                        if ( LOG.isDebugEnabled() )
                        {
                            LOG.debug( e );
                        }
                    }
                }
                finishLatch.countDown();
            }
        }


        final WorkPoolImpl wpi =
            new WorkPoolImpl( maxThreads, "ShutdownTest" ); 
        wpi.submit( new WaitRunnable() );
        try
        {
            assertTrue(startLatch.await( 30, TimeUnit.SECONDS ),
                    "One job should have been started.");

            wpi.submit( new Runnable()
            {
                public void run()
                {
                    finishLatch.countDown();
                }
            } );
        }
        finally
        {
            wpi.shutdownNow();
        }

        final Runnable task = new WaitRunnable();

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }

        synchronized ( sentinel )
        {
            sentinel.notifyAll();
        }
        assertTrue(finishLatch.await( 5, TimeUnit.SECONDS ),
                "Jobs should have finished.");

        try
        {
            wpi.submit( task );
            fail( "Submit should have rejected the job with an exception." ); 
        }
        catch ( final RejectedExecutionException ignored )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( ignored );
            }
        }
    }


    @Test
    public void testIsTerminatedWhenPoolIsEmpty() throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 1, "" );
        assertFalse(wpi.isTerminated(), "Pool should not be terminated.");
        wpi.shutdownNow();
        for ( int count = 0; count < 10; ++count )
        {
            if ( wpi.isTerminated() )
            {
                break;
            }
            Thread.sleep( 100 );
        }
        assertTrue(wpi.isTerminated(), "Pool should be terminated.");
    }


    @Test
    public void testIsTerminatedWhenPoolHasNothingQueued() throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 1, "IsTerminatedTest" ); 
        wpi.submit( new Runnable()
        {
            public void run()
            {
                synchronized ( this )
                {
                    try
                    {
                        wait();
                    }
                    catch ( final InterruptedException e )
                    {
                        if ( LOG.isDebugEnabled() )
                        {
                            LOG.debug( e );
                        }
                    }
                }
            }
        } );
        assertFalse(wpi.isTerminated(), "Pool should not be terminated.");
        wpi.shutdownNow();
        for ( int count = 0; count < 10; ++count )
        {
            if ( wpi.isTerminated() )
            {
                break;
            }
            Thread.sleep( 1000 );
        }
        assertTrue(wpi.isTerminated(), "Pool should be terminated.");
    }


    @Test
    public void testIsTerminatedWhenJobsAreQueued() throws InterruptedException
    {
        final int maxThreads = 1;
        final Object sentinel = new Object();
        final CountDownLatch startLatch = new CountDownLatch( maxThreads );
        final WorkPoolImpl wpi = new WorkPoolImpl( 1, "IsTerminatedTest" ); 
        wpi.submit( new Runnable()
        {
            public void run()
            {
                synchronized ( sentinel )
                {
                    startLatch.countDown();
                    try
                    {
                        sentinel.wait();
                    }
                    catch ( final InterruptedException e )
                    {
                        if ( LOG.isDebugEnabled() )
                        {
                            LOG.debug( e );
                        }
                    }
                }
            }
        } );
        try
        {
            assertTrue(startLatch.await( 30, TimeUnit.SECONDS ),
                    "One job should have been started.");

            wpi.submit( new Runnable()
            {
                public void run()
                {
                    // do nothing
                }
            } );
            assertFalse(wpi.isTerminated(), "Pool should not be terminated.");
        }
        finally
        {
            wpi.shutdown();
        }

        assertFalse(wpi.isTerminated(), "Pool should not be terminated.");

        synchronized ( sentinel )
        {
            sentinel.notifyAll();
        }

        assertTrue(wpi.awaitTermination( 60, TimeUnit.SECONDS ),
                "Pool should be terminated");

        assertTrue(wpi.isTerminated(), "Pool should be terminated.");
    }


    @Test
    public void testAwaitTerminationWhenPoolIsEmpty() throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 1, "" );
        wpi.shutdownNow();
        assertTrue(wpi.awaitTermination( 30, TimeUnit.SECONDS ),
                "Pool should be terminated.");
    }


    @Test
    public void testAwaitTerminationWhenPoolHasNothingQueued() throws InterruptedException
    {
        final WorkPoolImpl wpi = new WorkPoolImpl( 1, "AwaitTerminationTest" ); 
        wpi.submit( new Runnable()
        {
            public void run()
            {
                try
                {
                    wait();
                }
                catch ( final InterruptedException e )
                {
                    if ( LOG.isDebugEnabled() )
                    {
                        LOG.debug( e );
                    }
                }
            }
        } );
        wpi.shutdownNow();
        assertTrue(wpi.awaitTermination( 30, TimeUnit.SECONDS ),
                "Pool should be terminated.");
    }


    @Test
    public void testAwaitTerminationWhenJobsAreQueued() throws InterruptedException
    {
        final int maxThreads = 1;
        final Object sentinel = new Object();
        final CountDownLatch startLatch = new CountDownLatch( maxThreads );
        final WorkPoolImpl wpi = new WorkPoolImpl( 1, "AwaitTerminationTest" ); 
        wpi.submit( new Runnable()
        {
            public void run()
            {
                synchronized ( sentinel )
                {
                    startLatch.countDown();
                    try
                    {
                        sentinel.wait();
                    }
                    catch ( final InterruptedException e )
                    {
                        if ( LOG.isDebugEnabled() )
                        {
                            LOG.debug( e );
                        }
                    }
                }
            }
        } );
        try
        {
            assertTrue(startLatch.await( 30, TimeUnit.SECONDS ),
                    "One job should have been started.");

            wpi.submit( new Runnable()
            {
                public void run()
                {
                    // do nothing
                }
            } );
        }
        finally
        {
            wpi.shutdownNow();
        }

        synchronized ( sentinel )
        {
            sentinel.notifyAll();
        }
        assertTrue(wpi.awaitTermination( 30, TimeUnit.SECONDS ),
                "Pool should be terminated.");
    }

    
    @Test
    public void testBasenameIsUsedForThreadName() throws InterruptedException
    {
        final Thread[] thread = new Thread[1];
        final int maxThreads = 1;
        final CountDownLatch latch = new CountDownLatch( maxThreads );
        final WorkPoolImpl wpi = new WorkPoolImpl( 1, "Dory" ); 
        try
        {
            wpi.submit( new Runnable() {
                public void run()
                {
                    thread[0] = Thread.currentThread();
                    latch.countDown();
                }
            });
            assertTrue(latch.await( 30, TimeUnit.SECONDS ),
                    "Task should have run.");
            assertTrue(thread[0].getName().startsWith( "Dory" ),
                    "Thread name should begin with 'Dory'");
        }
        finally
        {
            wpi.shutdownNow();
        }
    }

    /**
     * 1. Check that with unlimited threads they each get a new thread.
     * 2. Check that extra threads (over core pool size) get killed when idle
     * 3. Check that idle threads in core pool are reused.
     */
    @Test
    public void testWorkPoolWithMaxThreadsRunsAllTasksConcurrently()
        throws InterruptedException
    {
        final int jobs = 4;
        final CountDownLatch latch = new CountDownLatch( jobs );
        final int maxThreads = 256;

        class WaitRunnable implements Runnable
        {
            private boolean m_waitingToStart = true;

            public void run()
            {
                m_waitingToStart = false;
                latch.countDown();
                synchronized ( this )
                {
                    try
                    {
                        wait();
                    }
                    catch ( final InterruptedException ignored )
                    {
                        if ( LOG.isDebugEnabled() )
                        {
                            LOG.debug( ignored );
                        }
                    }
                }
            }
        }

        final WorkPool wpi = new WorkPoolImpl(
                        maxThreads, "MaxThreadsTest" ); 
        final WaitRunnable wr1 = new WaitRunnable();
        final WaitRunnable wr2 = new WaitRunnable();
        final WaitRunnable wr3 = new WaitRunnable();
        final WaitRunnable wr4 = new WaitRunnable();
        wpi.submit( wr1 );
        wpi.submit( wr2 );
        wpi.submit( wr3 );
        wpi.submit( wr4 );

        List< Runnable > notRun = null;
        try
        {
            assertTrue(latch.await( 60, TimeUnit.SECONDS ),
                    "All jobs should have been started.");
            assertFalse(wr1.m_waitingToStart || wr2.m_waitingToStart || wr3.m_waitingToStart
                                        || wr4.m_waitingToStart,
                    "No jobs should be waiting to start.");
        }
        finally
        {
            notRun = wpi.shutdownNow();
        }
        assertEquals(  0,
                notRun.size(),
                "All tasks should have run: " );
    }


    @Test
    public void testPerformance() throws InterruptedException
    {
        final WorkPool wp = new WorkPoolImpl(
                        100,
                        "baseName" ); 
        final Runnable r =
            InterfaceProxyFactory.getProxy( Runnable.class, null );
        final Duration duration = new Duration();
        for ( int i = 0; i < 100000; ++i )
        {
            wp.submit( r );
        }

        wp.shutdownNow();
        int i = 1000;
        while ( --i > 0 && ! wp.isTerminated() )
        {
            Thread.sleep( 10 );
        }
        assertTrue(wp.isTerminated(),
                "Shoulda finished processing all runnables submitted.");

        LOG.info( new StringBuilder( 200 )
            .append( WorkPoolImpl.class.getSimpleName() )
            .append( " took " ) 
            .append( duration.getElapsedMillis() )
            .append( "ms to run 100,000 " ) 
            .append( "runnables that immediately returned." ) 
            .toString() );
    }
    
    
    private final static Logger LOG = Logger.getLogger( WorkPoolImpl_Test.class );
}
