/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Timer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class CpuHogDetector_Test 
{
    @Test
    public void testNoCpuHogResultsInNoListenerNotification()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final CpuHogListener listener = InterfaceProxyFactory.getProxy( CpuHogListener.class, btih );
        
        final CpuHogDetector detector = new CpuHogDetector( 250, 50 );
        detector.addCpuHogListener( listener );
        
        final WorkPool wp = WorkPoolFactory.createWorkPool( 2, getClass().getSimpleName() );
        try
        {
            final MockRunnable r = new MockRunnable( false );
            wp.submit( r );
            sleep( 1500 );
            
            assertFalse(
                    isReportedAsCpuHog( btih, r ),
                    "Should notta been a cpu hog."
                   );
        }
        finally
        {
            wp.shutdownNow();
            shutdownTimer( detector );
        }
    }
    

    @Test
    public void testCpuHogResultsInListenerNotification()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final CpuHogListener listener = InterfaceProxyFactory.getProxy( CpuHogListener.class, btih );
        
        final CpuHogDetector detector = new CpuHogDetector( 250, 50 );
        detector.addCpuHogListener( listener );
        final WorkPool wp = WorkPoolFactory.createWorkPool( 2, getClass().getSimpleName() );
        final MockRunnable r = new MockRunnable( true );
        try
        {
            wp.submit( r );
            
            int i = 100;
            while ( --i > 0 && ! isReportedAsCpuHog( btih, r ) )
            {
                sleep( 100 );
            }
            if ( 0 >= i )
            {
                fail( "Shoulda been a cpu hog." );
            }
        }
        finally
        {
            r.m_shutdown = true;
            wp.shutdownNow();
            shutdownTimer( detector );
        }
    }
    
    
    private void shutdownTimer( final CpuHogDetector detector )
    {
        try
        {
            final Field f = CpuHogDetector.class.getDeclaredField( "m_timer" ); 
            f.setAccessible( true );
            final Timer timer = (Timer)f.get( detector );
            timer.cancel();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }

    
    private boolean isReportedAsCpuHog(
            final BasicTestsInvocationHandler ih,
            final MockRunnable r )
    {
        try
        {
            final Method m = CpuHogListener.class.getMethod( "cpuHogOccurred", Map.class ); 
            for ( final MethodInvokeData data : ih.getMethodInvokeData( m ) )
            {
                @SuppressWarnings("unchecked")
                final Map< ThreadInfo, Integer > hogs = (Map< ThreadInfo, Integer >)data.getArgs().get( 0 );
                for ( final ThreadInfo thread : hogs.keySet() )
                {
                    if ( thread.getThreadId() == r.m_threadId )
                    {
                        return true;
                    }
                }
            }
            
            return false;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private static void sleep( final int millis )
    {
        try
        {
            Thread.sleep( millis );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private final static class MockRunnable implements Runnable
    {
        private MockRunnable( final boolean isCpuHog )
        {
            m_cpuHog = isCpuHog;
        }
        
        public void run()
        {
            m_threadId = Thread.currentThread().getId();
            if ( m_cpuHog )
            {
                double value = Long.MIN_VALUE;
                while ( 0 > value )
                {
                    value += 1.9;
                    if ( m_shutdown )
                    {
                        return;
                    }
                }
            }
        }
        
        private volatile long m_threadId;
        private volatile boolean m_shutdown;
        private final boolean m_cpuHog;
    } // end inner class def
}
