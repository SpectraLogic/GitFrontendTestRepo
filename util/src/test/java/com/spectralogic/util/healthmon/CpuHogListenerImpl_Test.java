/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Timer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class CpuHogListenerImpl_Test 
{
    @Test
    public void testRealCpuHogOccurredHandledGracefully()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final CpuHogListener listener = InterfaceProxyFactory.getProxy( CpuHogListener.class, btih );
        final CpuHogListener listener2 = new CpuHogListenerImpl();
        
        final CpuHogDetector detector = new CpuHogDetector( 100, 50 );
        detector.addCpuHogListener( InterfaceProxyFactory.getProxy(
                CpuHogListener.class, 
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args ) 
                            throws Throwable
                    {
                        method.invoke( listener2, args );
                        return method.invoke( listener, args ); // only call btih if real ih succeeded
                    }
                } ) );
        TestUtil.sleep( 50 );
        
        final WorkPool wp = WorkPoolFactory.createWorkPool( 2, getClass().getSimpleName() );
        try
        {
            final MockRunnable r = new MockRunnable( true );
            wp.submit( r );
            
            int i = 10;
            while ( --i > 0 && !isReportedAsCpuHog( btih, r ) )
            {
                TestUtil.sleep( 100 );
            }
            if ( 0 >= i )
            {
                fail( "Shoulda been a cpu hog." );
            }
        }
        finally
        {
            wp.shutdownNow();
            shutdownTimer( detector );
        }
    }
    
    
    @Test
    public void testCpuHogNotHoggedEnoughResultsInNoNotificationToCpuHogListeners()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final CpuHogListener listener = InterfaceProxyFactory.getProxy( CpuHogListener.class, btih );
        
        final CpuHogDetector detector = new CpuHogDetector( 100, 200 );
        detector.addCpuHogListener( listener );
        final WorkPool wp = WorkPoolFactory.createWorkPool( 2, getClass().getSimpleName() );
        try
        {
            final MockRunnable r = new MockRunnable( true );
            wp.submit( r );
            
            int i = 5;
            while ( --i > 0 && isReportedAsCpuHog( btih, r ) )
            {
                TestUtil.sleep( 100 );
            }
            if ( 0 >= i )
            {
                fail( "Should notta been a cpu hog." );
            }
        }
        finally
        {
            wp.shutdownNow();
            shutdownTimer( detector );
        }
    }
    
    
    @Test
    public void testNoCpuHoggingResultsInNoCpuHog()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final CpuHogListener listener = InterfaceProxyFactory.getProxy( CpuHogListener.class, btih );
        
        final CpuHogDetector detector = new CpuHogDetector( 100, 200 );
        detector.addCpuHogListener( listener );
        TestUtil.sleep( 20 );
        try
        {
            final MockRunnable r = new MockRunnable( true );
            
            int i = 5;
            while ( --i > 0 && isReportedAsCpuHog( btih, r ) )
            {
                TestUtil.sleep( 100 );
            }
            if ( 0 >= i )
            {
                fail( "Should notta reported CPU hog." );
            }
        }
        finally
        {
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
                long value = Long.MIN_VALUE + 1;
                while ( 0 > value )
                {
                    ++value;
                    if ( Thread.currentThread().isInterrupted() )
                    {
                        throw new RuntimeException( "Interrupted.");
                    }
                }
            }
        }
        
        private volatile long m_threadId;
        private final boolean m_cpuHog;
    } // end inner class def
}
