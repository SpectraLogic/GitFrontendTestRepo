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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;




import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class DeadlockDetector_Test 
{
    @Test
    public void testInOrder()
    {
        // TODO Verify the test below works on Mac and FreeBSD platform
        if ( System.getProperty( "os.name" ).contains( ( "Windows" ) ) ||
             System.getProperty( "os.name" ).contains( ( "Linux" ) ) )
        {
            internalTestNoDeadlockResultsInNoNotificationToListener();
        }
        internalTestDeadlockResultsInNotificationToAllListeners();
    }
    
    
    private void internalTestNoDeadlockResultsInNoNotificationToListener() 
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final DeadlockListener listener =
            InterfaceProxyFactory.getProxy( DeadlockListener.class, btih );
        
        final DeadlockDetector detector = new DeadlockDetector( 150 );
        detector.addDeadlockListener( listener );
        sleep( 120 );
        
        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put(
                getDeadlockOccurredMethod(), 
                Integer.valueOf( 0 ) );
        
        try
        {
            btih.verifyMethodInvocations( expectedCalls );
        }
        catch ( final RuntimeException ex )
        {
            final List< MethodInvokeData > invokeData = 
                    btih.getMethodInvokeData( getDeadlockOccurredMethod() );
            if ( ! invokeData.isEmpty() )
            {
                final DeadlockListenerImpl ldl = new DeadlockListenerImpl();
                @SuppressWarnings( "unchecked" )
                final Set< ThreadInfo > threadInfo = 
                    (Set< ThreadInfo >)invokeData.get( 0 ).getArgs().get( 0 );
                final String msg = ldl.getLogStatement( threadInfo );
                throw new RuntimeException( msg, ex );
            }
            
            throw ex;
        }
        shutdownTimer( detector );
    }
    
    
    private void internalTestDeadlockResultsInNotificationToAllListeners()
    {
        final BasicTestsInvocationHandler btih1 = getThreadInfoSetClearingIh();
        final DeadlockListener listener1 =
            InterfaceProxyFactory.getProxy( DeadlockListener.class, btih1 );
        
        final BasicTestsInvocationHandler btih2 = new BasicTestsInvocationHandler( null );
        final DeadlockListener listener2 =
            InterfaceProxyFactory.getProxy( DeadlockListener.class, btih2 );
        
        final DeadlockDetector detector = new DeadlockDetector( 100 );
        detector.addDeadlockListener( listener1 );
        detector.addDeadlockListener( listener2 );
        
        final Method m = getDeadlockOccurredMethod();
        final WorkPool wp = performDeadlock();
        try
        {
            int i = 1000;
            while ( --i > 0 &&
                    ( 0 == btih1.getMethodInvokeData( m ).size() 
                      || 0 == btih2.getMethodInvokeData( m ).size() ) )
            {
                sleep( 10 );
            }
            assertTrue(
                    0 < btih1.getMethodInvokeData( m ).size() ,
                    "Shoulda been a deadlock."
                    );
            assertTrue(
                    0 < btih2.getMethodInvokeData( m ).size(),
                    "Shoulda been a deadlock."
                     );
            
            final MethodInvokeData invokeData = 
                btih2.getMethodInvokeData( m ).get( 0 );
            @SuppressWarnings("unchecked")
            final Set< ThreadInfo > deadlockedThreads = 
                (Set< ThreadInfo >)invokeData.getArgs().get( 0 );
            assertTrue(
                    2 <= deadlockedThreads.size(),
                    "Shoulda reported at least our 2 deadlocked threads."
                    );
        }
        finally
        {
            wp.shutdownNow();
            shutdownTimer( detector );
        }
    }
    
    
    private void shutdownTimer( final DeadlockDetector detector )
    {
        try
        {
            final Field f = DeadlockDetector.class.getDeclaredField( "m_timer" ); 
            f.setAccessible( true );
            final Timer timer = (Timer)f.get( detector );
            timer.cancel();
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
    
    
    private BasicTestsInvocationHandler getThreadInfoSetClearingIh()
    {
        return new BasicTestsInvocationHandler( new InvocationHandler()
        {
            public Object invoke( Object proxy, Method method, Object[] args )
                    throws Throwable
            {
                ( (Set<?>)args[ 0 ] ).clear();
                return null;
            }
        } );
    }
    
    
    private WorkPool performDeadlock()
    {
        final WorkPool wp = WorkPoolFactory.createWorkPool( 2, getClass().getSimpleName() );
        
        final DeadlockingRunnable r1 = new DeadlockingRunnable();
        final DeadlockingRunnable r2 = new DeadlockingRunnable();
        r1.m_otherRunnable = r2;
        r2.m_otherRunnable = r1;
        wp.submit( r1 );
        wp.submit( r2 );
        
        return wp;
    }
    
    
    private final static class DeadlockingRunnable implements Runnable
    {
        public void run()
        {
            synchronized ( this )
            {
                m_tookLock = true;
                while ( ! m_otherRunnable.m_tookLock )
                {
                    sleep( 10 );
                }
                synchronized ( m_otherRunnable )
                {
                    throw new RuntimeException( "Took lock successfully, but shoulda deadlocked." ); 
                }
            }
        }
        
        private volatile DeadlockingRunnable m_otherRunnable;
        private volatile boolean m_tookLock;
    } // end inner class def
    
    
    private Method getDeadlockOccurredMethod()
    {
        return ReflectUtil.getMethod( 
                DeadlockListener.class, 
                "deadlockOccurred" ); 
    }
}
