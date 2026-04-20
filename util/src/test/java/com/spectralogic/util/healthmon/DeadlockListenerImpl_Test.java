/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.reflect.Field;
import java.util.Timer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;




import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class DeadlockListenerImpl_Test
{
    @Test
    public void testRealDeadlockOccurredHandledGracefully()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final DeadlockListener listener = InterfaceProxyFactory.getProxy( DeadlockListener.class, btih );
        
        final DeadlockDetector detector = new DeadlockDetector( 100 );
        detector.addDeadlockListener( new DeadlockListenerImpl() );
        detector.addDeadlockListener( listener );
        
        final WorkPool wp = performDeadlock();
        try
        {
            int i = 1000;
            while ( --i > 0 && 0 == btih.getTotalCallCount() )
            {
                sleep( 10 );
            }
            assertTrue(
                    0 < btih.getTotalCallCount(),
                    "Shoulda been a deadlock."
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
            Thread.currentThread().interrupt();
            throw new RuntimeException( ex );
        }
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
}
