/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;


public final class RecurringRunnableExecutor_Test 
{
    @Test
    public void testConstructorNullRunnableNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new RecurringRunnableExecutor( null, 1 );
                }
            } );
    }
    

    @Test
    public void testConstructorZeroIntervalNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new RecurringRunnableExecutor( InterfaceProxyFactory.getProxy( Runnable.class, null ), 0 );
                }
            } );
    }
    

    @Test
    public void testConstructorNegativeIntervalNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new RecurringRunnableExecutor( InterfaceProxyFactory.getProxy( Runnable.class, null ), -1 );
                }
            } );
    }
    

    @Test
    public void testRunnableExecutedRepeatedlyFromStartToShutdown()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final RecurringRunnableExecutor executor = 
                new RecurringRunnableExecutor( InterfaceProxyFactory.getProxy( Runnable.class, btih ), 1 );
        
        TestUtil.sleep( 10 );
        
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Runnable should notta been called yet."
                );
        
        executor.start();
        TestUtil.sleep( 350 );
        
        assertTrue(
                1 < btih.getTotalCallCount(),
                "Runnable shoula been called several times.");
        
        executor.shutdown();
        TestUtil.sleep( 100 );
        
        final int currentCallCount = btih.getTotalCallCount();
        TestUtil.sleep( 10 );
        assertEquals(
                currentCallCount,
                btih.getTotalCallCount(),
                "Runnable should notta been called after shutdown."
                );
    }
}
