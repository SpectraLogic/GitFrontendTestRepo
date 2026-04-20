/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;



public final class WorkPoolFactory_Test 
{
    @Test
    public void testConstuctorNullBaseNameNotAllowed()
    {
        TestUtil.assertThrows(
                null, 
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                        {
                            WorkPoolFactory.createWorkPool( 1, null );
                        }
                    } );
    }
    
    
    @Test
    public void testConstuctorInvalidNumberOfThreadsNotAllowed()
    {
        TestUtil.assertThrows(
                null, 
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                        {
                            WorkPoolFactory.createWorkPool( 0, getClass().getSimpleName() );
                        }
                    } );
        
        assertNotNull(
                WorkPoolFactory.createWorkPool( 1, getClass().getSimpleName() ),
                "Shoulda constructed a factory."
                 );
        assertNotNull(
                WorkPoolFactory.createWorkPool( 20, getClass().getSimpleName() ),
                "Shoulda constructed a factory."
                 );
        assertNotNull(
                WorkPoolFactory.createWorkPool( 100, getClass().getSimpleName()),
                "Shoulda constructed a factory."
                  );
        
        TestUtil.assertThrows(
                null, 
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                        {
                            WorkPoolFactory.createWorkPool( 1000, getClass().getSimpleName() );
                        }
                    } );
    }
    
    
    @Test
    public void testRunnableThatThrowsExceptionIsLogged() throws InterruptedException
    {
        // Manual verification is necessary for this test
        final WorkPool workPool = WorkPoolFactory.createWorkPool( 1, getClass().getSimpleName() );
        workPool.submit( 
                new Runnable()
                {
                    public void run()
                    {
                        throw new UnsupportedOperationException( "I like to throw exceptions." );
                    }
                } );
        workPool.shutdownNow();
        workPool.awaitTermination( 10, TimeUnit.SECONDS );
    }
    
    
    @Test
    public void testQueueSizeHonored()
    {
        // Manual verification is necessary for this test
        final WorkPool wp1 = WorkPoolFactory.createWorkPool( 1, "wp1" );
        final WorkPool wp2 = WorkPoolFactory.createBoundedWorkPool( 2, 1, "wp2" );
        
        final Runnable r = new Runnable()
        {
            public void run()
            {
                TestUtil.sleep( 10000 );
            }
        };
        
        wp1.submit( r );
        wp1.submit( r );
        wp1.submit( r );
        wp1.submit( r );
        wp2.submit( r );
        wp2.submit( r );
        wp2.submit( r );
        final AtomicBoolean wp2AdderDone = new AtomicBoolean( false );
        final Runnable wp2Adder = new Runnable()
        {
            public void run()
            {
                try
                {
                    wp2.submit( r );
                }
                finally
                {
                    wp2AdderDone.set( true );
                }
            }
        };
        SystemWorkPool.getInstance().submit( wp2Adder );
        
        TestUtil.sleep( 10 );
        assertFalse(
                wp2AdderDone.get(),
                "Shoulda blocked attempting to add.");
        
        wp1.shutdownNow();
        wp2.shutdownNow();
        
        int i = 100;
        while ( --i > 0 && !wp2AdderDone.get() )
        {
            TestUtil.sleep( 10 );
        }
        assertTrue(
                wp2AdderDone.get(),
                "Shoulda terminated.");
    }
}
