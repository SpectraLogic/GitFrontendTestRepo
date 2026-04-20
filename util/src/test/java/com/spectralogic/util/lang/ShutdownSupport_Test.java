/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.shutdown.ShutdownListener;
import com.spectralogic.util.shutdown.ShutdownSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ShutdownSupport_Test 
{
    @Test
    public void testNameCannotBeNull()
    {
        TestUtil.assertThrows( 
                null, 
                NullPointerException.class, new BlastContainer()
                {
                    public void test()
                        {
                            new ShutdownSupport( (Class<?>)null );
                        }
                    } );

        TestUtil.assertThrows( 
                null, 
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                        {
                            new ShutdownSupport( (String)null );
                        }
                    } );
    }
    
    
    @Test
    public void testVerifyNotShutdownThrowsExceptionIffAlreadyShutdown()
    {
        final ShutdownSupport support = new ShutdownSupport( "mock thing to shutdown" );
        support.verifyNotShutdown();
        support.shutdown();
        TestUtil.assertThrows( 
                null, 
                IllegalStateException.class, new BlastContainer()
                {
                    public void test()
                        {
                            support.verifyNotShutdown();
                        }
                    });
    }
    
    
    @Test
    public void testCleanupWorkerOnlyCalledOnceWhenMultipleShutdownCalls()
    {
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ShutdownListener r =
                InterfaceProxyFactory.getProxy( ShutdownListener.class, btih );
        final Method methodShutdownOccurred = 
                ReflectUtil.getMethod( ShutdownListener.class, "shutdownOccurred" );
        
        final ShutdownSupport support = new ShutdownSupport( "mock thing to shutdown" );
        support.addShutdownListener( r );
        assertEquals(
                0,
                btih.getMethodCallCount( methodShutdownOccurred ),
                "Shoulda been no calls initially."
                 );
        
        support.shutdown();
        assertEquals(
                1,
                btih.getMethodCallCount( methodShutdownOccurred ),
                "Shoulda been a call to cleanup."
               );
        
        support.shutdown();
        assertEquals(
                1,
                btih.getMethodCallCount( methodShutdownOccurred ),
                "Should notta been a subsequent call to cleanup."
                );
    }
}
