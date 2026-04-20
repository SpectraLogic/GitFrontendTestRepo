/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;

public final class MemoryHogDetector_Test 
{
    @Test
    public void testListenerNotifiedOfMemoryPools() throws InterruptedException
    {
        final MemoryHogDetector detector = new MemoryHogDetector( 500 );

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final MemoryHogListener listener = InterfaceProxyFactory.getProxy( 
                MemoryHogListener.class,
                btih );
        detector.addMemoryHogListener( listener );
        
        final int initialCallCount = btih.getTotalCallCount();
        int i = 1000;
        while ( --i > 0 && btih.getTotalCallCount() == initialCallCount )
        {
            Thread.sleep( 10 );
        }
        
        final List< MethodInvokeData > data = btih.getMethodInvokeData( ReflectUtil.getMethod( 
                MemoryHogListener.class, 
                "monitorMemoryUsage" ) ); 
        assertFalse(
                data.isEmpty(),
                "Shoulda called listener to check memory pools."
                 );
    }
}
