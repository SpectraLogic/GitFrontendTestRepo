/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class MutexedRunnable_Test 
{
    @Test
    public void testConstructorNullLockNameNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final MutexService service = dbSupport.getServiceManager().getService( MutexService.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new MutexedRunnable(
                        null,
                        service, 
                        InterfaceProxyFactory.getProxy( Runnable.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullServiceNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new MutexedRunnable(
                        "lock", 
                        null,
                        InterfaceProxyFactory.getProxy( Runnable.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullRunnableNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final MutexService service = dbSupport.getServiceManager().getService( MutexService.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new MutexedRunnable(
                        "lock", 
                        service, 
                        null );
            }
        } );
    }
    

    @Test
    public void testMutexedRunnableCallsRunnable()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final MutexService service = dbSupport.getServiceManager().getService( MutexService.class );

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final Runnable r = new MutexedRunnable(
                "lock", service, InterfaceProxyFactory.getProxy( Runnable.class, btih ) );
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta called run yet."
               );
        r.run();
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda called run."
                );
    }
}
