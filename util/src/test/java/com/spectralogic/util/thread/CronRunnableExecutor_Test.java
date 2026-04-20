/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.NullInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class CronRunnableExecutor_Test
{
    @Test
    public void testVerifyNullCronNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                CronRunnableExecutor.verify( null );
            }
        } );
    }
    

    @Test
    public void testVerifyInvalidCronNotAllowed()
    {
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                CronRunnableExecutor.verify( "hellokitty" );
            }
        } );
    }
    
    
    @Test
    public void testVerifyUnsupportedCronNotAllowed()
    {
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                CronRunnableExecutor.verify( "* * * * * *" );
            }
        } );
    }
    

    @Test
    public void testVerifySupportedValidCronAllowed()
    {
        CronRunnableExecutor.verify( "0 15 10 L * ?" );
    }
    
    
    @Test
    public void testScheduleNullIdentifierNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                CronRunnableExecutor.schedule( 
                        null,
                        "0 15 10 L * ?",
                        InterfaceProxyFactory.getProxy( Runnable.class, null ) );
            }
        } );
    }
    
    
    @Test
    public void testScheduleNullCronExpressionNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                CronRunnableExecutor.schedule( 
                        new CronRunnableIdentifier( "a" ), 
                        null,
                        InterfaceProxyFactory.getProxy( Runnable.class, null ) );
            }
        } );
    }
    
    
    @Test
    public void testScheduleWithoutRunnablesNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                CronRunnableExecutor.schedule( 
                        new CronRunnableIdentifier( "a" ), 
                        "0 15 10 L * ?" );
            }
        } );
    }
    
    
    @Test
    public void testScheduleWhenAlreadyScheduledAllowed()
    {
        assertFalse(
                CronRunnableExecutor.isScheduled( new CronRunnableIdentifier( "a" ) ),
                "Should notta reported job as scheduled yet.");
        CronRunnableExecutor.schedule( 
                new CronRunnableIdentifier( "a" ), 
                "0 15 10 L * ?",
                InterfaceProxyFactory.getProxy( Runnable.class, null ) );
        assertTrue(
                CronRunnableExecutor.isScheduled( new CronRunnableIdentifier( "a" ) ),
                "Shoulda reported job as scheduled.");
        CronRunnableExecutor.schedule( 
                new CronRunnableIdentifier( "a" ), 
                "0 15 10 L * ?",
                InterfaceProxyFactory.getProxy( Runnable.class, null ) );
        assertTrue(
                CronRunnableExecutor.isScheduled( new CronRunnableIdentifier( "a" ) ),
                "Shoulda reported job as scheduled.");
        CronRunnableExecutor.unschedule( new CronRunnableIdentifier( "a" ) );
        assertFalse(
                CronRunnableExecutor.isScheduled( new CronRunnableIdentifier( "a" ) ),
                "Should notta reported job as scheduled anymore.");
    }
    
    
    @Test
    public void testRunnablesAreExecutedPerCronSchedule()
    {
        final BasicTestsInvocationHandler btih1 = 
                new BasicTestsInvocationHandler( new MisbehavingInvocationHandler() );
        final BasicTestsInvocationHandler btih2 = 
                new BasicTestsInvocationHandler( new MisbehavingInvocationHandler() );
        final BasicTestsInvocationHandler btih3 = 
                new BasicTestsInvocationHandler( new MisbehavingInvocationHandler() );
        final BasicTestsInvocationHandler btih4 = 
                new BasicTestsInvocationHandler( null );
        final BasicTestsInvocationHandler btih5 = 
                new BasicTestsInvocationHandler( new LongRunningInvocationHandler() );

        CronRunnableExecutor.schedule(
                new CronRunnableIdentifier( "a" ), 
                "0/1 * * * * ?", 
                InterfaceProxyFactory.getProxy( Runnable.class, btih1 ) );
        CronRunnableExecutor.schedule(
                new CronRunnableIdentifier( "b" ), 
                "0/1 * * * * ?", 
                InterfaceProxyFactory.getProxy( Runnable.class, btih2 ),
                InterfaceProxyFactory.getProxy( Runnable.class, btih3 ) );
        CronRunnableExecutor.schedule(
                new CronRunnableIdentifier( "c" ), 
                "0/1 * * * * ?", 
                InterfaceProxyFactory.getProxy( Runnable.class, btih4 ) );
        CronRunnableExecutor.schedule(
                new CronRunnableIdentifier( "c" ), 
                "0/4 * * * * ?", 
                InterfaceProxyFactory.getProxy( Runnable.class, btih4 ) );
        CronRunnableExecutor.schedule(
                new CronRunnableIdentifier( "d" ), 
                "0/1 * * * * ?", 
                InterfaceProxyFactory.getProxy( Runnable.class, btih5 ) );
        
        TestUtil.sleep( 1200 );
        CronRunnableExecutor.unschedule( new CronRunnableIdentifier( "a" ) );
        TestUtil.sleep( 2000 );
        CronRunnableExecutor.unschedule( new CronRunnableIdentifier( "b" ) );
        CronRunnableExecutor.unschedule( new CronRunnableIdentifier( "c" ) );
        
        final Method methodRun = ReflectUtil.getMethod( Runnable.class, "run" );
        assertBetween(
                2,
                3,
                btih1.getMethodCallCount( methodRun ) );
        assertBetween(
                4,
                5,
                btih2.getMethodCallCount( methodRun ) );
        assertBetween(
                4,
                5,
                btih3.getMethodCallCount( methodRun ) );
        assertBetween(
                1,
                3,
                btih4.getMethodCallCount( methodRun ) );
        assertBetween(
                1,
                1,
                btih5.getMethodCallCount( methodRun ) );
    }
    
    
    private void assertBetween( final int min, final int max, final int actual )
    {
        if ( actual < min )
        {
            fail( "Shoulda had at least " + min + " invocations, but had " + actual + " invocations." );
        }
        if ( actual > max )
        {
            fail( "Shoulda had no more than " + max + " invocations, but had " + actual + " invocations." );
        }
    }
    
    
    private final static class MisbehavingInvocationHandler implements InvocationHandler
    {
        public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
        {
            if ( "run".equals( method.getName() ) )
            {
                throw new RuntimeException( "I like to misbehave." );
            }
            return NullInvocationHandler.getInstance().invoke( proxy, method, args );
        }
    } // end inner class def
    
    
    private final static class LongRunningInvocationHandler implements InvocationHandler
    {
        public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
        {
            if ( "run".equals( method.getName() ) )
            {
                TestUtil.sleep( 6000 );
            }
            return NullInvocationHandler.getInstance().invoke( proxy, method, args );
        }
    } // end inner class def
}
