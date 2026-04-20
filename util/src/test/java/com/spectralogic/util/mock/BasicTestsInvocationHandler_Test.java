/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class BasicTestsInvocationHandler_Test 
{

    @Test
    public void testConstructorNullDecoratedInvocationHandlerAllowed()
    {
        new BasicTestsInvocationHandler( null );
    }
    
    
    @Test
    public void testInvocationDataReportedAccuratelyWhenNoInvocations()
            throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        assertEquals(0,  btih.getTotalCallCount(), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodInvokeData().size(), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodCallCount(methodSize), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodInvokeData(methodSize).size(), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodCallCount(methodAdd), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodInvokeData(methodAdd).size(), "Should notta reported any invocations yet.");
    }
    
    
    @Test
    public void testInvocationDataReportedAccuratelyWhen1Invocation()
            throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        
        list.add( "bob" );

        assertEquals(1,  btih.getTotalCallCount(), "Shoulda reported single invocation to add 'bob'.");
        assertEquals(1,  btih.getMethodInvokeData().size(), "Shoulda reported single invocation to add 'bob'.");
        assertEquals(0,  btih.getMethodCallCount(methodSize), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodInvokeData(methodSize).size(), "Should notta reported any invocations yet.");
        assertEquals(1,  btih.getMethodCallCount(methodAdd), "Shoulda reported single invocation to add 'bob'.");
        assertEquals(1,  btih.getMethodInvokeData(methodAdd).size(), "Shoulda reported single invocation to add 'bob'.");
    }
    
    
    @Test
    public void testInvocationDataReportedAccuratelyWhen2Invocations()
            throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );

        list.add( "bob" );
        list.add( "jason" );

        assertEquals(2,  btih.getTotalCallCount(), "Shoulda reported invocations to add 'bob' and 'jason'.");
        assertEquals(2,  btih.getMethodInvokeData().size(), "Shoulda reported invocations to add 'bob' and 'jason'.");
        assertEquals(0,  btih.getMethodCallCount(methodSize), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodInvokeData(methodSize).size(), "Should notta reported any invocations yet.");
        assertEquals(2,  btih.getMethodCallCount(methodAdd), "Shoulda reported invocations to add 'bob' and 'jason'.");
        assertEquals(2,  btih.getMethodInvokeData(methodAdd).size(), "Shoulda reported invocations to add 'bob' and 'jason'.");
    }
    
    
    @Test
    public void testInvocationDataReportedAccuratelyWhen3Invocations()
            throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        
        list.add( "bob" );
        list.add( "jason" );
        list.add( "barry" );

        assertEquals(3,  btih.getTotalCallCount(), "Shoulda reported invocations to add 'bob', 'jason' and 'barry'.");
        assertEquals(3,  btih.getMethodInvokeData().size(), "Shoulda reported invocations to add 'bob', 'jason' and 'barry'.");
        assertEquals(0,  btih.getMethodCallCount(methodSize), "Should notta reported any invocations yet.");
        assertEquals(0,  btih.getMethodInvokeData(methodSize).size(), "Should notta reported any invocations yet.");
        assertEquals(3,  btih.getMethodCallCount(methodAdd), "Shoulda reported invocations to add 'bob', 'jason' and 'barry'.");
        assertEquals(3,  btih.getMethodInvokeData(methodAdd).size(), "Shoulda reported invocations to add 'bob', 'jason' and 'barry'.");
    }
    
    
    @Test
    public void testInvocationDataReportedAccuratelyWhen4Invocations()
            throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        
        list.add( "bob" );
        list.add( "jason" );
        list.add( "barry" );
        list.size();

        assertEquals(4,  btih.getTotalCallCount(), "Shoulda reported invocations to add 'bob', 'jason' and 'barry' plus the size invocation.");
        assertEquals(4,  btih.getMethodInvokeData().size(), "Shoulda reported invocations to add 'bob', 'jason' and 'barry' plus the size invocation.");
        assertEquals(1,  btih.getMethodCallCount(methodSize), "Shoulda reported single invocation.");
        assertEquals(1,  btih.getMethodInvokeData(methodSize).size(), "Shoulda reported single invocation.");
        assertEquals(3,  btih.getMethodCallCount(methodAdd), "Shoulda reported invocations to add 'bob', 'jason' and 'barry'.");
        assertEquals(3,  btih.getMethodInvokeData(methodAdd).size(), "Shoulda reported invocations to add 'bob', 'jason' and 'barry'.");
    }
    
    
    @Test
    public void testMethodInvokeDataReturnedIsCorrect() throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        
        list.add( "bob" );
        list.add( "jason" );
        list.add( "barry" );
        list.size();

        assertEquals(4,  btih.getMethodInvokeData().size(), "Shoulda reported invocation data correctly.");
        assertEquals(methodAdd, btih.getMethodInvokeData().get( 0 ).getMethod(), "Shoulda reported invocation data correctly.");
        assertEquals(methodAdd, btih.getMethodInvokeData().get( 1 ).getMethod(), "Shoulda reported invocation data correctly.");
        assertEquals(methodAdd, btih.getMethodInvokeData().get( 2 ).getMethod(), "Shoulda reported invocation data correctly.");
        assertEquals(methodSize, btih.getMethodInvokeData().get( 3 ).getMethod(), "Shoulda reported invocation data correctly.");

        assertEquals(3,  btih.getMethodInvokeData(methodAdd).size(), "Shoulda reported invocation data correctly.");
        assertEquals(methodAdd, btih.getMethodInvokeData( methodAdd ).get( 0 ).getMethod(), "Shoulda reported invocation data correctly.");
        assertEquals(methodAdd, btih.getMethodInvokeData( methodAdd ).get( 1 ).getMethod(), "Shoulda reported invocation data correctly.");
        assertEquals(methodAdd, btih.getMethodInvokeData( methodAdd ).get( 2 ).getMethod(), "Shoulda reported invocation data correctly.");

        assertEquals(1,  btih.getMethodInvokeData(methodSize).size(), "Shoulda reported invocation data correctly.");
        assertEquals(methodSize, btih.getMethodInvokeData( methodSize ).get( 0 ).getMethod(), "Shoulda reported invocation data correctly.");

        assertEquals(0,  btih.getMethodInvokeData(methodSize).get(0).getArgs().size(), "Shoulda reported invocation data correctly.");
        assertEquals("bob", btih.getMethodInvokeData( methodAdd ).get( 0 ).getArgs().get( 0 ), "Shoulda reported invocation data correctly.");
        assertEquals("jason", btih.getMethodInvokeData( methodAdd ).get( 1 ).getArgs().get( 0 ), "Shoulda reported invocation data correctly.");
        assertEquals("barry", btih.getMethodInvokeData( methodAdd ).get( 2 ).getArgs().get( 0 ), "Shoulda reported invocation data correctly.");
    }
    
    
    @Test
    public void testVerifyMethodCallsAsExpectedDoesSo() throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        
        list.add( "bob" );
        list.add( "jason" );
        list.add( "barry" );
        list.size();

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                final Map< Method, Integer > expectedCallCounts = new HashMap<>();
                expectedCallCounts.put( methodAdd, Integer.valueOf( 3 ) );
                btih.verifyMethodInvocations( expectedCallCounts );
            }
        } );
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                final Map< Method, Integer > expectedCallCounts = new HashMap<>();
                expectedCallCounts.put( methodAdd, Integer.valueOf( 3 ) );
                expectedCallCounts.put( methodSize, Integer.valueOf( 2 ) );
                btih.verifyMethodInvocations( expectedCallCounts );
            }
        } );
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                final Map< Method, Integer > expectedCallCounts = new HashMap<>();
                expectedCallCounts.put( methodAdd, Integer.valueOf( 3 ) );
                expectedCallCounts.put( methodSize, Integer.valueOf( 0 ) );
                btih.verifyMethodInvocations( expectedCallCounts );
            }
        } );
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                final Map< Method, Integer > expectedCallCounts = new HashMap<>();
                expectedCallCounts.put( methodAdd, Integer.valueOf( 2 ) );
                expectedCallCounts.put( methodSize, Integer.valueOf( 1 ) );
                btih.verifyMethodInvocations( expectedCallCounts );
            }
        } );
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                final Map< Method, Integer > expectedCallCounts = new HashMap<>();
                expectedCallCounts.put( 
                        ReflectUtil.getMethod( List.class, "toArray" ), Integer.valueOf( 1 ) );
                expectedCallCounts.put( methodAdd, Integer.valueOf( 3 ) );
                expectedCallCounts.put( methodSize, Integer.valueOf( 1 ) );
                btih.verifyMethodInvocations( expectedCallCounts );
            }
        } );
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodAdd, Integer.valueOf( 3 ) );
        expectedCallCounts.put( methodSize, Integer.valueOf( 1 ) );
        btih.verifyMethodInvocations( expectedCallCounts );
    }
    
    
    @Test
    public void testEventuallyVerifyMethodInvocationsFauksIfDoesNotEventuallyPassWithinTimeout() 
            throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        
        SystemWorkPool.getInstance().submit( new Runnable()
        {
            public void run()
            {
                final SecureRandom random = new SecureRandom();
                TestUtil.sleep( random.nextInt( 20 ) );
                list.add( "bob" );
                TestUtil.sleep( random.nextInt( 20 ) );
                list.add( "jason" );
                TestUtil.sleep( random.nextInt( 20 ) );
                list.add( "barry" );
            }
        } );

        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodAdd, Integer.valueOf( 3 ) );
        expectedCallCounts.put( methodSize, Integer.valueOf( 1 ) );

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 100 );
            }
        } );
    }
    
    
    @Test
    public void testEventuallyVerifyMethodInvocationsReturnsIfEventuallyPassesWithinTimeout() 
            throws NoSuchMethodException, SecurityException
    {
        final Method methodAdd = List.class.getDeclaredMethod( "add", Object.class );
        final Method methodSize = List.class.getDeclaredMethod( "size" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy( 
                List.class,
                btih );
        
        SystemWorkPool.getInstance().submit( new Runnable()
        {
            public void run()
            {
                final SecureRandom random = new SecureRandom();
                TestUtil.sleep( random.nextInt( 20 ) );
                list.add( "bob" );
                TestUtil.sleep( random.nextInt( 20 ) );
                list.add( "jason" );
                TestUtil.sleep( random.nextInt( 20 ) );
                list.add( "barry" );
                TestUtil.sleep( random.nextInt( 20 ) );
                list.size();
            }
        } );

        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodAdd, Integer.valueOf( 3 ) );
        expectedCallCounts.put( methodSize, Integer.valueOf( 1 ) );
        btih.eventuallyVerifyMethodInvocations( expectedCallCounts, 1000 );
    }
}
