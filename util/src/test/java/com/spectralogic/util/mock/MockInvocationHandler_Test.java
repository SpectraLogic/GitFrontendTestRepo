/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class MockInvocationHandler_Test 
{
    @Test
    public void testForMethodNullMethodNotAllowed()
    {
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                    {
                        MockInvocationHandler.forMethod(
                                (Method)null, 
                                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ),
                                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ) );
                    }
                } );
    }
    

    @Test
    public void testForMethodNullInvocationHandlerNotAllowed()
    {
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                    {
                        MockInvocationHandler.forMethod(
                                ReflectUtil.getMethod( List.class, "size" ), 
                                null,
                                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ) );
                    }
                } );
    }
    

    @Test
    public void testForMethodNullDecoratedInvocationHandlerAllowed()
    {
        MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( List.class, "size" ), 
                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ),
                null );
    }
    

    @Test
    public void testForMethodInvocationHandlerRespondsCorrectlyToInvocations() 
            throws NoSuchMethodException, SecurityException
    {
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy(
                List.class, 
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( List.class, "size" ),
                        new ConstantResponseInvocationHandler( Integer.valueOf( 22 ) ),
                        MockInvocationHandler.forMethod(
                                List.class.getMethod( "remove", Object.class ),
                                new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                null ) ) );
        assertEquals(
                22,
                list.size(),
                "Shoulda returned mocked response."
                 );
        assertEquals(
                true,
                list.remove( "a" ),
                "Shoulda returned mocked response."
                 );
        assertEquals(
                false,
                list.add( "a" ),
                "Shoulda returned null invocation handler response."
                 );
    }
    

    @Test
    public void testForReturnTypeNullReturnTypeNotAllowed()
    {
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                    {
                        MockInvocationHandler.forReturnType(
                                null, 
                                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ),
                                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ) );
                    }
                } );
    }
    

    @Test
    public void testForReturnTypeNullInvocationHandlerNotAllowed()
    {
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                    {
                        MockInvocationHandler.forReturnType(
                                Object.class, 
                                null,
                                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ) );
                    }
                } );
    }
    

    @Test
    public void testForReturnTypeNullDecoratedInvocationHandlerAllowed()
    {
        MockInvocationHandler.forReturnType(
                Object.class, 
                InterfaceProxyFactory.getProxy( InvocationHandler.class, null ),
                null );
    }
    

    @Test
    public void testForReturnTypeInvocationHandlerRespondsCorrectlyToInvocations()
    {
        @SuppressWarnings( "unchecked" )
        final List< String > list = InterfaceProxyFactory.getProxy(
                List.class, 
                MockInvocationHandler.forReturnType(
                        int.class,
                        new ConstantResponseInvocationHandler( Integer.valueOf( 22 ) ),
                        MockInvocationHandler.forReturnType(
                                boolean.class,
                                new ConstantResponseInvocationHandler( Boolean.TRUE ),
                                null ) ) );
        assertEquals(
                22,
                list.size(),
                "Shoulda returned mocked response."
                );
        assertEquals(
                true,
                list.remove( "a" ),
                "Shoulda returned mocked response."
                 );
        assertEquals(
                true,
                list.add( "a" ),
                "Shoulda returned mocked response."
                 );
    }
    

    @Test
    public void testForToStringAllowed()
    {
        MockInvocationHandler.forToString( "hello" );
    }
}
