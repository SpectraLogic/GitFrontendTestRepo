/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class InterfaceProxyFactory_Test 
{
    @Test
    public void testGetProxyNullInterfaceNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    InterfaceProxyFactory.getProxy( null, null );
                }
            } );
    }
    

    @Test
    public void testGetProxyNullInvocationHandlerCreatesValidProxy()
    {
        final List< ? > list = InterfaceProxyFactory.getProxy( List.class, null );
        assertEquals(
                0,
                list.size(),
                "Shoulda returned nullinvocationhandler value."
                );
        assertEquals(
                null,
                list.get( 0 ),
                "Shoulda returned nullinvocationhandler value."
                 );
    }
    

    @Test
    public void testGetProxyNonNullInvocationHandlerCreatesValidProxy()
    {
        final List< ? > list = InterfaceProxyFactory.getProxy( List.class, null );
        assertEquals(
                0,
                list.size(),
                "Shoulda returned nullinvocationhandler value."
                 );
        assertEquals(
                null,
                list.get( 0 ),
                "Shoulda returned nullinvocationhandler value."
                 );
    }
    
    
    @Test
    public void testGetTypeNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    InterfaceProxyFactory.getType( null );
                }
            } );
    }
    
    
    @Test
    public void testGetTypeReturnsConcreteClassIfNotProxy()
    {
        assertEquals(
                String.class,
                InterfaceProxyFactory.getType( String.class ),
                "If not proxied, shoulda returned actual type."
               );
    }
    
    
    @Test
    public void testGetTypeReturnsProxiedInterfaceIfProxy()
    {
        assertEquals(
                TestBean.class,
                InterfaceProxyFactory.getType( BeanFactory.newBean( TestBean.class ).getClass() ),
                "If proxied, shoulda returned interface type."
                 );
    }
}
