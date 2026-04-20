/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanInfo_Test 
{
    @Test
    public void testConstructorNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanInfo( 
                        null,
                        InterfaceProxyFactory.getProxy( BeanInfoCache.class, null ) );
            }
        } );
    }
    
    
    @Test
    public void testConstructorNullCacheNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanInfo( 
                        TestBean.class,
                        null );
            }
        } );
    }
    
    
    @Test
    public void testHappyConstruction()
    {
        new BeanInfo( 
                TestBean.class,
                InterfaceProxyFactory.getProxy( BeanInfoCache.class, null ) );
    }
}
