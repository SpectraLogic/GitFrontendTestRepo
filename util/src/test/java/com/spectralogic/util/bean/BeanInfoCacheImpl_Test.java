/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;




import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanInfoCacheImpl_Test
{
    @Test
    public void testGetBeanInfoWithNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanInfoCacheImpl().getBeanInfo( null );
            }
        } );
    }
    
    
    @Test
    public void testGetBeanInfoReturnsCachedInstance()
    {
        final BeanInfoCache cache = new BeanInfoCacheImpl();
        final BeanInfo beanInfo = cache.getBeanInfo( TestBean.class );
        assertTrue(
                beanInfo == cache.getBeanInfo( TestBean.class ),
                "Shoulda returned same non-null instance of BeanInfo."
                 );
        assertNotNull(
                beanInfo,
                "Shoulda returned same non-null instance of BeanInfo."
                 );
    }
}
