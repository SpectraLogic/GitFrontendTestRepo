/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.NamingConventionType;



import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;


public final class ToXmlInvocationHandler_Test
{
    @Test
    public void testInvocationForWrongMethodNotAllowed()
    {
        final XmlableTestBean bean = InterfaceProxyFactory.getProxy(
                XmlableTestBean.class,
                new ToXmlInvocationHandler() );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
                {
                    bean.getIntProp();
                }
            } );
    }
    
    
    private interface XmlableTestBean extends Marshalable, TestBean
    {
        @ExcludeFromMarshaler( When.ALWAYS )
        String getPropOne();
        void setPropOne( final String propOne );
        
        String getPropTwo();
        void setPropTwo( final String propTwo );
    }
    
    
    @Test
    public void testXmlInvocationHandlerCorrectlyGeneratesXmlOutput()
    {
        final XmlableTestBean bean = BeanFactory.newBean( XmlableTestBean.class );
        bean.setPropOne( "Justin" );
        bean.setPropTwo( "Jason" );
        final String xml = bean.toXml( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE );

        assertFalse(
                xml.contains( "Justin" ),
                "JSON output should notta included prop one."
                );
        assertTrue(
                xml.contains( "Jason" ),
                "JSON output shoulda included prop two."
                 );
    }
}
