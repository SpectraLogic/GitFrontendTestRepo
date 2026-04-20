/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.testfrmwrk.TestConcreteBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanFactory_Test 
{
    interface TestBean
    {
        String NAME = "name";
        
        String getName();
        
        void setName( final String value );
        
        
        String SERIAL_NUMBER = "serialNumber";
        
        String getSerialNumber();
        
        void setSerialNumber( final String value );
        
        
        String ERROR_MESSAGE = "errorMessage";
        
        String getErrorMessage();
        
        void setErrorMessage( final String value );
    } // end inner class def
    
    
    interface ProxyOnlyTestBean extends SimpleBeanSafeToProxy, TestBean
    {
        // empty
    } // end inner class def

    
    @ConcreteImplementation( ProxyableWithConcreteImplementationTestBeanImpl.class )
    interface ProxyableWithConcreteImplementationTestBean extends SimpleBeanSafeToProxy, TestBean
    {
        // empty
    } // end inner class def
    
    
    final static class ProxyableWithConcreteImplementationTestBeanImpl
        extends BaseMarshalable 
        implements ProxyableWithConcreteImplementationTestBean
    {
        public String getName()
        {
            return m_name;
        }

        public void setName( final String value )
        {
            m_name = value;
        }
        
        public String getSerialNumber()
        {
            return m_serialNumber;
        }

        public void setSerialNumber( final String value )
        {
            m_serialNumber = value;
        }
        
        public String getErrorMessage()
        {
            return m_errorMessage;
        }

        public void setErrorMessage( final String value )
        {
            m_errorMessage = value;
        }
        
        private volatile String m_name;
        private volatile String m_serialNumber;
        private volatile String m_errorMessage;
    } // end inner class def
    
    
    public final static class ConcreteTestBean extends BaseMarshalable implements TestBean
    {
        public String getName()
        {
            return m_name;
        }

        public void setName( final String value )
        {
            m_name = value;
        }
        
        public String getSerialNumber()
        {
            return m_serialNumber;
        }

        public void setSerialNumber( final String value )
        {
            m_serialNumber = value;
        }
        
        public String getErrorMessage()
        {
            return m_errorMessage;
        }

        public void setErrorMessage( final String value )
        {
            m_errorMessage = value;
        }
        
        private volatile String m_name;
        private volatile String m_serialNumber;
        private volatile String m_errorMessage;
    } // end inner class def
    
    
    @Test
    public void testGetTypeNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanFactory.getType( null );
            }
        } );
    }
    
    
    @Test
    public void testGetTypeReturnsConcreteClassIfNotProxy()
    {
        assertEquals(
                String.class,
                BeanFactory.getType( String.class ),
                "If not proxied, shoulda returned actual type."
                 );
    }
    
    
    @Test
    public void testGetTypeReturnsProxiedInterfaceIfProxy()
    {
        assertEquals(
                ProxyOnlyTestBean.class,
                BeanFactory.getType( BeanFactory.newBean( ProxyOnlyTestBean.class ).getClass() ),
                "If proxied, shoulda returned interface type."
                 );
    }
    
    
    @Test
    public void testGetTypeReturnsProxiedInterfaceIfProxyConcreteImpl()
    {
        assertEquals(
                ProxyableWithConcreteImplementationTestBean.class,
                BeanFactory.getType( BeanFactory.newBean(
                        ProxyableWithConcreteImplementationTestBean.class ).getClass() ),
                "If proxied, shoulda returned interface type."
                );
    }
    
    
    @Test
    public void testNewBeanClassIsInterfaceReturnsNonNull()
    {
        assertNotNull(
                BeanFactory.newBean( com.spectralogic.util.testfrmwrk.TestBean.class ),
                "Shoulda returned a bean."
                 );
    }
    
    
    @Test
    public void testNewBeanClassIsConcreteReturnsNonNull()
    {
        assertNotNull(
                BeanFactory.newBean( TestConcreteBean.class ),
                "Shoulda returned a bean."
                 );
    }
    
    
    @Test
    public void testBeanConstructionPerformance()
    {
        final int millis = 100;
        long count = 0;
        Duration d = new Duration();
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            new ConcreteTestBean();
        }
        LOG.info( "Creating beans statically via explicit constructor calls at:" 
                  + Platform.NEWLINE + ( count * 1 ) / d.getElapsedMillis() + "K per second." );

        count = 0;
        d = new Duration();
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            new ConcreteTestBean();
        }
        LOG.info( "Creating beans statically via factory at:" 
                  + Platform.NEWLINE + ( count * 1 ) / d.getElapsedMillis() + "K per second." );

        count = 0;
        d = new Duration();
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            BeanFactory.newBean( TestConcreteBean.class );
        }
        LOG.info( "Creating beans reflectively via factory at:" 
                  + Platform.NEWLINE + ( count * 1 ) / d.getElapsedMillis() + "K per second." );

        count = 0;
        d = new Duration();
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            BeanFactory.newBean( ProxyableWithConcreteImplementationTestBean.class );
        }
        LOG.info( "Creating beans reflectively (with concrete impls defined) via factory at:" 
                  + Platform.NEWLINE + ( count * 1 ) / d.getElapsedMillis() + "K per second." );
    }
    
    
    @Test
    public void testBeanMethodInvocationPerformance()
    {
        final int millis = 100;
        long count = 0;
        Duration d = new Duration();
        TestBean bean = BeanFactory.newBean( ConcreteTestBean.class );
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            bean.setName( "hello i am the string prop that is being set." );
            bean.getName();
        }
        LOG.info( "Invoking compiled bean methods at:" 
                  + Platform.NEWLINE + ( count * 2 ) / d.getElapsedMillis() + "K per second." );

        count = 0;
        d = new Duration();
        bean = BeanFactory.newBean( ProxyOnlyTestBean.class );
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            bean.setName( "hello i am the string prop that is being set." );
            bean.getName();
        }
        LOG.info( "Invoking bean methods reflectively at:" 
                  + Platform.NEWLINE + ( count * 2 ) / d.getElapsedMillis() + "K per second." );

        count = 0;
        d = new Duration();
        bean = BeanFactory.newBean( ProxyableWithConcreteImplementationTestBean.class );
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            bean.setName( "hello i am the string prop that is being set." );
            bean.getName();
        }
        LOG.info( "Invoking bean methods reflectively (with concrete impls defined) at:" 
                  + Platform.NEWLINE + ( count * 2 ) / d.getElapsedMillis() + "K per second." );
    }
    
    
    @Test
    public void testGetTypePerformance()
    {
        final int millis = 100;
        long count = 0;
        Duration d = new Duration();
        TestBean bean = BeanFactory.newBean( ConcreteTestBean.class );
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            BeanFactory.getType( bean.getClass() );
        }
        LOG.info( "Getting the type of compiled beans at:" 
                  + Platform.NEWLINE + ( count ) / d.getElapsedMillis() + "K per second." );

        count = 0;
        d = new Duration();
        bean = BeanFactory.newBean( ProxyOnlyTestBean.class );
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            BeanFactory.getType( bean.getClass() );
        }
        LOG.info( "Getting the type of reflective beans at:" 
                  + Platform.NEWLINE + ( count ) / d.getElapsedMillis() + "K per second." );

        count = 0;
        d = new Duration();
        bean = BeanFactory.newBean( ProxyableWithConcreteImplementationTestBean.class );
        while ( millis > d.getElapsedMillis() )
        {
            ++count;
            BeanFactory.getType( bean.getClass() );
        }
        LOG.info( "Getting the type of reflective beans with concrete implementations at:" 
                  + Platform.NEWLINE + ( count ) / d.getElapsedMillis() + "K per second." );
    }
    
    
    private final static Logger LOG = Logger.getLogger( BeanFactory_Test.class );
}
