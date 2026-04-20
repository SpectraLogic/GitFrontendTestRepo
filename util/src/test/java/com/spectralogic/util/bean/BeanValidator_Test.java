/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanValidator_Test 
{
    @Test
    public void testValidateValidBeanSuccessfully()
    {
        BeanValidator.test( ValidBean.class );
    }
    
   
    public interface ValidBean extends SimpleBeanSafeToProxy
    {
        String NAME = "name";
        
        String getName();
        
        void setName( final String name );
        
        
        String CREATION_DATE = "creationDate";
        
        String getCreationDate();
        
        void setCreationDate( final String value );
        
        
        String ID = "id";
        
        UUID getId();
        
        void setId( final UUID id );
        

        String COMMON_PREFIXES = "commonPrefixes";

        @CustomMarshaledName( 
                value = "Prefix", 
                collectionValue = "CommonPrefixes", 
                collectionValueRenderingMode = CollectionNameRenderingMode.BLOCK_FOR_EVERY_ELEMENT )
        List< String > getCommonPrefixes();
        
        void setCommonPrefixes( final List< String > value );
        

        int DEFAULT_MAX_KEYS = 1000;

        String MAX_KEYS = "maxKeys";
                
        int getMaxKeys();
        
        void setMaxKeys( final int value );
    } // end inner class def
    
    
    @Test
    public void testInvalidBeanMissingPropertyDescriptionThrowsRuntimeException()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanValidator.test( InvalidBeanMissingPropertyDescription.class );
            }
        } );
    }
    
    
    public interface InvalidBeanMissingPropertyDescription extends SimpleBeanSafeToProxy
    {
        String getName();
        
        void setName( final String name );
    } // end inner class def
    
    
    @Test
    public void testInvalidBeanReaderReturnTypeDoesNotMatchWriterParamTypeThrowsRuntimeException()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanValidator.test( InvalidBeanReaderReturnTypeDoesNotMatchWriterParamType.class );
            }
        } );
    }
    
    
    public interface InvalidBeanReaderReturnTypeDoesNotMatchWriterParamType extends SimpleBeanSafeToProxy
    {
        String PROPERTY = "property";

        String getProperty();
        
        void setProperty( final Integer value );
    } // end inner class def
 
    
    @Test
    public void testInvalidBeanDeclaredFinalStaticStringsAreUniqueThrowsRuntimeException()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanValidator.test( InvalidBeanDeclaredFinalStaticStringsAreUnique.class );
            }
        } );
    }
    
    
    public interface InvalidBeanDeclaredFinalStaticStringsAreUnique extends SimpleBeanSafeToProxy
    {
        String PROPERTY = "property";

        String getProperty();
        
        void setProperty( final String value );
        
        
        String OTHER_PROPERTY = "property";

        String getOtherProperty();
        
        void setOtherProperty( final String value );
    } // end inner class def
    
    
    @Test
    public void testInvalidBeanDeclaredFinalStaticStringsDefineAllBeanPropsThrowsRuntimeException()
    {
        TestUtil.assertThrows( null,   RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanValidator.test( InvalidBeanDeclaredFinalStaticStringsDefineAllBeanProps.class );
            }
        } );
    }
    
    
    public interface InvalidBeanDeclaredFinalStaticStringsDefineAllBeanProps extends SimpleBeanSafeToProxy
    {
        String OTHER_PROPERTY = "property";

        String getOtherProperty();
        
        void setOtherProperty( final String value );
    } // end inner class def
    
    
    @Test
    public void testValidBeanConcreteImplementation()
    {
        BeanValidator.test( ValidBeanConcreteImpl.class );
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanValidator.test( ValidBeanConcrete.class );
            }
        } );
    }
    
    
    @ConcreteImplementation( ValidBeanConcreteImpl.class )
    public interface ValidBeanConcrete extends SimpleBeanSafeToProxy
    {
        String SIZE = "size";

        int getSize();
        
        ValidBeanConcrete setSize( final int value );
    } // end inner class def
    
    
    final class ValidBeanConcreteImpl extends BaseMarshalable implements ValidBeanConcrete
    {
        public int getSize()
        {
            return m_size;
        }
        
        @Override
        public ValidBeanConcrete setSize( int value )
        {
            m_size = value;
            return this;
        }
        
        private volatile int m_size;
    } // end inner class def
    
    
    @Test
    public void testInvalidPublicConcreteImplBeanThrowsRuntimeException()
    {
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanValidator.test( InvalidPublicConcreteConcrete.class );
            }
        } );
    }
    
    
    @ConcreteImplementation( InvalidPublicConcreteConcreteImpl.class )
    public interface InvalidPublicConcreteConcrete extends SimpleBeanSafeToProxy
    {
        String SIZE = "size";

        int getSize();
        
        InvalidPublicConcreteConcrete setSize( final int value );
    } // end inner class def
    
    
    public final class InvalidPublicConcreteConcreteImpl extends BaseMarshalable 
        implements InvalidPublicConcreteConcrete
    {
        public int getSize()
        {
            return m_size;
        }
        
        @Override
        public InvalidPublicConcreteConcrete setSize( int value )
        {
            m_size = value;
            return this;
        }
        
        private volatile int m_size;
    } // end inner class def
    
    
    @Test
    public void testInvalidNotFinalConcreteImplBeanThrowsRuntimeException()
    {
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanValidator.test( InvalidNotFinalConcreteBean.class );
            }
        } );
    }
    
    
    @ConcreteImplementation( InvalidNotFinalConcreteBeanImpl.class )
    public interface InvalidNotFinalConcreteBean extends SimpleBeanSafeToProxy
    {
        String SIZE = "size";

        int getSize();
        
        InvalidNotFinalConcreteBean setSize( final int value );
    } // end inner class def
    
    
    class InvalidNotFinalConcreteBeanImpl extends BaseMarshalable implements InvalidNotFinalConcreteBean
    {
        public int getSize()
        {
            return m_size;
        }
        
        @Override
        public InvalidNotFinalConcreteBean setSize( int value )
        {
            m_size = value;
            return this;
        }
        
        private volatile int m_size;
    } // end inner class def
}
