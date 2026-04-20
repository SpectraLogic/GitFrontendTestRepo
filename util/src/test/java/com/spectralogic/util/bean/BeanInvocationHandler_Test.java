/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.DefaultLongValue;
import com.spectralogic.util.bean.lang.DefaultStringValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanInvocationHandler_Test 
{
    @Test
    public void testConstructorNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanInvocationHandler( null, new HashMap< String, Object >() );
            }
        } );
    }
    
    
    @Test
    public void testConstructorClassThatIsNotSimpleBeanSafeToProxyNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanInvocationHandler( NonSimpleBeanSafeToProxy.class, new HashMap< String, Object >() );
            }
        } );
    }
    
    
    @Test
    public void testConstructorNullInitialPropertiesMapNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanInvocationHandler( TestBean.class, null );
            }
        } );
    }
    
    
    interface NonSimpleBeanSafeToProxy
    {
        // empty
    }
    
    
    @Test
    public void testBeanSetterMethodThatReturnsSelfReturnsSelfBasic()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertTrue(bean == bean.setStringProp( "aa" ), "Shoulda returned self.");
        assertEquals("aa", bean.getStringProp(), "Shoulda set value.");
    }
    
    
    @Test
    public void testBeanSetterMethodThatReturnsSelfReturnsSelfWithInheritence()
    {
        final TestBean bean = BeanFactory.newBean( NonStandardBean.class );
        assertTrue(bean == bean.setStringProp( "aa" ), "Shoulda returned self.");
        assertEquals("aa", bean.getStringProp(), "Shoulda set value.");
    }
    
    
    @Test
    public void testInitialPropertiesMapPrePopulatesCorrectly()
    {
        final TestBean bean = BeanFactory.newBean( 
                TestBean.class,
                CollectionFactory.< String, Object >toMap( TestBean.STRING_PROP, "test" ) );
        assertEquals("test", bean.getStringProp(), "Shoulda initialized value to test.");
        bean.setStringProp( "hi" );
        assertEquals("hi", bean.getStringProp(), "Shoulda set value.");
    }
    
    
    @Test
    public void testGetAndSetStringPropertyWorks()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(null, bean.getStringProp(), "Shoulda initialized value to null.");
        bean.setStringProp( "hi" );
        assertEquals("hi", bean.getStringProp(), "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetPrimitiveIntegerPropertyWorks()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(0,  bean.getIntProp(), "Shoulda initialized value to 0.");
        bean.setIntProp( 22 );
        assertEquals(22,  bean.getIntProp(), "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetPrimitiveLongPropertyWorks()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(0,  bean.getLongProp(), "Shoulda initialized value to 0.");
        bean.setLongProp( 22 );
        assertEquals(22,  bean.getLongProp(), "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetPrimitiveBooleanPropertyWorks()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(false, bean.getBooleanProp(), "Shoulda initialized value to false.");
        bean.setBooleanProp( true );
        assertEquals(true, bean.getBooleanProp(), "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetIntegerPropertyWorks()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(null,  bean.getObjectIntProp(), "Shoulda initialized value to null.");
        bean.setObjectIntProp( Integer.valueOf( 22 ) );
        assertEquals(Integer.valueOf( 22 ),
                bean.getObjectIntProp(),
                "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetLongPropertyWorks()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(null,  bean.getObjectLongProp(), "Shoulda initialized value to null.");
        bean.setObjectLongProp( Long.valueOf( 22 ) );
        assertEquals(Long.valueOf( 22 ),
                bean.getObjectLongProp(),
                "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetBooleanPropertyWorks()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(null, bean.getObjectBooleanProp(), "Shoulda initialized value to null.");
        bean.setObjectBooleanProp( Boolean.FALSE );
        assertEquals(Boolean.FALSE, bean.getObjectBooleanProp(), "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetNestedBeanPropertyWorks()
    {
        final TestBean nestedBean = BeanFactory.newBean( TestBean.class );
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        assertEquals(null, bean.getNestedBean(), "Shoulda initialized value to null.");
        bean.setNestedBean( nestedBean );
        assertEquals(nestedBean, bean.getNestedBean(), "Shoulda set value.");
    }
    

    @Test
    public void testGetAndSetSetPropertyMakesDefensiveCopies()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        final Set< String > set = CollectionFactory.toSet( "a", "b", "c" );
        bean.setSetProp( set );
        assertEquals(3,  bean.getSetProp().size(), "Shoulda set the set.");
        set.add( "d" );
        assertEquals(3,  bean.getSetProp().size(), "Shoulda defensively copied the set.");

        bean.getSetProp().add( "d" );
        assertEquals(3,  bean.getSetProp().size(), "Shoulda defensively copied the set.");
    }
    

    @Test
    public void testGetAndSetListPropertyMakesDefensiveCopies()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        final List< String > list = CollectionFactory.toList( "a", "b", "c" );
        bean.setListProp( list );
        assertEquals(3,  bean.getListProp().size(), "Shoulda set the list.");
        list.add( "d" );
        assertEquals(3,  bean.getListProp().size(), "Shoulda defensively copied the list.");

        bean.getListProp().add( "d" );
        assertEquals(3,  bean.getListProp().size(), "Shoulda defensively copied the list.");
    }
    

    @Test
    public void testGetAndSetArrayPropertyMakesDefensiveCopies()
    {
        final TestBean bean = BeanFactory.newBean( TestBean.class );
        final String [] array = { "a", "b", "c" };
        bean.setArrayProp( array );
        assertEquals(3,  bean.getArrayProp().length, "Shoulda set the array.");
        assertEquals("a", bean.getArrayProp()[ 0 ], "Shoulda defensively copied the array.");
        array[ 0 ] = "d";
        assertEquals("a", bean.getArrayProp()[ 0 ], "Shoulda defensively copied the array.");

        bean.getArrayProp()[ 0 ] = "d";
        assertEquals("a", bean.getArrayProp()[ 0 ], "Shoulda defensively copied the array.");
    }
    
    
    @Test
    public void testCustomDefaultsRespectedAtInitializationTime()
    {
        BeanFactory.newBean( MyTestBean.class );
        TestUtil.sleep( 1 );
        final Date date1 = new Date();
        final MyTestBean bean = BeanFactory.newBean( MyTestBean.class );
        final Date date2 = new Date();

        assertEquals(-2,  bean.getIntWithCustomDefault(), "Shoulda loaded custom default.");
        assertEquals(22,  bean.getLongWithCustomDefault().longValue(), "Shoulda loaded custom default.");
        assertEquals(true, bean.isBooleanWithCustomDefault(), "Shoulda loaded custom default.");
        assertTrue(bean.getDateWithCustomDefault().getTime() >= date1.getTime(), "Shoulda loaded custom default.");
        assertTrue(bean.getDateWithCustomDefault().getTime() <= date2.getTime(), "Shoulda loaded custom default.");
        assertEquals("abc", bean.getStringWithCustomDefault(), "Shoulda loaded custom default.");
        assertEquals(Gender.MALE, bean.getEnumWithCustomDefault(), "Shoulda loaded custom default.");
    }
    
    
    @Test
    public void testCustomDefaultsIgnoredWhenPropertyValuesExplicitlyProvidedAtInitializationTime()
    {
        final Map< String, Object > initialBeanPropertyValues = new HashMap<>();
        initialBeanPropertyValues.put( MyTestBean.LONG_WITH_CUSTOM_DEFAULT, Long.valueOf( 333 ) );
        final MyTestBean bean = BeanFactory.newBean( MyTestBean.class, initialBeanPropertyValues );

        assertEquals(-2,  bean.getIntWithCustomDefault(), "Shoulda loaded custom default.");
        assertEquals(333,  bean.getLongWithCustomDefault().longValue(), "Shoulda loaded custom default.");
        assertEquals(true, bean.isBooleanWithCustomDefault(), "Shoulda loaded custom default.");
    }
    
    
    @Test
    public void testIncorrectDefaultValueAnnotationsCaught()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanFactory.newBean( IncorrectDefaultValueSpecTestBean.class );
            }
        } );
    }
    
    
    @Test
    public void testEqualsReturnsTrueIffProxiesAreEqual()
    {
        final TestBean bean1 = BeanFactory.newBean( TestBean.class );
        final TestBean bean2 = BeanFactory.newBean( TestBean.class );
        assertTrue(bean1.equals( bean1 ), "Beans were the same.");
        assertFalse(
                bean1.equals( bean2 ),
                "Beans were not the same."
                 );
    }
    
    
    @Test
    public void testHashCodeDoesNotBlowUp()
    {
        BeanFactory.newBean( TestBean.class ).hashCode();
    }
    
    
    @Test
    public void testToStringReturnsNonNullSanitizedRepresentation()
    {
        final MyTestBean bean = BeanFactory.newBean( MyTestBean.class );
        bean.setStringProp( "abcd" );
        bean.setSecret( "efg" );
        assertTrue(bean.toString().contains( "abcd" ), "Shoulda returned something useful.");
        assertFalse(
                bean.toString().contains( "efg" ),
                "Shoulda concealed secret attributes."
                 );
    }
    
    
    public interface MyTestBean extends TestBean
    {
        String SECRET = "secret";
        
        @Secret
        String getSecret();
        
        void setSecret( final String value );
        
        
        String INT_WITH_CUSTOM_DEFAULT = "intWithCustomDefault";
        
        @DefaultIntegerValue( -2 )
        int getIntWithCustomDefault();
        
        
        String LONG_WITH_CUSTOM_DEFAULT = "longWithCustomDefault";
        
        @DefaultLongValue( 22 )
        Long getLongWithCustomDefault();
        
        
        String BOOLEAN_WITH_CUSTOM_DEFAULT = "booleanWithCustomDefault";
        
        @DefaultBooleanValue( true )
        boolean isBooleanWithCustomDefault();
        
        
        String DATE_WITH_CUSTOM_DEFAULT = "dateWithCustomDefault";
        
        @DefaultToCurrentDate
        Date getDateWithCustomDefault();
        
        
        String STRING_WITH_CUSTOM_DEFAULT = "stringWithCustomDefault";
        
        @DefaultStringValue( "abc" )
        String getStringWithCustomDefault();
        
        
        String ENUM_WITH_CUSTOM_DEFAULT = "enumWithCustomDefault";
        
        @DefaultEnumValue( "MALE" )
        Gender getEnumWithCustomDefault();
    } // end inner class def
    
    
    public interface IncorrectDefaultValueSpecTestBean extends MyTestBean
    {
        @DefaultToCurrentDate
        @DefaultEnumValue( "MALE" )
        Gender getEnumWithCustomDefault();
    }
    
    
    public enum Gender
    {
        MALE,
        FEMALE
    }
    
    
    @Test
    public void testBeanMethodInvocationHandlerRespectedAsTheInvocationHandlerWhenSpecified()
    {
        MyInvocationHandler.s_callCount = 0;
        final NonStandardBean bean = BeanFactory.newBean( NonStandardBean.class );
        bean.toString();
        bean.setStringProp( "a" );
        assertEquals(0,  MyInvocationHandler.s_callCount, "Should notta made any calls on the special invocation handler yet.");

        bean.setLongProp( 11 );
        assertEquals(1,  MyInvocationHandler.s_callCount, "Shoulda called special invocation handler for setter.");

        assertEquals("a", bean.getStringProp(), "Regular bean prop should notta been affected by override.");
        assertEquals(0,  bean.getLongProp(), "Overriden bean prop didn't get its value updated.");
    }
    
    
    final static class MyInvocationHandler implements InvocationHandler
    {
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            s_callCount += 1;
            return null;
        }
        
        private static int s_callCount;
    }
    
    
    private interface NonStandardBean extends TestBean
    {
        @BeanMethodInvocationHandler( MyInvocationHandler.class )
        void setLongProp( final long value );
    }
}
