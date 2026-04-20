/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestConcreteBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanUtils_Test 
{
    @Test
    public void testGetPropertyNamesNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.getPropertyNames( null );
            }
        } );
    }
    
    
    @Test
    public void testGetPropertyNamesReturnsBeanPropertyNames()
    {
        TestUtil.assertSame( 
                CollectionFactory.toSet( 
                        TestBean.INT_PROP,
                        TestBean.INT_PROP,
                        TestBean.STRING_PROP,
                        TestBean.OBJECT_LONG_PROP,
                        TestBean.ARRAY_PROP,
                        TestBean.SET_PROP,
                        TestBean.BOOLEAN_PROP,
                        TestBean.LIST_PROP,
                        TestBean.OBJECT_BOOLEAN_PROP,
                        TestBean.OBJECT_INT_PROP,
                        TestBean.LONG_PROP,
                        TestBean.NESTED_BEAN,
                        NotificationPayload.NOTIFICATION_GENERATION_DATE ), 
                BeanUtils.getPropertyNames( TestBean.class ) );
        
        TestUtil.assertSame( 
                CollectionFactory.toSet( 
                        "intProp",
                        "stringProp" ), 
                BeanUtils.getPropertyNames( TestConcreteBean.class ) );
    }
    
    
    @Test
    public void testHasPropertyNameNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.hasPropertyName( null, "oops" );
            }
        } );
    }
    
    
    @Test
    public void testHasPropertyNameNullPropertyNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.hasPropertyName( TestBean.class, null );
            }
        } );
    }
    
    
    @Test
    public void testHasPropertyNameReturnsTrueIfHasPropertyName()
    {
        assertTrue(BeanUtils.hasPropertyName( TestBean.class, TestBean.INT_PROP ), "Shoulda reported had property.");
    }
    
    
    @Test
    public void testHasPropertyNameReturnsTrueIfHasWriterPropertyName()
    {
        assertTrue(BeanUtils.hasPropertyName( MockBean6.class, MockBean6.WRITER_ONLY_PROP ), "Shoulda reported had property.");
    }
    
    
    @Test
    public void testHasPropertyNameReturnsFalseIfDoesNotHavePropertyName()
    {
        assertFalse(
                BeanUtils.hasPropertyName( TestBean.class, "oops" ),
                "Shoulda reported didn't have property."
                 );
    }
    
    
    @Test
    public void testGetPropertyNameNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.getPropertyName( 
                        null,
                        ReflectUtil.getMethod( TestBean.class, "getIntProp" ) );
            }
        } );
    }
    
    
    @Test
    public void testGetPropertyNameNullMethodNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.getPropertyName( 
                        TestBean.class,
                        null );
            }
        } );
    }
    
    
    @Test
    public void testGetPropertyNameThatDoesNotExistReturnsNull()
    {
        assertEquals(null, BeanUtils.getPropertyName(
                        TestBean.class,
                        ReflectUtil.getMethod( Object.class, "hashCode" ) ), "Shoulda returned property name for getter.");
    }
    
    
    @Test
    public void testGetPropertyNameThatIsReaderOnlyReturnsPropertyName()
    {
        assertEquals(MockBean6.READER_ONLY_PROP, BeanUtils.getPropertyName(
                        MockBean6.class,
                        ReflectUtil.getMethod( MockBean6.class, "getReaderOnlyProp" ) ), "Shoulda returned property name for getter.");
    }
    
    
    @Test
    public void testGetPropertyNameThatIsWriterOnlyReturnsPropertyName()
    {
        assertEquals(MockBean6.WRITER_ONLY_PROP, BeanUtils.getPropertyName(
                        MockBean6.class,
                        ReflectUtil.getMethod( MockBean6.class, "setWriterOnlyProp" ) ), "Shoulda returned property name for getter.");
    }
    
    
    @Test
    public void testGetPropertyNameReturnsPropertyName()
    {
        assertEquals(TestBean.INT_PROP, BeanUtils.getPropertyName(
                        TestBean.class,
                        ReflectUtil.getMethod( TestBean.class, "getIntProp" ) ), "Shoulda returned property name for getter.");
    }
    
    
    @Test
    public void testIsReaderNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.isReader( 
                        null,
                        ReflectUtil.getMethod( TestBean.class, "getIntProp" ) );
            }
        } );
    }
    
    
    @Test
    public void testIsReaderNullMethodNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.isReader( 
                        TestBean.class,
                        null );
            }
        } );
    }
    
    
    @Test
    public void testIsReaderReturnsTrueIffItsAnActualReader()
    {
        assertEquals(false, BeanUtils.isReader(
                        MockBean.class,
                        ReflectUtil.getMethod( MockBean.class, "someNonBeanMethod" ) ), "Method wasn't for a bean method.");
        assertEquals(false, BeanUtils.isReader(
                        MockBean.class,
                        ReflectUtil.getMethod( MockBean.class, "setIntProp" ) ), "Method was a setter - not a getter.");
        assertEquals(true, BeanUtils.isReader(
                        MockBean.class,
                        ReflectUtil.getMethod( MockBean.class, "getIntProp" ) ), "Method was a getter.");
    }
    
    
    @Test
    public void testIsWriterNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.isWriter( 
                        null,
                        ReflectUtil.getMethod( TestBean.class, "getIntProp" ) );
            }
        } );
    }
    
    
    @Test
    public void testIsWriterNullMethodNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.isWriter( 
                        TestBean.class,
                        null );
            }
        } );
    }
    
    
    @Test
    public void testIsWriterReturnsTrueIffItsAnActualWriter()
    {
        assertEquals(false, BeanUtils.isWriter(
                        MockBean.class,
                        ReflectUtil.getMethod( MockBean.class, "someNonBeanMethod" ) ), "Method wasn't for a bean method.");
        assertEquals(true, BeanUtils.isWriter(
                        MockBean.class,
                        ReflectUtil.getMethod( MockBean.class, "setIntProp" ) ), "Method was a setter.");
        assertEquals(false, BeanUtils.isWriter(
                        MockBean.class,
                        ReflectUtil.getMethod( MockBean.class, "getIntProp" ) ), "Method was a getter.");
    }
    
    
    @Test
    public void testGetReaderNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.getReader( 
                        null,
                        TestBean.INT_PROP );
            }
        } );
    }
    
    
    @Test
    public void testGetReaderNullPropNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.getReader( 
                        TestBean.class,
                        null );
            }
        } );
    }
    
    
    @Test
    public void testGetReaderReturnsNonNullIffExists()
    {
        assertEquals(null, BeanUtils.getReader(
                        MockBean.class,
                        "nonBeanMethod" ), "Method wasn't for a bean method.");
        assertEquals( ReflectUtil.getMethod( MockBean.class, "getIntProp" ),
                BeanUtils.getReader(
                        MockBean.class,
                        TestBean.INT_PROP ),
                "Shoulda reported the bean method.");
    }
    
    
    @Test
    public void testVerifyReaderReturnTypeNullBeanTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.verifyReaderReturnType( null, TestBean.INT_PROP, Integer.class );
            }
        } );
    }
    
    
    @Test
    public void testVerifyReaderReturnTypeNullBeanPropertyNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.verifyReaderReturnType( TestBean.class, null, Integer.class );
            }
        } );
    }
    
    
    @Test
    public void testVerifyReaderReturnTypeNullExpectedReaderReturnTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.verifyReaderReturnType( TestBean.class, TestBean.INT_PROP, null );
            }
        } );
    }
    
    
    @Test
    public void testVerifyReaderReturnTypePropertyDoesNotHaveReaderNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.verifyReaderReturnType( TestBean.class, "invalid", String.class );
            }
        } );
    }
    
    
    @Test
    public void testVerifyReaderReturnTypeReturnTypeNotCorrectNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.verifyReaderReturnType( TestBean.class, TestBean.INT_PROP, String.class );
            }
        } );
    }
    
    
    @Test
    public void testVerifyReaderReturnTypePassesWhenTypeIsAsExpected()
    {
        BeanUtils.verifyReaderReturnType( TestBean.class, TestBean.INT_PROP, Integer.class );
    }
    
    
    @Test
    public void testToMapNullBeansNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.toMap( null );
            }
        } );
    }
    
    
    @Test
    public void testToMapForEmptySetReturnsEmptyMapSuccessfully()
    {        
        final Set< MockBean2 > beans = new HashSet<>();
        assertTrue(BeanUtils.toMap( beans, "someProperty" ).isEmpty(), (String) null);
    }
    
    
    @Test
    public void testToMapWhenAtLeastOneBeanHasNullIdNotAllowed()
    {
        final MockBean7 bean1 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean2 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean3 = BeanFactory.newBean( MockBean7.class );
        
        final Set< MockBean7 > beans = new HashSet<>();
        beans.add( bean1 );
        beans.add( bean2 );
        beans.add( bean3 );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.toMap( beans );
            }
        } );
    }
    
    
    @Test
    public void testToMapWhenAtLeastOneBeanHasNullIdDoesSo()
    {
        final MockBean7 bean1 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean2 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean3 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        
        final Set< MockBean7 > beans = new HashSet<>();
        beans.add( bean1 );
        beans.add( bean2 );
        beans.add( bean3 );
        
        final Map< UUID, MockBean7 > map = BeanUtils.toMap( beans );
        assertEquals(3,  map.size(), "Every bean shoulda been in the map generated.");
        assertEquals(bean1, map.get( bean1.getId() ), "Every bean shoulda been in the map generated.");
        assertEquals(bean2, map.get( bean2.getId() ), "Every bean shoulda been in the map generated.");
        assertEquals(bean3, map.get( bean3.getId() ), "Every bean shoulda been in the map generated.");
    }
    
    
    @Test
    public void testToCustomMapNullBeansNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.toMap( null, TestBean.LONG_PROP );
            }
        } );
    }
    
    
    @Test
    public void testToCustomMapWhenAtLeastOneBeanHasNullIdNotAllowed()
    {
        final MockBean7 bean1 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean2 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean3 = BeanFactory.newBean( MockBean7.class );
        
        final Set< MockBean7 > beans = new HashSet<>();
        beans.add( bean1 );
        beans.add( bean2 );
        beans.add( bean3 );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.toMap( beans, TestBean.LONG_PROP );
            }
        } );
    }
    
    
    @Test
    public void testToCustomMapWhenAtLeastOneBeanHasNullIdDoesSo()
    {
        final MockBean7 bean1 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean2 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean3 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        
        final Set< MockBean7 > beans = new HashSet<>();
        beans.add( bean1 );
        beans.add( bean2 );
        beans.add( bean3 );
        
        bean2.setObjectLongProp( Long.valueOf( 33 ) );
        
        final Map< UUID, Long > map = BeanUtils.toMap( beans, TestBean.OBJECT_LONG_PROP );
        assertEquals(3,  map.size(), "Every bean shoulda been in the map generated.");
        assertEquals(null,
                map.get(bean1.getId()),
                "Every bean shoulda been in the map generated.");
        assertEquals(Long.valueOf( 33 ),
                map.get( bean2.getId()),
                "Every bean shoulda been in the map generated.");
        assertEquals(null,
                map.get(bean3.getId()),
                "Every bean shoulda been in the map generated.");
    }
    
    
    @Test
    public void testExtractBeanPropertyValuesNullBeansNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.extractPropertyValues( null, TestBean.LONG_PROP );
            }
        } );
    }
    
    
    @Test
    public void testExtractBeanPropertyValuesBeansContainsNullNotAllowed()
    {
        final MockBean7 bean1 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean2 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean3 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        
        final Set< MockBean7 > beans = new HashSet<>();
        beans.add( bean1 );
        beans.add( bean2 );
        beans.add( null );
        beans.add( bean3 );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.extractPropertyValues( beans, TestBean.LONG_PROP );
            }
        } );
    }
    
    
    @Test
    public void testExtractBeanPropertyValuesEmptyBeansAllowed()
    {
        assertEquals(0,  BeanUtils.extractPropertyValues(new HashSet<>(), TestBean.LONG_PROP).size(), "Shoulda returned empty set.");
    }
    
    
    @Test
    public void testExtractBeanPropertyValuesWhenAtLeastOneBeanHasNullPropertyValueDoesSo()
    {
        final MockBean7 bean1 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean2 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        final MockBean7 bean3 = (MockBean7)BeanFactory.newBean( MockBean7.class ).setId( UUID.randomUUID() );
        
        final Set< MockBean7 > beans = new HashSet<>();
        beans.add( bean1 );
        beans.add( bean2 );
        beans.add( bean3 );
        
        bean2.setObjectLongProp( Long.valueOf( 33 ) );
        
        final Set< Long > map = BeanUtils.extractPropertyValues( beans, TestBean.OBJECT_LONG_PROP );
        assertEquals(2,  map.size(), "Every bean value shoulda been added to set.");
        assertTrue(map.contains( null ), "Every bean value shoulda been added to set.");
        assertTrue(map.contains( Long.valueOf( 33 ) ), "Every bean value shoulda been added to set.");
    }
    
    
    @Test
    public void testGetWriterNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.getWriter( 
                        null,
                        TestBean.INT_PROP );
            }
        } );
    }
    
    
    @Test
    public void testGetWriterNullPropNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                BeanUtils.getWriter( 
                        TestBean.class,
                        null );
            }
        } );
    }
    
    
    @Test
    public void testGetWriterReturnsWriterIffExists()
    {
        assertEquals(null, BeanUtils.getWriter(
                        MockBean.class,
                        "someNonBeanMethod" ), "Method wasn't for a bean method.");
        assertEquals( ReflectUtil.getMethod( MockBean.class, "setIntProp" ),
                BeanUtils.getWriter(
                        MockBean.class,
                        TestBean.INT_PROP ),
                "Method was a setter.");
    }
    
    
    @Test
    public void testSortTypeThatHasNoSortAnnotationsReturnsSortedSet()
    {
        final Set< Identifiable > original = new HashSet<>();
        assertFalse(
                original == BeanUtils.sort( original ),
                "Should notta been same instance."
                 );
    }
    
    
    @Test
    public void testSortDoesSoBasic()
    {
        final Set< MockBean2 > beans = new HashSet<>();
        beans.add( BeanFactory.newBean( MockBean2.class ).setSortProp( "a" ) );
        beans.add( BeanFactory.newBean( MockBean2.class ).setSortProp( "c" ) );
        beans.add( BeanFactory.newBean( MockBean2.class ).setSortProp( "b" ) );

        int index = -1;
        final List< MockBean2 > sortedBeans = new ArrayList<>( BeanUtils.sort( beans ) );
        assertEquals("a", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("b", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
    }
    
    
    @Test
    public void testSortDoesSoWhenSortPropertiesAreAcrossNestedTypesCaseA()
    {
        final Set< MockBean3 > beans = new HashSet<>();
        beans.add( (MockBean3)BeanFactory.newBean( MockBean3.class )
                .setSecondSortProp( "e" ).setSortProp( "a" ) );
        beans.add( (MockBean3)BeanFactory.newBean( MockBean3.class )
                .setSecondSortProp( "d" ).setSortProp( "c" ) );
        beans.add( (MockBean3)BeanFactory.newBean( MockBean3.class )
                .setSecondSortProp( "c" ).setSortProp( "c" ) );
        beans.add( (MockBean3)BeanFactory.newBean( MockBean3.class )
                .setSecondSortProp( "b" ).setSortProp( "c" ) );
        beans.add( (MockBean3)BeanFactory.newBean( MockBean3.class )
                .setSecondSortProp( "a" ).setSortProp( "b" ) );
        
        int index = -1;
        final List< MockBean3 > sortedBeans = new ArrayList<>( BeanUtils.sort( beans ) );
        assertEquals("a", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("e", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("b", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("a", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("b", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("d", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
    }
    
    
    @Test
    public void testSortDoesSoWhenSortPropertiesAreAcrossNestedTypesCaseB()
    {
        final Set< MockBean4 > beans = new HashSet<>();
        beans.add( (MockBean4)BeanFactory.newBean( MockBean4.class )
                .setSecondSortProp( "e" ).setSortProp( "a" ) );
        beans.add( (MockBean4)BeanFactory.newBean( MockBean4.class )
                .setSecondSortProp( "d" ).setSortProp( "c" ) );
        beans.add( (MockBean4)BeanFactory.newBean( MockBean4.class )
                .setSecondSortProp( "c" ).setSortProp( "c" ) );
        beans.add( (MockBean4)BeanFactory.newBean( MockBean4.class )
                .setSecondSortProp( "b" ).setSortProp( "c" ) );
        beans.add( (MockBean4)BeanFactory.newBean( MockBean4.class )
                .setSecondSortProp( "a" ).setSortProp( "b" ) );
        
        int index = -1;
        final List< MockBean4 > sortedBeans = new ArrayList<>( BeanUtils.sort( beans ) );
        assertEquals("b", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("a", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("b", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("c", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("d", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
        assertEquals("a", sortedBeans.get( ++index ).getSortProp(), "Shoulda sorted beans.");
        assertEquals("e", sortedBeans.get( index ).getSecondSortProp(), "Shoulda sorted beans.");
    }
    
    
    interface MockBean extends TestBean
    {
        String someNonBeanMethod();
    }
    
    
    interface MockBean2 extends TestBean, Identifiable
    {
        String SORT_PROP = "sortProp";
        
        @SortBy( 2 )
        String getSortProp();
        
        MockBean2 setSortProp( final String value );
    }
    
    
    interface MockBean3 extends MockBean2
    {
        String SECOND_SORT_PROP = "secondSortProp";
        
        @SortBy( 44 )
        String getSecondSortProp();
        
        MockBean3 setSecondSortProp( final String value );
    }
    
    
    interface MockBean4 extends MockBean2
    {
        String SECOND_SORT_PROP = "secondSortProp";
        
        @SortBy( 1 )
        String getSecondSortProp();
        
        MockBean4 setSecondSortProp( final String value );
    }
    
    
    interface MockBean5 extends TestBean
    {
        String SORT_PROP = "sortProp";
        
        @SortBy( 2 )
        String getSortProp();
        
        MockBean5 setSortProp( final String value );
    }
    
    
    interface MockBean6 extends TestBean
    {
        String READER_ONLY_PROP = "readerOnlyProp";
        
        String getReaderOnlyProp();
        
        
        String WRITER_ONLY_PROP = "writerOnlyProp";
        
        void setWriterOnlyProp( final String value );
    }
    
    
    interface MockBean7 extends TestBean, Identifiable
    {
        // empty
    }
}
