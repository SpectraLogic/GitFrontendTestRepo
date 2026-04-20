/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanComparator.BeanPropertyComparisonSpecifiction;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanComparator_Test
{
    @Test
    public void testConstructorNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanComparator<>( null, TestBean.STRING_PROP );
            }
        } );
    }
    

    @Test
    public void testConstructorNullPropertyNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanComparator<>( TestBean.class, (String)null );
            }
        } );
    }
    

    @Test
    public void testNullComparatorResultsInObjectsBeingComparedUsingTheirComparableTypes()
    {
        final Comparator< TestBean > comparator =
                new BeanComparator<>( TestBean.class, TestBean.STRING_PROP );
        final List< TestBean > beans = new ArrayList<>();
        beans.add( getBean( "d" ) );
        beans.add( getBean( "b" ) );
        beans.add( getBean( null ) );
        beans.add( getBean( null ) );
        beans.add( getBean( "a" ) );
        beans.add( getBean( "b" ) );
        beans.add( getBean( "c" ) );
        
        Collections.sort( beans, comparator );
        int index = -1;
        assertEquals(null, beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(null, beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("a", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("b", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("b", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("c", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("d", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
    }
    

    @Test
    public void testMultipleComparisonSpecsResultsInObjectsBeingComparedUsingInNestedFashionWhenAscending()
    {
        final Comparator< TestBean > comparator = new BeanComparator<>( 
                TestBean.class, 
                new BeanPropertyComparisonSpecifiction( TestBean.STRING_PROP, Direction.ASCENDING, null ),
                new BeanPropertyComparisonSpecifiction( TestBean.INT_PROP, Direction.ASCENDING, null ) );
        final List< TestBean > beans = new ArrayList<>();
        beans.add( getBean( "d" ) );
        beans.add( getBean( "b", 22 ) );
        beans.add( getBean( null, 2 ) );
        beans.add( getBean( null, 1 ) );
        beans.add( getBean( "a" ) );
        beans.add( getBean( "b", 11 ) );
        beans.add( getBean( "c" ) );
        
        Collections.sort( beans, comparator );
        int index = -1;
        assertEquals(null, beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(1,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals(null, beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(2,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals("a", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("b", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(11,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals("b", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(22,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals("c", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("d", beans.get( ++index ).getStringProp(), "Shoulda sorted using comparator.");
    }
    

    @Test
    public void testMultipleComparisonSpecsResultsInObjectsBeingComparedUsingInNestedFashionWhenDescending()
    {
        final Comparator< TestBean > comparator = new BeanComparator<>( 
                TestBean.class, 
                new BeanPropertyComparisonSpecifiction( TestBean.STRING_PROP, Direction.DESCENDING, null ),
                new BeanPropertyComparisonSpecifiction( TestBean.INT_PROP, Direction.DESCENDING, null ) );
        final List< TestBean > beans = new ArrayList<>();
        beans.add( getBean( "d" ) );
        beans.add( getBean( "b", 22 ) );
        beans.add( getBean( null, 2 ) );
        beans.add( getBean( null, 1 ) );
        beans.add( getBean( "a" ) );
        beans.add( getBean( "b", 11 ) );
        beans.add( getBean( "c" ) );
        
        Collections.sort( beans, comparator );
        int index = beans.size();
        assertEquals(null, beans.get( --index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(1,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals(null, beans.get( --index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(2,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals("a", beans.get( --index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("b", beans.get( --index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(11,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals("b", beans.get( --index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals(22,  beans.get(index).getIntProp(), "Shoulda sorted using comparator.");
        assertEquals("c", beans.get( --index ).getStringProp(), "Shoulda sorted using comparator.");
        assertEquals("d", beans.get( --index ).getStringProp(), "Shoulda sorted using comparator.");
    }
    
    
    private TestBean getBean( final String stringPropValue )
    {
        final TestBean retval = BeanFactory.newBean( TestBean.class );
        retval.setStringProp( stringPropValue );
        return retval;
    }
    
    
    private TestBean getBean( final String stringPropValue, final int intPropValue )
    {
        final TestBean retval = getBean( stringPropValue );
        retval.setIntProp( intPropValue );
        return retval;
    }
}
