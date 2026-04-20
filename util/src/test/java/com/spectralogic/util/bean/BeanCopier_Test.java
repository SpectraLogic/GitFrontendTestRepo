/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanCopier_Test
{
    @Test
    public void testCopyNullSourceNotAllowed()
    {
        try
        {
            BeanCopier.copy( newPopulatedTestBean( null, null ), null );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull(
                    ex,
                    "Shoulda thrown exception." );
        }
    }
    

    @Test
    public void testCopyNullDestinationNotAllowed()
    {
        try
        {
            BeanCopier.copy( null, newPopulatedTestBean( null, null ) );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull(
                    ex,
                    "Shoulda thrown exception." );
        }
    }
    
    
    @Test
    public void testCopySrcIsDifferentTypeFromDestinationAllowed()
    {
        final TestBean source =
            BeanFactory.newBean( TestBean.class );
        final TestBean destination = 
            BeanFactory.newBean( ExtendedTestBean.class );

        BeanCopier.copy( destination, source );
    }
    

    @Test
    public void testCopyNullsWorks()
    {
        final TestBean source = 
            BeanFactory.newBean( TestBean.class );
        final TestBean destination = 
            BeanFactory.newBean( TestBean.class );
        
        BeanCopier.copy( destination, source );
        assertIdentical( source, destination );
    }
    
    
    @Test
    public void testCopyWhereBeanArrayNullnessMismatchNotAllowed()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        source.setReadWriteArrayProp( new TestBean [] { BeanFactory.newBean( TestBean.class ) } );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanCopier.copy( destination, source );
            }
        } );
    }
    
    
    @Test
    public void testCopyWhereBeanArrayLengthsMismatchNotAllowed()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        source.setReadWriteArrayProp( new TestBean [] { BeanFactory.newBean( TestBean.class ) } );
        destination.setReadWriteArrayProp( new TestBean [] {
                BeanFactory.newBean( TestBean.class ), BeanFactory.newBean( TestBean.class ) } );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                BeanCopier.copy( destination, source );
            }
        } );
    }
    
    
    @Test
    public void testCopyWhereNonBeanArrayLengthsMismatchAllowed()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        source.setStringArray( new String [] { "a", "b" } );
        
        BeanCopier.copy( destination, source );
        assertEquals(2,  destination.getStringArray().length, "Shoulda copied string array source to destination.");
    }
    

    @Test
    public void testCopyNonNullNonNestedPropsWorks()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        
        BeanCopier.copy( destination, source );
        assertIdentical( source, destination );
    }
    

    @Test
    public void testCopyReadWriteNestedPropNonNullToNonNullWorks()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        
        source.setReadWriteNestedBean( newPopulatedTestBean( null, null ) );
        destination.setReadWriteNestedBean( newPopulatedTestBean( null, null ) );
        
        BeanCopier.copy( destination, source );
        assertIdentical( source, destination );
    }
    
    
    @Test
    public void testCopyReadWriteNestedPropNullToNonNullWorks()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        
        source.setReadWriteNestedBean( null );
        destination.setReadWriteNestedBean( newPopulatedTestBean( null, null ) );
        
        BeanCopier.copy( destination, source );
        assertIdentical( source, destination );
    }
    
    
    @Test
    public void testCopyReadWriteNestedPropNonNullToNullNotAllowed()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        
        source.setReadWriteNestedBean( newPopulatedTestBean( null, null ) );
        destination.setReadWriteNestedBean( null );

        try
        {
            BeanCopier.copy( destination, source );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }
    }
    

    @Test
    public void testCopyReadWriteArrayNestedPropNonNullToNonNullWorks()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        
        final TestBean [] src = new TestBean[ 2 ];
        src[ 0 ] = newPopulatedTestBean( null, null );
        src[ 1 ] = newPopulatedTestBean( null, null );
        
        final TestBean [] dest = new TestBean[ 2 ];
        dest[ 0 ] = newPopulatedTestBean( null, null );
        dest[ 1 ] = newPopulatedTestBean( null, null );
        
        source.setReadWriteArrayProp( src );
        destination.setReadWriteArrayProp( dest );
        
        BeanCopier.copy( destination, source );
        assertIdentical( source, destination );
    }
    
    
    @Test
    public void testCopyReadWriteArrayNestedPropNullToNonNullWorks()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        
        final TestBean [] dest = new TestBean[ 2 ];
        dest[ 0 ] = newPopulatedTestBean( null, null );
        dest[ 1 ] = newPopulatedTestBean( null, null );
        
        source.setReadWriteArrayProp( null );
        destination.setReadWriteArrayProp( dest );
        
        BeanCopier.copy( destination, source );
        assertIdentical( source, destination );
    }
    
    
    @Test
    public void testCopyReadWriteArrayNestedPropNonNullToNullNotAllowed()
    {
        final TestBean source = newPopulatedTestBean( null, null );
        final TestBean destination = newPopulatedTestBean( null, null );
        
        final TestBean [] src = new TestBean[ 2 ];
        src[ 0 ] = newPopulatedTestBean( null, null );
        src[ 1 ] = newPopulatedTestBean( null, null );
        
        source.setReadWriteArrayProp( src );
        destination.setReadWriteArrayProp( null );

        try
        {
            BeanCopier.copy( destination, source );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown exception." );
        }
    }
    
    
    private TestBean newPopulatedTestBean( 
            final TestBean readOnlyNestedPropValue, 
            final TestBean[] readOnlyArrayNestedPropValue )
    {
        final Map< String, Object > initialPropValuesMap = new HashMap<>();
        initialPropValuesMap.put( TestBean.READ_ONLY_NESTED_BEAN, readOnlyNestedPropValue );
        initialPropValuesMap.put( TestBean.READ_ONLY_ARRAY_PROP, readOnlyArrayNestedPropValue );
        final TestBean retval = 
            BeanFactory.newBean( TestBean.class, initialPropValuesMap );
        
        final SecureRandom random = new SecureRandom();
        retval.setInt( random.nextInt() );
        retval.setInteger( Integer.valueOf( random.nextInt() ) );
        retval.setString( String.valueOf( random.nextInt() ) );
        retval.setReadWriteProp( String.valueOf( random.nextInt() ) );
        retval.setWriteOnlyProp( String.valueOf( random.nextInt() ) );
        
        return retval;
    }
    
    
    private void assertIdentical( final TestBean source, final TestBean destination )
    {
        if ( null == source || null == destination )
        {
            assertNull(source, "Destination null, but source not.");
            assertNull(destination, "Source null, but destination not.");
        }
        
        if ( null == source || null == destination )
        {
            return;
        }

        assertEquals(source.getInt(), destination.getInt(), "Shoulda been equal.");
        assertEquals(source.getInteger(), destination.getInteger(), "Shoulda been equal.");
        assertEquals(source.getReadWriteProp(), destination.getReadWriteProp(), "Shoulda been equal.");
        assertEquals(source.getString(), destination.getString(), "Shoulda been equal.");

        assertIdentical( 
                source.getReadOnlyNestedBean(), 
                destination.getReadOnlyNestedBean() );
        assertIdentical( 
                source.getReadWriteNestedBean(), 
                destination.getReadWriteNestedBean() );
        assertIdentical(
                source.getReadOnlyArrayProp(), 
                destination.getReadOnlyArrayProp() );
        assertIdentical(
                source.getReadWriteArrayProp(), 
                destination.getReadWriteArrayProp() );
    }
    

    @Test
    public void testCreateCopyNonNullNonNestedPropsWorks()
    {
        final TestBean original = newPopulatedTestBean( null, null );
        final TestBean copy = BeanCopier.createCopy( TestBean.class, original );
        
        assertIdentical( original, copy );
    }
    
    
    private void assertIdentical( final TestBean [] source, final TestBean [] destination )
    {
        if ( null == source || null == destination )
        {
            assertNull(source, "Destination null, but source not.");
            assertNull(destination, "Source null, but destination not.");
        }
        
        if ( null == source || null == destination )
        {
            return;
        }

        assertEquals(source.length,  destination.length, "Source length not equal to destination length.");

        for ( int i = 0; i < source.length; ++i )
        {
            assertIdentical( source[ i ], destination[ i ] );
        }
    }
    
    
    interface TestBean extends SimpleBeanSafeToProxy
    {
        String STRING = "string"; 
        
        String getString();
        
        void setString( final String value );
        


        String INT = "int"; 
        
        int getInt();

        void setInt( final int value );
        


        String INTEGER = "integer"; 
        
        Integer getInteger();

        void setInteger( final Integer value );
        
        

        String READ_ONLY_PROP = "readOnlyProp"; 
        
        String getReadOnlyProp();
        


        String WRITE_ONLY_PROP = "writeOnlyProp"; 
        
        void setWriteOnlyProp( final String value );
        
        

        String READ_WRITE_PROP = "readWriteProp"; 
        
        String getReadWriteProp();

        void setReadWriteProp( final String value );
        
        

        String READ_ONLY_NESTED_BEAN = "readOnlyNestedBean"; 
        
        TestBean getReadOnlyNestedBean();
        


        String WRITE_ONLY_NESTED_BEAN = "writeOnlyNestedBean"; 
        
        void setWriteOnlyNestedBean( final TestBean value );
        
        

        String READ_WRITE_NESTED_BEAN = "readWriteNestedBean"; 
        
        TestBean getReadWriteNestedBean();

        void setReadWriteNestedBean( final TestBean value );
        
        

        String READ_ONLY_ARRAY_PROP = "readOnlyArrayProp"; 
        
        TestBean [] getReadOnlyArrayProp();
        


        String WRITE_ONLY_ARRAY_PROP = "writeOnlyArrayProp"; 
        
        void setWriteOnlyArrayProp( final TestBean [] value );
        
        

        String READ_WRITE_ARRAY_PROP = "readWriteArrayProp"; 
        
        TestBean [] getReadWriteArrayProp();

        void setReadWriteArrayProp( final TestBean [] value );
        
        
        String STRING_ARRAY = "stringArray";
        
        String [] getStringArray();
        
        void setStringArray( final String [] value );
    } // end inner class def
    
    
    private interface ExtendedTestBean extends TestBean
    {
        // empty
    } // end inner class def
}
