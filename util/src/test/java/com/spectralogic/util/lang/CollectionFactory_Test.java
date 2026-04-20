/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class CollectionFactory_Test 
{
    @Test
    public void testToMapReturnsMap()
    {
        assertEquals("key", CollectionFactory.toMap( "key", Long.valueOf( 1 ) ).entrySet().iterator().next().getKey(), "Shoulda returned a map.");
        assertEquals(Long.valueOf( 1 ),  CollectionFactory.toMap("key", Long.valueOf(1)).entrySet().iterator().next().getValue(), "Shoulda returned a map.");
    }
    
    
    @Test
    public void testToSetNullArrayReturnsEmptySet()
    {
        assertTrue(CollectionFactory.toSet( (Object[])null ).isEmpty(), "Shoulda returned an empty set.");
    }
    

    @Test
    public void testToSetConvertsArrayToSet()
    {
        final Set< String > set = CollectionFactory.toSet( "a", "b" );
        assertTrue(set.remove( "a" ), "Shoulda constructed set from array.");
        assertTrue(set.remove( "b" ), "Shoulda constructed set from array.");
    }
    
    
    @Test
    public void testToListNullArrayReturnsEmptyList()
    {
        assertTrue(CollectionFactory.toList( (Object[])null ).isEmpty(), "Shoulda returned an empty list.");
    }
    

    @Test
    public void testToListConvertsArrayToList()
    {
        final List< String > list = CollectionFactory.toList( "a", "b" );
        assertTrue(list.remove( "a" ), "Shoulda constructed set from array.");
        assertTrue(list.remove( "b" ), "Shoulda constructed set from array.");
    }
    
    
    @Test
    public void testToArrayNullClassNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    CollectionFactory.toArray( null, new ArrayList< String >() );
                }
            } );
    }
    
    
    @Test
    public void testToArrayNullListNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                CollectionFactory.toArray( String.class, null );
            }
        } );
    }
    
    
    @Test
    public void testToArrayConvertsToArray()
    {
        assertEquals(0,  CollectionFactory.toArray(String.class, new ArrayList<String>()).length, "Shoulda converted empty list to empty array.");
        assertEquals(2,  CollectionFactory.toArray(
                String.class, CollectionFactory.toList("a", "b")).length, "Shoulda converted empty list to empty array.");
    }
    

    @Test
    public void testGetGetDefensiveCopyOfTreeSetDoesSo()
    {
        final Set< String > original = new TreeSet<>();
        original.add( "b" );
        original.add( "a" );
        original.add( "c" );
        final Set< String > defensiveCopy = CollectionFactory.getDefensiveCopy( original );
        defensiveCopy.add( "d" );
        assertEquals(3,  original.size(), "Shoulda defensively copied the set.");
        assertEquals("a", defensiveCopy.iterator().next(), "Defensive copy shoulda retained order.");
    }
    

    @Test
    public void testGetGetDefensiveCopyOfSetDoesSo()
    {
        final Set< String > original = CollectionFactory.toSet( "a", "b", "c" );
        final Set< String > defensiveCopy = CollectionFactory.getDefensiveCopy( original );
        defensiveCopy.add( "d" );
        assertEquals(3,  original.size(), "Shoulda defensively copied the set.");
    }
    

    @Test
    public void testGetGetDefensiveCopyOfListDoesSo()
    {
        final List< String > original = CollectionFactory.toList( "a", "b", "c" );
        final List< String > defensiveCopy = CollectionFactory.getDefensiveCopy( original );
        defensiveCopy.add( "d" );
        assertEquals(3,  original.size(), "Shoulda defensively copied the list.");
    }
    

    @Test
    public void testGetGetDefensiveCopyOfArrayDoesSo()
    {
        final String [] original = new String [] { "a", "b", "c" };
        final String [] defensiveCopy = CollectionFactory.getDefensiveCopy( original );
        defensiveCopy[ 0 ] = "d";
        assertEquals("a", original[ 0 ], "Shoulda defensively copied the array.");
    }
    

    @Test
    public void testGetGetDefensiveCopyOfMapDoesSo()
    {
        final Map< String, Object > original = CollectionFactory.toMap( "a", null );
        final Map< String, Object > defensiveCopy = CollectionFactory.getDefensiveCopy( original );
        defensiveCopy.put( "b", null );
        assertEquals(1,  original.size(), "Shoulda defensively copied the map.");
    }
}
