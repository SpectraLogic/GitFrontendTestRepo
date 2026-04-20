/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.spectralogic.util.lang.CollectionFactory;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class StringListComparator_Test
{
    @Test
    public void testCompareEqualListsReturns0()
    {
        final Comparator< List< String >> instance = StringListComparator.instance();
        assertEquals(0,  instance.compare(new ArrayList<String>(), new ArrayList<String>()), "Shoulda returned 0 because the lists were the same.");
        assertEquals(0,  instance.compare(CollectionFactory.toList("foo"), CollectionFactory.toList("foo")), "Shoulda returned 0 because the lists were the same.");
        assertEquals(0,  instance.compare(
                CollectionFactory.toList("foo", "bar"),
                CollectionFactory.toList("foo", "bar")), "Shoulda returned 0 because the lists were the same.");
        assertEquals(0,  instance.compare(
                CollectionFactory.toList("foo", "bar", "baz"),
                CollectionFactory.toList("foo", "bar", "baz")), "Shoulda returned 0 because the lists were the same.");
    }
    
    
    @Test
    public void testCompareReturnsNegative1WhenSameSizeAndFirstComponentSmaller()
    {
        final Comparator< List< String >> instance = StringListComparator.instance();
        assertEquals(-1,  instance.compare(CollectionFactory.toList("foo"), CollectionFactory.toList("goo")), "Shoulda returned -1 because the first list was first.");
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("foo", "bar"),
                CollectionFactory.toList("goo", "bar")), "Shoulda returned -1 because the first list was first.");
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("foo", "bar", "baz"),
                CollectionFactory.toList("goo", "bar", "baz")), "Shoulda returned -1 because the first list was first.");
    }
    
    
    @Test
    public void testCompareReturnsNegative1WhenFirstComponentsSameAndLaterOnesSmaller()
    {
        final Comparator< List< String >> instance = StringListComparator.instance();
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("foo", "bar"),
                CollectionFactory.toList("foo", "car")), "Shoulda returned -1 because the first list was first.");
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("foo", "bar", "baz"),
                CollectionFactory.toList("foo", "bar", "caz")), "Shoulda returned -1 because the first list was first.");
    }
    
    
    @Test
    public void testCompareReturns1WhenFirstComponentsSameAndLaterOnesLarger()
    {
        final Comparator< List< String >> instance = StringListComparator.instance();
        assertEquals(1,  instance.compare(
                CollectionFactory.toList("foo", "car"),
                CollectionFactory.toList("foo", "bar")), "Shoulda returned 1 because the first list was second.");
        assertEquals(1,  instance.compare(
                CollectionFactory.toList("foo", "bar", "caz"),
                CollectionFactory.toList("foo", "bar", "baz")), "Shoulda returned 1 because the first list was second.");
    }
    
    
    @Test
    public void testCompareReturnsCorrectlyWhenOneIsPrefixOfOther()
    {
        final Comparator< List< String >> instance = StringListComparator.instance();
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("foo"),
                CollectionFactory.toList("foo", "bar")), "Shoulda returned -1 because the first list was first.");
        assertEquals(1,  instance.compare(
                CollectionFactory.toList("foo", "bar", "caz"),
                CollectionFactory.toList("foo")), "Shoulda returned 1 because the first list was second.");
    }
    
    
    @Test
    public void testCompareReturnsCorrectlyWhenOneIsLargerButPrefixDiffers()
    {
        final Comparator< List< String >> instance = StringListComparator.instance();
        assertEquals(1,  instance.compare(
                CollectionFactory.toList("foo"),
                CollectionFactory.toList("eoo", "bar")), "Shoulda returned 1 because the first list was second.");
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("eoo", "bar", "caz"),
                CollectionFactory.toList("foo")), "Shoulda returned -1 because the first list was first.");
        assertEquals(1,  instance.compare(
                CollectionFactory.toList("hello", "foo"),
                CollectionFactory.toList("hello", "eoo", "bar")), "Shoulda returned 1 because the first list was second.");
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("hello", "eoo", "bar", "caz"),
                CollectionFactory.toList("hello", "foo")), "Shoulda returned -1 because the first list was first.");
        assertEquals(-1,  instance.compare(
                CollectionFactory.toList("eoo"),
                CollectionFactory.toList("foo", "bar")), "Shoulda returned -1 because the first list was first.");
        assertEquals(1,  instance.compare(
                CollectionFactory.toList("foo", "bar", "caz"),
                CollectionFactory.toList("eoo")), "Shoulda returned 1 because the first list was second.");
    }
}
