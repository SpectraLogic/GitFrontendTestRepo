/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class CronRunnableIdentifier_Test 
{
    @Test
    public void testConstructorNoIdentifierPartsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new CronRunnableIdentifier();
            }
        } );
    }
    

    @Test
    public void testConstructorIdentifierPartsIncludesNullNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new CronRunnableIdentifier( "hello", null );
            }
        } );
    }
    
    
    @Test
    public void testHappyConstruction()
    {
        new CronRunnableIdentifier( "hello" );
        new CronRunnableIdentifier( "hello", "cat" );
    }
    
    
    @Test
    public void testEqualsAndHashCodeContractsObeyed()
    {
        final CronRunnableIdentifier id1 =
                new CronRunnableIdentifier( Integer.valueOf( Integer.MAX_VALUE ), "hello" );
        final CronRunnableIdentifier id2 =
                new CronRunnableIdentifier( Integer.valueOf( Integer.MAX_VALUE ), "hello" );
        final CronRunnableIdentifier id3 =
                new CronRunnableIdentifier( Integer.valueOf( Integer.MAX_VALUE ), "cat" );
        final CronRunnableIdentifier id4 =
                new CronRunnableIdentifier( Integer.valueOf( Integer.MIN_VALUE ), "hello" );
        final CronRunnableIdentifier id5 =
                new CronRunnableIdentifier( Integer.valueOf( Integer.MAX_VALUE ) );
        final CronRunnableIdentifier id6 =
                new CronRunnableIdentifier( Integer.valueOf( Integer.MAX_VALUE ), "hello", "hello" );
        assertEquals(
                id1.hashCode(),
                id1.hashCode(),
                "Hash code should be the same for objects that are equal."
                 );
        assertEquals(
                id1.hashCode(),
                id2.hashCode(),
                "Hash code should be the same for objects that are equal."
                 );
        assertTrue(
                id1.equals( id1 ),
                "Object instances are logically the same, so shoulda said was equal.");
        assertTrue(
                id1.equals( id2 ),
                "Object instances are logically the same, so shoulda said was equal.");
        assertFalse(
                id1.equals( id3 ),
                "Object instances aren't logically the same, so shoulda said wasn't equal.");
        assertFalse(
                id1.equals( id4 ),
                "Object instances aren't logically the same, so shoulda said wasn't equal.");
        assertFalse(
                id1.equals( id5 ),
                "Object instances aren't logically the same, so shoulda said wasn't equal.");
        assertFalse(
                id1.equals( id6 ),
                "Object instances aren't logically the same, so shoulda said wasn't equal.");
        final String nullString = null;
        assertFalse(
                id1.equals( nullString ),
                "Object instances aren't logically the same, so shoulda said wasn't equal.");
        assertFalse(
                id1.equals( "wrongtype" ),
                "Object instances aren't logically the same, so shoulda said wasn't equal.");
    }
    
    
    @Test
    public void testToStringReturnsHumanReadableDescriptions()
    {
        final CronRunnableIdentifier id1 =
                new CronRunnableIdentifier( "hello", "cat" );
        final CronRunnableIdentifier id2 =
                new CronRunnableIdentifier( String.class );
        assertEquals(
                "CronJob[hello|cat]",
                id1.toString(),
                "Shoulda generated human-readable string."
                 );
        assertEquals(
                "CronJob[String]",
                id2.toString(),
                "Shoulda generated human-readable string."
                );
    }
}
