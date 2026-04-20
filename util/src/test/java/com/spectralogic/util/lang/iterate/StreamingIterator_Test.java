/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class StreamingIterator_Test 
{
    @Test
    public void testHasNextBasedUsageWorks()
    {
        try ( final StreamingIterator< String > iterator = new StreamingIterator<>(
                new ConstantStreamedResultsProvider<>( CollectionFactory.toList( "a" ) ) ) )
        {
            assertTrue(iterator.hasNext(), "Shoulda reported has next.");
            assertTrue(iterator.hasNext(), "Shoulda reported has next.");
            assertEquals("a", iterator.next(), "Shoulda reported next.");
            assertFalse(
                    iterator.hasNext(),
                    "Should reported no next."
                     );
            assertFalse(
                    iterator.hasNext(),
                    "Should reported no next."
                     );
        }
    }
    
    
    @Test
    public void testExceptionBasedUsageWorks()
    {
        try ( final StreamingIterator< String > iterator = new StreamingIterator<>(
                new ConstantStreamedResultsProvider<>( CollectionFactory.toList( "a", "b" ) ) ) )
        {
            final Set< String > results = new HashSet<>();
            while ( true )
            {
                try
                {
                    results.add( iterator.next() );
                }
                catch ( final NoSuchElementException ex )
                {
                    Validations.verifyNotNull( "Shut up CodePro", ex );
                    break;
                }
            }
            assertEquals(2,  results.size(), "Shoulda collected results.");
        }
    }
    
    
    @Test
    public void testRemoveNotSupported()
    {
        try ( final StreamingIterator< String > iterator = new StreamingIterator<>(
                new ConstantStreamedResultsProvider<>( CollectionFactory.toList( "a" ) ) ) )
        {
            assertTrue(iterator.hasNext(), "Shoulda reported has next.");
            assertEquals("a", iterator.next(), "Shoulda reported next.");
            TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    iterator.remove();
                }
            } );
        }
    }
}
