/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class StreamingIterable_Test
{
    @Test
    public void testConstructorNullIteratorNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                try ( final StreamingIterable< ? > iter = new StreamingIterable<>( null ) ) {}
            }
         } );
    }
    
    
    @Test
    public void testToSetWorks()
    {
        final int count = 10000;
        final StreamingIterator< Long > iterator = new StreamingIterator<>(
                new ConstantStreamedResultsProvider<>( generateConstantResults( count ) ) );
        try (final StreamingIterable< Long > iter = new StreamingIterable<>( iterator ) )
        {
            final Set< Long > values = iter.toSet();
            assertEquals(
                    count,
                    values.size(),
                    "Shoulda iterated over the number of objects produced."
                     );
        }
    }
    
    
    private List< Long > generateConstantResults( final int count )
    {
        final List< Long > retval = new ArrayList<>();
        for ( int i = 0; i < count; ++i )
        {
            retval.add( Long.valueOf( i ) );
        }
        return retval;
    }
    
    
    @Test
    public void testToListWorks()
    {
        final int count = 10000;
        final StreamingIterator< Long > iterator = new StreamingIterator<>(
                new ConstantStreamedResultsProvider<>( generateConstantResults( count ) ) );
        try (final StreamingIterable< Long > iter = new StreamingIterable<>( iterator ) )
        {
            final List< Long > values = iter.toList();
            assertEquals(
                    count,
                    values.size(),
                    "Shoulda iterated over the number of objects produced."
                     );
        }
    }
    
    
    @Test
    public void testIteratorCalledTwiceNotAllowed()
    {
        try ( final StreamingIterable< String > streamingIterable =
                new StreamingIterable<>(
                        new TestIteratorImpl<>( Arrays.asList( "foo", "bar", "bar" )
                                .iterator() ) ) )
        {
            streamingIterable.iterator();
            TestUtil.assertThrows(
                    null,
                    IllegalStateException.class,
                    new BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            streamingIterable.iterator();
                        }
                    } );
        }
    }
    
    
    @Test
    public void testRegisterAfterIteratorNotAllowed()
    {
        try ( final StreamingIterable< String > streamingIterable = new StreamingIterable<>(
                new TestIteratorImpl<>( Arrays.asList( "foo", "bar", "bar" ).iterator() ) ) )
        {
            streamingIterable.iterator();
            TestUtil.assertThrows(
                    null,
                    IllegalStateException.class,
                    new BlastContainer()
                    {
                        public void test() throws Throwable
                        {
                            streamingIterable.register( new PreProcessor< String >()
                            {
                                public void process( final String value )
                                {
                                    // empty
                                }
                            } );
                        }
                    } );
        }
    }
    
    
    @Test
    public void testRegisterCallsIteratorRegister()
    {
        final List< String > items = Arrays.asList( "foo", "bar", "bar" );
        final TestIteratorImpl< String > streamingIterator = new TestIteratorImpl<>( items.iterator() );
        try ( final StreamingIterable< String > streamingIterable =
                new StreamingIterable<>( streamingIterator ) )
        {
            final EmptyPreProcessor firstPreProcessor = new EmptyPreProcessor();
            final EmptyPreProcessor secondPreProcessor = new EmptyPreProcessor();
            streamingIterable.register( firstPreProcessor );
            streamingIterable.register( secondPreProcessor );
            assertEquals(
                    Arrays.asList( firstPreProcessor, secondPreProcessor ),
                    streamingIterator.getPreProcessors(),
                    "Shoulda added the correct pre processors."
                    );
        }
    }
    
    
    private final class EmptyPreProcessor implements PreProcessor< String >
    {
        public void process( final String value )
        {
            // empty
        }
    }


    private static final class TestIteratorImpl< T > implements EnhancedIterator< T > 
    {
        TestIteratorImpl( final Iterator< T > items )
        {
            m_items = items;
            m_preProcessors = new ArrayList<>();
        }
        
        
        public boolean hasNext()
        {
            return m_items.hasNext();
        }
        

        public T next()
        {
            return m_items.next();
        }
        

        public void remove()
        {
            m_items.remove();
        }
        

        public void register( final PreProcessor< T > preProcessor )
        {
            m_preProcessors.add( preProcessor );
        }
        
        
        public Collection< PreProcessor< T > > getPreProcessors()
        {
            return m_preProcessors;
        }
        
        
        public void close()
        {
            // empty
        }
        
        
        private final Iterator< T > m_items;
        private final Collection< PreProcessor< T > > m_preProcessors;
    }// end inner class
}
