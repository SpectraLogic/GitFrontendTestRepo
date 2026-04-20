/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.util.bean.lang.Identifiable;
import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;

/**
 * An {@link Iterable} that streams results to its consumer so that the entire result set does not have to fit
 * in memory.
 */
public final class StreamingIterable< T > implements EnhancedIterable< T >
{
    public StreamingIterable( final EnhancedIterator< T > streamingIterator )
    {
        m_iterator = streamingIterator;
        Validations.verifyNotNull( "Iterator", m_iterator );
    }
    
    
    public EnhancedIterator< T > iterator()
    {
        if ( m_used.getAndSet( true ) )
        {
            throw new IllegalStateException(
                    "Since the iterator is streamed, it cannot be re-used, " 
                    + "since the results to iterate over are not retained once consumed." );
        }
        
        return m_iterator;
    }
    
    
    public List< T > toList()
    {
        try ( final EnhancedIterable< T > iterable = this )
        {
            final List< T > retval = new ArrayList<>();
            for ( final T e : iterable )
            {
                retval.add( e );
                verifyNotTooManyResultsDuringCollectionBuild( retval );
            }
            verifyNotTooManyResultsToDisableStreaming( retval );
            return retval;
        }
    }
    
    
    public Set< T > toSet()
    {
        try ( final EnhancedIterable< T > iterable = this )
        {
            final Set< T > retval = new HashSet<>();
            for ( final T e : iterable )
            {
                retval.add( e );
                verifyNotTooManyResultsDuringCollectionBuild( retval );
            }
            verifyNotTooManyResultsToDisableStreaming( retval );
            return retval;
        }
    }


    public Map< UUID, T > toMap()
    {
        try ( final EnhancedIterable< T > iterable = this )
        {
            final Map< UUID, T > retval = new HashMap<>();
            for ( final T e : iterable )
            {
                //TODO: Ideally we should switch the StreamingIterable class to only accept DatabasePersistable so this check and cast is unnecessary.
                if (e instanceof Identifiable) {
                    if (retval.containsKey(((Identifiable) e).getId())) {
                        throw new RuntimeException("Duplicate ID detected in map: " + ((Identifiable) e).getId());
                    }
                    retval.put(((Identifiable) e).getId(), e );
                } else {
                    throw new IllegalArgumentException("Cannot create a map of type " + e.getClass().getSimpleName() + " because it has no ID.");
                }
                verifyNotTooManyResultsDuringCollectionBuild( retval.values() );
            }
            verifyNotTooManyResultsToDisableStreaming( retval.values() );
            return retval;
        }
    }


    private void verifyNotTooManyResultsToDisableStreaming( final Collection< ? > collection )
    {
        // Need to be paranoid about too large of a collection, check it again
        verifyNotTooManyResultsDuringCollectionBuild( collection );
    
        if ( MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED / 2 < collection.size() )
        {
            try
            {
                throw new RuntimeException( "Streaming was recommended for " + collection.size() + " objects." );
            }
            catch ( final Exception ex )
            {
                LOG.info( "We don't have unlimited RAM.  Consider streaming your results.", ex );
            }
        }
    }
    
    
    private void verifyNotTooManyResultsDuringCollectionBuild( final Collection< ? > collection )
    {
        if ( MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED < collection.size() )
        {
            LOG.error( "We don't have unlimited RAM.  You must stream your results." );
            throw new RuntimeException( "Streaming was disabled for " + collection.size() + " objects." );
        }
    }
    
    
    @Override
    public void register( final PreProcessor< T > preProcessor )
    {
        if ( m_used.get() )
        {
            throw new IllegalStateException( "Preprocessor must be added before the iterator is gotten." );
        }
        m_iterator.register( preProcessor );
    }
    
    
    public void close()
    {
        m_iterator.close();
    }
    
    
    private final AtomicBoolean m_used = new AtomicBoolean( false );
    private final EnhancedIterator< T > m_iterator;
    
    private final static Logger LOG = Logger.getLogger( StreamingIterable.class );
}
