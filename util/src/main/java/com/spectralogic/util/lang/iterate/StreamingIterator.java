/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.spectralogic.util.lang.Validations;

/**
 * An {@link Iterator} that streams results to its consumer so that the entire result set does not have to fit
 * in memory.
 */
public final class StreamingIterator< T > implements EnhancedIterator< T >
{
    public StreamingIterator( final StreamedResultsProvider< T > streamedResultsProvider )
    {
        m_streamedResultsProvider = streamedResultsProvider;
        Validations.verifyNotNull( "Streamed results provider", streamedResultsProvider );
    }
    
    
    synchronized public boolean hasNext()
    {
        if ( m_done )
        {
            return false;
        }
        if ( null == m_next )
        {
            m_next = populateNextResult();
        }
        return ( null != m_next );
    }
    
    
    private T populateNextResult()
    {
        final T retval = m_streamedResultsProvider.getNextResult();
        if ( null == retval )
        {
            close();
        }
        return retval;
    }
    
    
    public void close()
    {
        if ( m_done )
        {
            return;
        }
        
        try
        {
            m_done = true;
            m_streamedResultsProvider.close();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }

    
    synchronized public T next() throws NoSuchElementException
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException( "Out of elements." );
        }
        
        final T retval = m_next;
        for ( final PreProcessor< T > pp : m_preProcessors )
        {
            pp.process( retval );
        }
        
        m_next = null;
        return retval;
    }

    
    public void remove()
    {
        throw new UnsupportedOperationException( "Not supported." );
    }
    
    
    public void register( final PreProcessor< T > preProcessor )
    {
        Validations.verifyNotNull( "Preprocessor", preProcessor );
        m_preProcessors.add( preProcessor );
    }
    
    
    private T m_next;
    private boolean m_done;
    
    private final Set< PreProcessor< T > > m_preProcessors = new CopyOnWriteArraySet<>();
    private final StreamedResultsProvider< T > m_streamedResultsProvider;
}
