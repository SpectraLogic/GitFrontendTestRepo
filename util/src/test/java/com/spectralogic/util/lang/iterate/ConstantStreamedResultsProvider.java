/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.List;

final class ConstantStreamedResultsProvider< T > implements StreamedResultsProvider< T >
{
    ConstantStreamedResultsProvider( final List< T > results )
    {
        m_results = results;
    }
    
    
    public T getNextResult()
    {
        if ( m_results.isEmpty() )
        {
            if ( m_calledWhenEmpty )
            {
                throw new IllegalStateException( "I already told you I was empty.  Stop asking." );
            }
            m_calledWhenEmpty = true;
            return null;
        }
        return m_results.remove( 0 );
    }
    
    
    public void close()
    {
        // empty
    }
    
    
    private volatile boolean m_calledWhenEmpty;
    private final List< T > m_results;
}