/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.util.*;

import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.BatchIterable;
import com.spectralogic.util.lang.iterate.CloseableIterable;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.lang.iterate.EnhancedIterator;

final class RetrieveBeansResultImpl< T > implements RetrieveBeansResult< T >
{
    RetrieveBeansResultImpl( final EnhancedIterable< T > iterable )
    {
        m_iterable = iterable;
        Validations.verifyNotNull( "Iterable", m_iterable );
    }
    
    
    public T getFirst()
    {
        try ( final EnhancedIterator< T > iterator = m_iterable.iterator(); )
        {
            if ( iterator.hasNext() )
            {
                return iterator.next();
            }
            return null;
        }
    }
    
    
    public boolean isEmpty()
    {
        return null == getFirst();
    }

    
    public Set< T > toSet()
    {
        return m_iterable.toSet();
    }


    public Map< UUID, T > toMap()
    {
        return m_iterable.toMap();
    }
    
    
    public BatchEnhancedIterable< T, Set< T > > toSetsOf( final int batchSize )
    {
        return new BatchEnhancedIterable<T, Set< T > >( m_iterable, batchSize ) {
            @Override
            protected Set< T > instantiateNewCollection()
            {
                return new HashSet<>();
            }
        };
    }

    
    public List< T > toList()
    {
        return m_iterable.toList();
    }
    
    
    public BatchEnhancedIterable< T, List< T > > toListsOf( final int batchSize )
    {
        return new BatchEnhancedIterable< T, List< T > >( m_iterable, batchSize ) {
            @Override
            protected List< T > instantiateNewCollection()
            {
                return new ArrayList<>();
            }
        };
    }

    
    public EnhancedIterable< T > toIterable()
    {
        return m_iterable;
    }
    
    
    private abstract class BatchEnhancedIterable< T1, C1 extends Collection< T1 > > implements CloseableIterable< C1 >
    {

        public BatchEnhancedIterable( final EnhancedIterable< T1 > iterable, final int batchSize )
        {
            m_enhancedIterable = iterable;
            m_batchIterable = new BatchIterable< T1, C1 >( m_enhancedIterable, batchSize ) {
                @Override
                protected C1 instantiateNewCollection()
                {
                    return BatchEnhancedIterable.this.instantiateNewCollection();
                }
            };
        }

        @Override
        public void close()
        {
            m_enhancedIterable.close();
            
        }
        
        
        @Override
        public Iterator<C1> iterator()
        {
            return m_batchIterable.iterator();
        }
        
        
        abstract C1 instantiateNewCollection();
        
        
        final EnhancedIterable< T1 > m_enhancedIterable;
        final BatchIterable<T1, C1 > m_batchIterable;
    }
    
    
    private final EnhancedIterable< T > m_iterable;
}
