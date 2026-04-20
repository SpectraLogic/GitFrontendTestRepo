package com.spectralogic.util.lang.iterate;

import java.util.Collection;
import java.util.Iterator;

import com.spectralogic.util.lang.Validations;

public abstract class BatchIterable< T, C extends Collection< T > > implements Iterable< C >
{
    public BatchIterable( final Iterable< T > iterable, final int batchSize )
    {
        Validations.verifyInRange("Batch Size", 1, Integer.MAX_VALUE, batchSize);
        m_iterable = iterable;
        m_batchSize = batchSize;
    }


    @Override
    public Iterator< C > iterator()
    {
        return new BatchIterator( m_iterable.iterator(), m_batchSize );
    }
        
        
    private class BatchIterator implements Iterator< C >
    {
        public BatchIterator( final Iterator< T > iterator, final int batchSize )
        {
            m_iterator = iterator;
            m_batchSize = batchSize;
        }


        public boolean hasNext()
        {
            return m_iterator.hasNext();
        }


        public C next()
        {
            final C collection = instantiateNewCollection();
            for ( int i = 0; i < m_batchSize && m_iterator.hasNext(); i++ )
            {
                collection.add( m_iterator.next() );
            }
            return collection;
        }
        
        
        private final Iterator< T > m_iterator;        
        private final int m_batchSize;
    }
    
    
    protected abstract C instantiateNewCollection();
    

    private final int m_batchSize;
    private final Iterable< T > m_iterable;
}
