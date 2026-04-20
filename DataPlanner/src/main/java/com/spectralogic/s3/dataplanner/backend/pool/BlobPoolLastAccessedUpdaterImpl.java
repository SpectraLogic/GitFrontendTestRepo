/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

final class BlobPoolLastAccessedUpdaterImpl implements BlobPoolLastAccessedUpdater
{
    BlobPoolLastAccessedUpdaterImpl( final BlobPoolService service, final int flushFrequencyInMillis )
    {
        if ( 0 < flushFrequencyInMillis )
        {
            m_maxBlobsAccessedBeforeFlush = 10000;
            m_periodicFlusherExecutor = new RecurringRunnableExecutor( 
                    m_periodicFlusherWorker, flushFrequencyInMillis );
            m_periodicFlusherExecutor.start();
        }
        else
        {
            m_maxBlobsAccessedBeforeFlush = 0;
            m_periodicFlusherExecutor = null;
        }
        
        m_service = service;
        Validations.verifyNotNull( "Service", m_service );
    }


    synchronized public void accessed( final UUID blobId )
    {
        Validations.verifyNotNull( "Blob id", blobId );
        m_blobsAccessed.add( blobId );
        
        if ( m_blobsAccessed.size() >= m_maxBlobsAccessedBeforeFlush )
        {
            flush();
        }
    }
    
    
    private final class PeriodicFlusherWorker implements Runnable
    {
        public void run()
        {
            flush();
        }
    } // end inner class def
    
    
    synchronized private void flush()
    {
        if ( m_blobsAccessed.isEmpty() )
        {
            return;
        }
        
        m_service.updateLastAccessed( m_blobsAccessed );
        m_blobsAccessed.clear();
    }
    
    
    private final Set< UUID > m_blobsAccessed = new HashSet<>();
    private final Runnable m_periodicFlusherWorker = new PeriodicFlusherWorker();
    private final RecurringRunnableExecutor m_periodicFlusherExecutor;
    private final int m_maxBlobsAccessedBeforeFlush;
    private final BlobPoolService m_service;
}
