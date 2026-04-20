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

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;

public final class BlobPoolReadLockReleaser
{
    public BlobPoolReadLockReleaser(
            final PoolLockSupport< ? > lockSupport,
            final BeansRetrieverManager brm,
            final int delayInMillis )
    {
        m_lockSupport = lockSupport;
        m_brm = brm;
        m_executor = new ThrottledRunnableExecutor<>( delayInMillis, null );
        Validations.verifyNotNull( "Lock support", m_lockSupport );
        Validations.verifyNotNull( "Service manager", m_brm );
    }
    
    
    public void schedule()
    {
        m_executor.add( m_worker );
    }
    
    
    private final class PoolBlobReadLockReleaser implements ThrottledRunnable
    {
        @Override
        public void run( final RunnableCompletionNotifier completionNotifier )
        {
            try
            {
                synchronized ( m_lockSupport )
                {
                    runInternal();
                }
            }
            finally
            {
                completionNotifier.completed();
            }
        }
    } // end inner class def
    
    
    private void runInternal()
    {
        final Set< UUID > lockHolders = m_lockSupport.getBlobLockHolders();
        final Set<JobEntry> activeEntries = m_brm.getRetriever( JobEntry.class ).retrieveAll( Require.all(
                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, lockHolders ),
                Require.exists( 
                        JobEntry.JOB_ID,
                        Require.beanPropertyEquals(
                                JobObservable.REQUEST_TYPE,
                                JobRequestType.GET ) ) ) ).toSet();
        final Set< UUID > activeLockHolders = 
                BeanUtils.extractPropertyValues( activeEntries, BlobObservable.BLOB_ID );
        
        final Set< UUID > staleLockHolders = new HashSet<>( lockHolders );
        staleLockHolders.removeAll( activeLockHolders );
        if ( staleLockHolders.isEmpty() )
        {
            return;
        }
        
        LOG.info( staleLockHolders.size() 
                  + " blobs holding pool read locks were stale and will be released." );
        m_lockSupport.releaseBlobLocks( staleLockHolders );
    }
    
    
    private final PoolLockSupport< ? > m_lockSupport;
    private final BeansRetrieverManager m_brm;
    private final PoolBlobReadLockReleaser m_worker = new PoolBlobReadLockReleaser();
    private final ThrottledRunnableExecutor< PoolBlobReadLockReleaser > m_executor;
    
    private final static Logger LOG = Logger.getLogger( BlobPoolReadLockReleaser.class );
}
