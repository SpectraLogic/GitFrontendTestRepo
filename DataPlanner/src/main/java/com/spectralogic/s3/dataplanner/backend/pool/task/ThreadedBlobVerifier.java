/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.util.io.SingleInputStreamProvider;
import com.spectralogic.util.io.ThreadedDataMover;
import com.spectralogic.util.io.ThreadedDataMover.BytesReadListener;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import com.spectralogic.util.tunables.Tunables;

final class ThreadedBlobVerifier extends BaseShutdownable
{
    ThreadedBlobVerifier( final Pool pool )
    {
        super();
        initWorkPool();
        m_pool = pool;
        Validations.verifyNotNull( "Pool", m_pool );
    }
    
    
    void verify( 
            final Bucket bucket, 
            final S3Object object,
            final Blob blob )
    {
        verifyNotShutdown();
        
        final BlobToVerify btv = new BlobToVerify( m_pool, bucket, object, blob, m_pending, m_failures );
        m_pending.incrementAndGet();
        try
        {
            PENDING.put( btv );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private final static class BlobToVerify
    {
        private BlobToVerify(
                final Pool pool,
                final Bucket bucket, 
                final S3Object object,
                final Blob blob,
                final AtomicInteger pending,
                final Map< UUID, String > failures )
        {
            m_pool = pool;
            m_bucket = bucket;
            m_object = object;
            m_blob = blob;
            m_pending = pending;
            m_failures = failures;
        }
        

        private final Pool m_pool;
        private final Bucket m_bucket;
        private final S3Object m_object;
        private final Blob m_blob;
        private final AtomicInteger m_pending;
        private final Map< UUID, String > m_failures;
    } // end inner class def
    
    
    Map< UUID, String > getFailures()
    {
        shutdown();
        while ( 0 < m_pending.get() )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        synchronized ( m_failures )
        {
            return m_failures;
        }
    }
    
    
    private final static class BlobVerifier implements Runnable
    {
        public void run()
        {
            while ( true )
            {
                final BlobToVerify btv;
                try
                {
                    btv = PENDING.take();
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
                
                try
                {
                    verify( btv );
                }
                catch ( final Exception ex )
                {
                    LOG.warn( "Blob verification failed for " + btv.m_blob + ".", ex );
                    synchronized ( btv.m_failures )
                    {
                        btv.m_failures.put( btv.m_blob.getId(), ex.getMessage() );
                    }
                }
                finally
                {
                    btv.m_pending.decrementAndGet();
                }
            }
        }
    } // end inner class def
    
    
    private static void verify( final BlobToVerify btv ) throws IOException
    {
        final Path blobFile =
                PoolUtils.getPath( btv.m_pool, btv.m_bucket.getName(), btv.m_object.getId(), btv.m_blob.getId() );
        final Path blobPropsFile = PoolUtils.getPropsFile( blobFile );
        if ( !Files.exists( blobFile ) )
        {
            throw new RuntimeException( "Blob file doesn't exist: " + blobFile );
        }
        if ( !Files.exists( blobPropsFile ) )
        {
            throw new RuntimeException( "Blob props file doesn't exist: " + blobFile );
        }
    
        final InputStream is = Files.newInputStream( blobFile );
        final ThreadedDataMover threadedDataMover = new ThreadedDataMover(
                HardwareInformationProvider.getZfsCacheFilesystemRecordSize(),
                HardwareInformationProvider.getZfsCacheFilesystemRecordSize(),
                null,
                btv.m_blob.getLength(),
                btv.m_blob.getChecksumType(),
                null, 
                new SingleInputStreamProvider( is ), 
                new NoOpBytesReadListener() );
        threadedDataMover.run();
        is.close();
        final String checksum = Base64.encodeBase64String( threadedDataMover.getChecksum() );
        if ( !ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED.equals( btv.m_blob.getChecksum() )
        		&& !checksum.equals( btv.m_blob.getChecksum() ) )
        {
            throw new RuntimeException( 
                    "Blob had a checksum of " + checksum + ", but was supposed to have a checksum of "
                    + btv.m_blob.getChecksum() + "." );
        }
    }
    
    
    private final static class NoOpBytesReadListener implements BytesReadListener
    {
        public void bytesRead( final int numberOfBytes )
        {
            // empty
        }
    } // end inner class def
    
    
    private final AtomicInteger m_pending = new AtomicInteger( 0 );
    private final Map< UUID, String > m_failures = new HashMap<>();
    private final Pool m_pool;
    
    private final static ArrayBlockingQueue< BlobToVerify > PENDING = new ArrayBlockingQueue<>( 128 );

    private static WorkPool s_workPool = null; // Assignment to this variable
                                               // only happens in a sync block,
                                               // therefore it need not be
                                               // declared volatile.
    private final static Object WORK_POOL_LOCK = new Object();
    
    /** 
     *  This init approach is designed to work most easily with test run thread
     *  leak detection and prevention, and to do so without inducing negative
     *  production impacts on this class or any classes depending on it. Do not
     *  change this approach except as part of changes to test run thread leak
     *  detection and prevention (see TheadLeakHunter).
     */
    private static void initWorkPool()
    {
        synchronized ( WORK_POOL_LOCK )
        {
            if ( null == s_workPool || s_workPool.isShutdown() )
            {
                final int numThreads = Tunables.threadedBlobVerifierNumThreads();
                s_workPool = WorkPoolFactory.createWorkPool( numThreads,
                                   ThreadedBlobVerifier.class.getSimpleName() );

                for ( int i = 0; i < numThreads; ++i )
                {
                    s_workPool.submit( new BlobVerifier() );
                }
            }
        }
    }
    
    private final static Logger LOG = Logger.getLogger( ThreadedBlobVerifier.class );
}
