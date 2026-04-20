/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;


public class AsyncBlobCacheDeleterImpl implements AsyncBlobCacheDeleter {
    public AsyncBlobCacheDeleterImpl(final BlobCacheService blobCacheDeleter, final long filesystemOverheadPerBlob)
    {
        m_filesystemOverheadPerBlob = filesystemOverheadPerBlob;
        final Runnable fileDeleter = new FileDeleter(blobCacheDeleter);
        for ( int i = 0; i < NUMBER_OF_CACHE_FILE_DELETE_THREADS; ++i )
        {
            workPool.submit( fileDeleter );
        }
    }


    @Override
    public void delete(final BlobCache bc)
    {
        m_bytesPendingDelete.addAndGet(bc.getSizeInBytes() + m_filesystemOverheadPerBlob);
        m_entriesPendingDelete.incrementAndGet();
        m_pendingDeletes.add(new PendingDelete(bc));
    }

    @Override
    public long getBytesPendingDelete() {
        return m_bytesPendingDelete.get();
    }


    @Override
    public long getEntriesPendingDelete() {
        return m_entriesPendingDelete.get();
    }


    @Override
    public void waitUntilCurrentDeletionsDone() {
        final CountDownLatch latch = new CountDownLatch(1);
        m_pendingDeletes.add(new PendingDelete(latch));
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for AsyncBlobCacheDeleterImpl to finish", e);
        }
    }


    private final class FileDeleter implements Runnable
    {
        public FileDeleter(final BlobCacheService blobCacheDeleter)
        {
            m_blobCacheDeleter = blobCacheDeleter;
        }

        public void run()
        {
            while ( true )
            {
                final BlobCache bc;
                try
                {
                    final PendingDelete pd = m_pendingDeletes.take();
                    if (pd.latch != null) {
                        if (m_entriesPendingDelete.get() == 0) {
                            pd.latch.countDown();
                        } else {
                            m_pendingDeletes.add(pd);
                        }
                        continue;
                    } else {
                        bc = pd.bc;
                    }
                }
                catch ( final InterruptedException ex )
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException( ex );
                }
                if ( Thread.currentThread().isInterrupted() )
                {
                    return;
                }
                final File file = new File(bc.getPath());
                final File validfile = new File(bc.getPath() + ".v");
                if (validfile.exists()) {
                    if (!validfile.delete()) {
                        LOG.warn("Failed to delete: " + validfile.getPath());
                    }
                }
                if (file.exists()) {
                    if (!file.delete()) {
                        LOG.warn("Failed to delete: " + file.getPath());
                    }
                } else {
                    LOG.warn( "Cache file pending asynchronous delete does not exist: " + file.getPath());
                }
                m_blobCacheDeleter.delete(bc.getId());
                m_pendingDeletes.remove(bc);
                m_bytesPendingDelete.addAndGet(-(bc.getSizeInBytes() + m_filesystemOverheadPerBlob));
                m_entriesPendingDelete.decrementAndGet();
            }
        }
        private final BeanDeleter m_blobCacheDeleter;
    } // end inner class def

    private final class PendingDelete {
        final BlobCache bc;
        final CountDownLatch latch;

        public PendingDelete(final BlobCache bc) {
            this.bc = bc;
            latch = null;
        }

        public PendingDelete(final CountDownLatch latch) {
            this.bc = null;
            this.latch = latch;
        }
    } // end inner class def

    private final BlockingQueue< PendingDelete > m_pendingDeletes = new LinkedBlockingQueue<>();
    private final AtomicLong m_bytesPendingDelete = new AtomicLong(0);
    private final AtomicLong m_entriesPendingDelete = new AtomicLong(0);
    private final WorkPool workPool = WorkPoolFactory.createWorkPool( NUMBER_OF_CACHE_FILE_DELETE_THREADS, "CacheFileDeleter" );
    private final long m_filesystemOverheadPerBlob;
    private final static int NUMBER_OF_CACHE_FILE_DELETE_THREADS = 4;
    private final static Logger LOG = Logger.getLogger( AsyncBlobCacheDeleterImpl.class );
}
