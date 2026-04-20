package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.platform.cache.CacheUtils;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
final class CacheInitializer {
    public CacheInitializer(final CacheFilesystem cacheFilesystem, TierExistingCache tierExistingCache, final BeansServiceManager serviceManager, long mFilesystemOverheadPerBlob) {
        m_filesystem = cacheFilesystem;
        m_blobCacheService = serviceManager.getService(BlobCacheService.class);
        m_blobRetriever = serviceManager.getRetriever(Blob.class);
        m_filesystemOverheadPerBlob = mFilesystemOverheadPerBlob;
        m_tierExistingCache = tierExistingCache;
    }

    public long init() {
        //tier
        try
        {
            m_tierExistingCache.createTieredCacheStructure();
            m_tierExistingCache.moveFilesIntoTierStructure();
        }
        catch ( Exception e )
        {
            LOG.error( "Failed to setup and migrate cache into tiered directory structure.", e );
        }

        //reconcile
        if (m_filesystem.getNeedsReconcile()) {
            final MonitoredWork work = new MonitoredWork(
                    MonitoredWork.StackTraceLogging.NONE,
                    "Reconcile physical cache " + m_filesystem.getPath());
            try {
                runInternal();
            } finally {
                work.completed();
            }
            return m_bytesReconciled;
        } else {
            LOG.info("Filesystem does not require reconcile.");
            return calculateBytesAllocated();
        }
    }

    private long calculateBytesAllocated() {
        final WhereClause filter = Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE));
        return m_blobCacheService.getSum(BlobCache.SIZE_IN_BYTES, filter) + (m_blobCacheService.getCount(filter) * m_filesystemOverheadPerBlob);
    }


    private void runInternal() {
        m_totalDuration.reset();
        m_incrementalDuration.reset();
        m_bytesReconciled = 0;
        m_blobCacheService.delete(Require.nothing()); //clear all existing blob cache entries
        final AtomicInteger numImportedFiles = new AtomicInteger(0);
        reconcile(Paths.get(m_filesystem.getPath()), numImportedFiles);
        LOG.info("Reconciled " + m_filesystem.getPath() + " with " + numImportedFiles + " files in " +
                m_totalDuration);
    }


    private void reconcile(final Path file, final AtomicInteger numImportedFiles) {
        if (Files.isDirectory(file)) {
            try (final DirectoryStream<Path> stream = Files.newDirectoryStream(file)) {
                for (final Path p : stream) {
                    reconcile(p, numImportedFiles);
                }
            } catch (final IOException ex) {
                throw new RuntimeException(ex);
            }
            return;
        }

        if (!Files.exists(file)) {
            return;
        }

        final UUID blobId;
        try {
            blobId = CacheUtils.getBlobId(file.toFile());
        } catch (final Exception ex) {
            //We can't parse a blob ID from this file - this should never happen unless someone has put strange file in the cache dir
            LOG.warn("Could not parse blob ID from unrecognized file " + file + ". Will delete it.", ex);
            try {
                Files.delete(file);
            } catch (IOException e) {
                final long length = file.toFile().length();
                LOG.warn("Failed to delete unrecognized file " + file, e);
                m_bytesReconciled += length + m_filesystemOverheadPerBlob;
            }
            return;
        }

        final boolean isValidMarker = file.toString().endsWith(CacheUtils.CACHE_FILE_VALID_SUFFIX);
        if (isValidMarker) {
            final Path fileThatFileIsValidMarkerFor = Paths.get(file.toString()
                    .substring(0, file.toString()
                            .length()
                            - CacheUtils.CACHE_FILE_VALID_SUFFIX.length()));
            if (!Files.exists(fileThatFileIsValidMarkerFor)) {
                LOG.warn("Valid marker for found file blob that is not present in cache: " + blobId);
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    LOG.warn("Failed to delete invalid marker file " + file, e);
                }
            }
            return;
        }

        final int filesSoFar = numImportedFiles.getAndIncrement();
        if (UPDATE_INTERVAL_SECONDS <= m_incrementalDuration.getElapsedSeconds()) {
            LOG.info(String.format(
                    "Cache entries found on cache filesystem %s - files validated so far: %d (%d/second)",
                    m_filesystem.getPath(), filesSoFar, filesSoFar / m_totalDuration.getElapsedSeconds()));
            m_incrementalDuration.reset();
        }

        final Blob blob = m_blobRetriever.retrieve(blobId);
        final boolean blobIsInDatabase = blob != null;
        final long size = file.toFile().length();
        m_bytesReconciled += size + m_filesystemOverheadPerBlob;
        if (!blobIsInDatabase) {
            m_blobCacheService.create(
                    BeanFactory.newBean(BlobCache.class)
                            .setBlobId(null)
                            .setPath(file.toString())
                            .setState(CacheEntryState.PENDING_DELETE)
                            .setLastAccessed(new Date(System.currentTimeMillis()))
                            .setSizeInBytes(size));
            return;
        }

        final BlobCache bc = m_blobCacheService.allocate(blobId, size, m_filesystem);
        final Path validFile = Paths.get(file.toString() + CacheUtils.CACHE_FILE_VALID_SUFFIX);
        if (Files.exists(validFile)) {
            m_blobCacheService.cacheEntryLoaded(bc, m_filesystem.getCacheSafetyEnabled());
        }
    }



    private long m_bytesReconciled;
    private final Duration m_totalDuration = new Duration();
    private final Duration m_incrementalDuration = new Duration();
    private final CacheFilesystem m_filesystem;
    private final BlobCacheService m_blobCacheService;
    private final BeansRetriever<Blob> m_blobRetriever;
    private final long m_filesystemOverheadPerBlob;
    private final TierExistingCache m_tierExistingCache;
    private static final int UPDATE_INTERVAL_SECONDS = 60;
    private final static Logger LOG = Logger.getLogger(CacheInitializer.class);
} // end inner class def
