package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.google.common.collect.Lists;
import com.spectralogic.s3.common.dao.domain.ds3.*;

import java.util.Collection;
import java.util.List;

public class LocalWriteDirective extends WriteDirective {

    public LocalWriteDirective(final Collection<? extends LocalBlobDestination> persistenceTargets,
                               final StorageDomain storageDomain,
                               final BlobStoreTaskPriority priority,
                               final Collection<JobEntry> chunks,
                               final long sizeInBytes,
                               final Bucket bucket) {
        this(persistenceTargets,
                storageDomain,
                priority,
                chunks,
                sizeInBytes,
                bucket,
                false,
                false);
    }

    public LocalWriteDirective(final Collection<? extends LocalBlobDestination> persistenceTargets,
                               final StorageDomain storageDomain,
                               final BlobStoreTaskPriority priority,
                               final Collection<JobEntry> chunks,
                               final long sizeInBytes,
                               final Bucket bucket,
                               final boolean isStageJob,
                               final boolean isVerifyAfterWrite) {
        super(priority, chunks, bucket, sizeInBytes);
        this.m_persistenceTargets = Lists.newArrayList(persistenceTargets);
        this.m_storageDomain = storageDomain;
        this.m_isStageJob = isStageJob;
        this.m_isVerifyAfterWrite = isVerifyAfterWrite;
    }


    public List<? extends LocalBlobDestination> getDestinations() {
        return m_persistenceTargets;
    }


    public StorageDomain getStorageDomain() {
        return m_storageDomain;
    }

    public boolean isStageJob() {
        return m_isStageJob;
    }


    public boolean isVerifyAfterWrite() {
        return m_isVerifyAfterWrite;
    }


    final List<? extends LocalBlobDestination> m_persistenceTargets;
    final StorageDomain m_storageDomain;
    final boolean m_isStageJob;
    final boolean m_isVerifyAfterWrite;
}