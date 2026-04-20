package com.spectralogic.s3.dataplanner.backend.api;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import lombok.Data;

import java.util.UUID;

public @Data class WriteAggregationKey {
    public WriteAggregationKey(final UUID storageDomainId,
                               final BlobStoreTaskPriority priority,
                               final UUID bucketId,
                               final boolean isStageJob) {
        this.m_storageDomainId = storageDomainId;
        this.m_priority = priority;
        this.m_bucketId = bucketId;
        this.m_isStageJob = isStageJob;
    }

    public UUID getStorageDomainId() {
        return m_storageDomainId;
    }

    public BlobStoreTaskPriority getPriority() {
        return m_priority;
    }

    public UUID bucketId() {
        return m_bucketId;
    }

    final UUID m_bucketId;
    final UUID m_storageDomainId;
    final BlobStoreTaskPriority m_priority;
    final boolean m_isStageJob;
}
