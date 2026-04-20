package com.spectralogic.s3.dataplanner.backend.api;

import com.spectralogic.s3.common.dao.domain.ds3.*;

import java.util.UUID;

public class TargetWriteAggregationKey {
    public TargetWriteAggregationKey(final UUID targetId,
                               final UUID bucketId,
                               final BlobStoreTaskPriority priority) {
        this.m_targetId = targetId;
        this.m_priority = priority;
        this.m_bucketId = bucketId;
    }

    public UUID getTargetId() {
        return m_targetId;
    }

    public UUID getBucketId() {
        return m_bucketId;
    }

    public BlobStoreTaskPriority getPriority() {
        return m_priority;
    }

    final UUID m_targetId;
    final UUID m_bucketId;
    final BlobStoreTaskPriority m_priority;
}
