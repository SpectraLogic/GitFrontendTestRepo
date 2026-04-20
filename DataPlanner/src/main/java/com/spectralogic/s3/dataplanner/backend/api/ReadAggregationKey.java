package com.spectralogic.s3.dataplanner.backend.api;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;

import java.util.UUID;

public class ReadAggregationKey {
    public ReadAggregationKey(final BlobStoreTaskPriority priority,
                              final UUID readSourceId,
                              final PersistenceType persistenceType) {
        this.m_priority = priority;
        this.m_readSourceId = readSourceId;
        this.m_persistenceType = persistenceType;
    }

    public BlobStoreTaskPriority getPriority() {
        return m_priority;
    }

    //NOTE: this might be a target id, a tape id, pool id, etc.
    public UUID readSourceId() {
        return m_readSourceId;
    }

    public PersistenceType getPersistenceType() {
        return m_persistenceType;
    }

    final UUID m_readSourceId;
    final BlobStoreTaskPriority m_priority;
    final PersistenceType m_persistenceType;
}
