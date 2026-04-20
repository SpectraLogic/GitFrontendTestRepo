package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.google.common.collect.Iterables;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;

import java.util.*;

public class ReadDirective implements Comparable<ReadDirective>, IODirective {

    public ReadDirective(final BlobStoreTaskPriority priority,
                         final UUID readSourceId,
                         final PersistenceType persistenceType,
                         final List<JobEntry> entries) {
        this.m_priority = priority;
        this.m_readSourceId = readSourceId;
        this.m_persistenceType = persistenceType;
        this.m_entries = entries;
    }

    public BlobStoreTaskPriority getPriority() {
        return m_priority;
    }

    //NOTE: this might be a target id, a tape id, pool id, etc.
    public UUID getReadSourceId() {
        return m_readSourceId;
    }

    public PersistenceType getReadSourceType() {
        return m_persistenceType;
    }

    public List<JobEntry> getEntries() {
        return m_entries;
    }

    public Map<UUID, List<JobEntry>> getEntriesByJobId() {
        if (m_entriesByJobId == null) {
            m_entriesByJobId = new HashMap<>();
            for (final JobEntry entry : m_entries) {
                final UUID jobId = entry.getJobId();
                if (!m_entriesByJobId.containsKey(jobId)) {
                    m_entriesByJobId.put(jobId, new ArrayList<>());
                }
                m_entriesByJobId.get(jobId).add(entry);
            }
        }
        return m_entriesByJobId;
    }

    @Override
    public int compareTo(ReadDirective o) {
        return m_priority.compareTo(o.getPriority());
    }

    final UUID m_readSourceId;
    final BlobStoreTaskPriority m_priority;
    final PersistenceType m_persistenceType;
    final List<JobEntry> m_entries;
    transient Map<UUID, List<JobEntry>> m_entriesByJobId = null;
}