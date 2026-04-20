package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;

import java.util.*;

public class WriteDirective implements Comparable<WriteDirective>, IODirective {

    public WriteDirective(BlobStoreTaskPriority priority, Collection<JobEntry> chunks, Bucket bucket, long sizeInBytes) {
        this.m_priority = priority;
        this.m_entries = chunks;
        this.m_bucket = bucket;
        this.m_sizeInBytes = sizeInBytes;
    }

    public BlobStoreTaskPriority getPriority() {
        return m_priority;
    }

    public Collection<JobEntry> getEntries() {
        return m_entries;
    }

    public long getSizeInBytes() { return m_sizeInBytes; }

    public Bucket getBucket() {
        return m_bucket;
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
    public int compareTo(WriteDirective o) {
        return m_priority.compareTo(o.getPriority());
    }

    private final BlobStoreTaskPriority m_priority;
    private final Collection<JobEntry> m_entries;
    private final Bucket m_bucket;
    private final long m_sizeInBytes;
    transient Map<UUID, List<JobEntry>> m_entriesByJobId = null;
}
