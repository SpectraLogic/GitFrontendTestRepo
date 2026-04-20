package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;

import java.util.Collection;

public interface IODirective {
    public BlobStoreTaskPriority getPriority();

    public Collection<JobEntry> getEntries();
}
