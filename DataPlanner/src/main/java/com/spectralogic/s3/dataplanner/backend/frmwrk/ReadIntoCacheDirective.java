package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;

import java.util.List;
import java.util.UUID;

public class ReadIntoCacheDirective extends ReadDirective{
    public ReadIntoCacheDirective(BlobStoreTaskPriority priority, UUID readSourceId, PersistenceType persistenceType, List<JobEntry> entries) {
        super(priority, readSourceId, persistenceType, entries);
    }
}
