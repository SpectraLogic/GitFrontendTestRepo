package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.tape.api.IoTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.service.api.BeansServiceManager;

import java.util.*;

public abstract class BaseIoTask extends BaseBlobTask implements IoTask {

    protected BaseIoTask(final BlobStoreTaskPriority priority,
                         final UUID tapeId,
                         final DiskManager diskManager,
                         final JobProgressManager jobProgressManager,
                         final TapeFailureManagement tapeFailureManagement,
                         final BeansServiceManager serviceManager) {
        super(priority, tapeId, diskManager, tapeFailureManagement, serviceManager);
        m_jobProgressManager = jobProgressManager;
    }

    @Override
    public void prepareForExecutionIfPossible(
            final TapeDriveResource tapeDriveResource,
            final TapeAvailability tapeAvailability )
    {
        try {
            m_serviceManager.getService(JobEntryService.class).verifyEntriesExist(getChunkIds());
        } catch ( final RuntimeException e) {
            invalidateTaskAndThrow( e );
        }
        super.prepareForExecutionIfPossible(tapeDriveResource, tapeAvailability);
    }

    protected final JobProgressManager m_jobProgressManager;
}
