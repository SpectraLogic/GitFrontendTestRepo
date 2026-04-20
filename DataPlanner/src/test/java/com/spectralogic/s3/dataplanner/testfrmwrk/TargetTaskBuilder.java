package com.spectralogic.s3.dataplanner.testfrmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.Ds3BlobDestination;
import com.spectralogic.s3.common.dao.domain.ds3.S3BlobDestination;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.AzureBlobDestination;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.s3.dataplanner.backend.target.OfflineDataStagingWindowManagerImpl;
import com.spectralogic.s3.dataplanner.backend.target.api.OfflineDataStagingWindowManager;
import com.spectralogic.s3.dataplanner.backend.target.task.*;
import com.spectralogic.util.db.service.api.BeansServiceManager;

import java.util.UUID;

public class TargetTaskBuilder implements S3TargetTaskBuilder, AzureTargetTaskBuilder, Ds3TargetTaskBuilder {
    public TargetTaskBuilder(final BeansServiceManager serviceManager) {
        m_diskManager = new MockDiskManager(serviceManager);
        m_jobProgressManager = new JobProgressManagerImpl( serviceManager, JobProgressManagerImpl.BufferProgressUpdates.NO );
        m_s3ConnectionFactory = new MockS3ConnectionFactory();
        m_azureConnectionFactory = new MockAzureConnectionFactory();
        m_ds3ConnectionFactory = new MockDs3ConnectionFactory();
        m_offlineStagingWindow = new OfflineDataStagingWindowManagerImpl( serviceManager, 1000 * 60 );
        m_serviceManager = serviceManager;
    }

    public TargetTaskBuilder withDiskManager(final DiskManager diskManager) {
        m_diskManager = diskManager;
        return this;
    }

    public TargetTaskBuilder withJobProgressManager(final JobProgressManager jobProgressManager) {
        m_jobProgressManager = jobProgressManager;
        return this;
    }

    public TargetTaskBuilder withServiceManager(final BeansServiceManager serviceManager) {
        m_serviceManager = serviceManager;
        return this;
    }

    public TargetTaskBuilder withOfflineStagingWindow(final OfflineDataStagingWindowManager offlineStagingWindow) {
        m_offlineStagingWindow = offlineStagingWindow;
        return this;
    }

    @Override
    public TargetTaskBuilder withS3ConnectionFactory(S3ConnectionFactory s3ConnectionFactory) {
        this.m_s3ConnectionFactory = s3ConnectionFactory;
        return this;
    }

    @Override
    public ReadChunkFromS3TargetTask buildReadChunkFromS3TargetTask(final ReadDirective readDirective) {
        return new ReadChunkFromS3TargetTask(
                m_offlineStagingWindow,
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_s3ConnectionFactory,
                readDirective);
    }

    @Override
    public WriteChunkToS3TargetTask buildWriteChunkToS3TargetTask(final TargetWriteDirective<S3Target, S3BlobDestination> writeDirective) {
        return new WriteChunkToS3TargetTask(
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_s3ConnectionFactory,
                writeDirective);
    }

    @Override
    public ImportS3TargetTask buildImportS3TargetTask(final BlobStoreTaskPriority priority, final UUID targetId) {
        return new ImportS3TargetTask(priority,
                targetId,
                m_diskManager,
                m_jobProgressManager,
                m_serviceManager,
                m_s3ConnectionFactory);
    }

    @Override
    public TargetTaskBuilder withAzureConnectionFactory(AzureConnectionFactory azureConnectionFactory) {
        this.m_azureConnectionFactory = azureConnectionFactory;
        return this;
    }

    @Override
    public ReadChunkFromAzureTargetTask buildReadChunkFromAzureTargetTask(ReadDirective readDirective) {
        return new ReadChunkFromAzureTargetTask(
                new OfflineDataStagingWindowManagerImpl( m_serviceManager, 1000 * 60 ),
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_azureConnectionFactory,
                readDirective);
    }

    @Override
    public WriteChunkToAzureTargetTask buildWriteChunkToAzureTargetTask(TargetWriteDirective<AzureTarget, AzureBlobDestination> writeDirective) {
        return new WriteChunkToAzureTargetTask(
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_azureConnectionFactory,
                writeDirective);
    }

    @Override
    public ImportAzureTargetTask buildImportAzureTargetTask(BlobStoreTaskPriority priority, UUID targetId) {
        return new ImportAzureTargetTask(priority,
                targetId,
                m_diskManager,
                m_jobProgressManager,
                m_serviceManager,
                m_azureConnectionFactory);
    }

    @Override
    public TargetTaskBuilder withDs3ConnectionFactory(Ds3ConnectionFactory ds3ConnectionFactory) {
        this.m_ds3ConnectionFactory = ds3ConnectionFactory;
        return this;
    }

    @Override
    public ReadChunkFromDs3TargetTask buildReadChunkFromDs3TargetTask(ReadDirective readDirective) {
        return new ReadChunkFromDs3TargetTask(
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_ds3ConnectionFactory,
                readDirective
        );
    }

    @Override
    public WriteChunkToDs3TargetTask buildWriteChunkToDs3TargetTask(TargetWriteDirective<Ds3Target, Ds3BlobDestination> writeDirective) {
        return new WriteChunkToDs3TargetTask(
                m_jobProgressManager,
                m_diskManager,
                m_serviceManager,
                m_ds3ConnectionFactory,
                writeDirective
        );
    }

    DiskManager m_diskManager;
    JobProgressManager m_jobProgressManager;
    S3ConnectionFactory m_s3ConnectionFactory;
    AzureConnectionFactory m_azureConnectionFactory;
    Ds3ConnectionFactory m_ds3ConnectionFactory;
    OfflineDataStagingWindowManager m_offlineStagingWindow;
    BeansServiceManager m_serviceManager;
}
