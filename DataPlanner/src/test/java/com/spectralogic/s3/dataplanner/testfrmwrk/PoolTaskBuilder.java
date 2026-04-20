package com.spectralogic.s3.dataplanner.testfrmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.frmwrk.LocalWriteDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.pool.PoolLockSupportImpl;
import com.spectralogic.s3.dataplanner.backend.pool.api.*;
import com.spectralogic.s3.dataplanner.backend.pool.task.*;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.mock.InterfaceProxyFactory;

import java.util.UUID;

public class PoolTaskBuilder {

    public PoolTaskBuilder(final BeansServiceManager serviceManager) {
        m_diskManager = new MockDiskManager(serviceManager);
        m_jobProgressManager = new JobProgressManagerImpl( serviceManager, JobProgressManagerImpl.BufferProgressUpdates.NO );
        m_poolEnvironmentResource = InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, null );
        m_lockSupport = new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ),
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
        m_serviceManager = serviceManager;
    }

    public PoolTaskBuilder withDiskManager(final DiskManager diskManager) {
        m_diskManager = diskManager;
        return this;
    }

    public PoolTaskBuilder withJobProgressManager(final JobProgressManager jobProgressManager) {
        m_jobProgressManager = jobProgressManager;
        return this;
    }

    public PoolTaskBuilder withPoolEnvironmentResource(final PoolEnvironmentResource poolEnvironmentResource) {
        m_poolEnvironmentResource = poolEnvironmentResource;
        return this;
    }

    public PoolTaskBuilder withLockSupport(final PoolLockSupport<PoolTask> lockSupport) {
        m_lockSupport = lockSupport;
        return this;
    }

    public WriteChunkToPoolTask buildWriteTask(final LocalWriteDirective writeDirective) {
        return new WriteChunkToPoolTask(
                writeDirective,
                m_serviceManager,
                m_poolEnvironmentResource,
                m_lockSupport,
                m_diskManager,
                m_jobProgressManager
        );
    }

    public VerifyPoolTask buildVerifyPoolTask(final BlobStoreTaskPriority priority, final UUID poolId, BeansServiceManager serviceManager) {
        return new VerifyPoolTask(
                priority,
                poolId,
                m_serviceManager,
                m_poolEnvironmentResource,
                m_lockSupport,
                m_diskManager,
                m_jobProgressManager
        );
    }

    public VerifyPoolTask buildVerifyPoolTask(final BlobStoreTaskPriority priority, final UUID poolId, final int maxNumberOfBlobsToVerifyAtOnce) {
        return new VerifyPoolTask(
                priority,
                poolId,
                m_serviceManager,
                maxNumberOfBlobsToVerifyAtOnce,
                m_poolEnvironmentResource,
                m_lockSupport,
                m_diskManager,
                m_jobProgressManager
        );
    }

    public VerifyChunkOnPoolTask buildVerifyChunkOnPoolTask(final ReadDirective readDirective) {
        return new VerifyChunkOnPoolTask(
                readDirective,
                m_serviceManager,
                m_poolEnvironmentResource,
                m_lockSupport,
                m_diskManager,
                m_jobProgressManager
        );
    }

    public CompactPoolTask buildCompactPoolTask( final BlobStoreTaskPriority priority, final UUID poolId ) {
        return new CompactPoolTask(
                priority,
                poolId,
                m_serviceManager,
                m_poolEnvironmentResource,
                m_lockSupport,
                m_diskManager,
                m_jobProgressManager
        );
    }

    public ImportPoolTask buildImportPoolTask( final BlobStoreTaskPriority priority, final ImportPoolDirective directive,
                                               final BlobStore blobStore) {
        return new ImportPoolTask(
                priority,
                directive,
                blobStore,
                m_serviceManager,
                m_poolEnvironmentResource,
                m_lockSupport,
                m_diskManager,
                m_jobProgressManager
        );
    }

    public ImportPoolTask buildImportPoolTask( final BlobStoreTaskPriority priority,
                                               final ImportPoolDirective directive,
                                               final int maxBlobsPerWorkChunk,
                                               final BlobStore blobStore) {
        return new ImportPoolTask(
                priority,
                directive,
                maxBlobsPerWorkChunk,
                blobStore,
                m_serviceManager,
                m_poolEnvironmentResource,
                m_lockSupport,
                m_diskManager,
                m_jobProgressManager
        );
    }

    DiskManager m_diskManager;
    JobProgressManager m_jobProgressManager;
    BeansServiceManager m_serviceManager;
    PoolEnvironmentResource m_poolEnvironmentResource;
    PoolLockSupport<PoolTask> m_lockSupport;
}
