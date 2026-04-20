/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.dataplanner.backend.frmwrk.*;
import com.spectralogic.s3.dataplanner.backend.frmwrk.PoolWorkAggregationUtils;
import com.spectralogic.util.db.query.Query;
import lombok.NonNull;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.service.ds3.DataPathBackendService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.ImportPoolDirectiveService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.rpc.pool.domain.PoolInformation;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.s3.dataplanner.backend.pool.task.CompactPoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.task.ImportPoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.task.PoolObjectDeleter;
import com.spectralogic.s3.dataplanner.backend.pool.task.ThreadedTrashCollector;
import com.spectralogic.s3.dataplanner.backend.pool.task.VerifyChunkOnPoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.task.VerifyPoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.task.WriteChunkToPoolTask;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;
import com.spectralogic.util.thread.ThrottledRunnableExecutor.WhenAggregating;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class PoolBlobStoreImpl extends BaseShutdownable implements PoolBlobStore
{
    public PoolBlobStoreImpl(
            final RpcClient rpcClient,
            final CacheManager cacheManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        m_jobProgressManager = jobProgressManager;
        m_diskManager = new DiskManagerImpl( cacheManager, this);
        Validations.verifyNotNull( "RPC client", rpcClient );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        Validations.verifyNotNull( "Job progress manager", m_jobProgressManager );
        Validations.verifyNotNull( "Cache manager", m_diskManager );

        LOG.info( getClass().getSimpleName() + " is starting up..." );
        m_poolEnvironmentResource = rpcClient.getRpcResource(
                PoolEnvironmentResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        m_lockSupport = new PoolLockSupportImpl<>(
                new BlobPoolLastAccessedUpdaterImpl(
                        m_serviceManager.getService( BlobPoolService.class ),
                        10 * 60 * 1000 ),
                new PoolPowerManagerImpl( m_serviceManager.getService( PoolService.class ), m_poolEnvironmentResource ),
                new PoolQuiescedManagerImpl( m_serviceManager ) );
        m_reclaimPoolProcessor = new ReclaimPoolProcessor( m_serviceManager, m_lockSupport );

        final RecurringRunnableExecutor m_periodicPoolTaskStarterExecutor =
                new RecurringRunnableExecutor( m_periodicPoolTaskStarter, 57000 );
        addShutdownListener( m_periodicPoolTaskStarterExecutor );
        addShutdownListener(
                new VerifyMediaProcessor<>( Pool.class, Require.beanPropertyEquals( Pool.STATE, PoolState.NORMAL ),
                        BlobPool.class, VerifyPoolTask.class, serviceManager, this, 15 * 60000, 64 ) );

        m_lockSupport.fullyQuiesceUnlockedPoolsThatAreQuiescePending( m_serviceManager );
        restartPendingWork();
        m_environmentManager = new PoolEnvironmentManager(
                m_poolEnvironmentResource, m_serviceManager, m_lockSupport );
        m_blobReadLockReleaser =
                new BlobPoolReadLockReleaser( m_lockSupport, m_serviceManager, 60000 );
        m_periodicPoolTaskStarterExecutor.start();

        for ( final Pool pool : m_serviceManager.getRetriever( Pool.class )
                                                .retrieveAll()
                                                .toSet() )
        {
            new ThreadedTrashCollector().emptyTrash( PoolUtils.getTrashPath( pool ) );
        }
        LOG.info( getClass().getSimpleName() + " is online and ready." );
    }


    @Override public void deleteBucket( final UUID bucketId, final String bucketName )
    {
        for ( final Pool pool : m_serviceManager.getRetriever( Pool.class )
                                                .retrieveAll()
                                                .toSet() )
        {
            if ( Files.exists( PoolUtils.getPath( pool, bucketName, null, null ) ) )
            {
                final PoolTask lockHolder = waitForDeleteLock( pool, "Delete Bucket " + bucketName );
                try
                {
                    final Bucket bucket = m_serviceManager.getRetriever( Bucket.class )
                                                          .retrieve( Require.beanPropertyEquals( Bucket.NAME,
                                                                  bucketName ) );
                    if ( null == bucket )
                    {
                        try
                        {
                            final Path trash = PoolUtils.getTrashPath( pool );
                            Files.createDirectories( trash );
                            Files.move( PoolUtils.getPath( pool, bucketName, null, null ),
                                    trash.resolve( bucketId.toString() ) );
                        }
                        catch ( final IOException ex )
                        {
                            LOG.warn( "Failed to move bucket " + bucketName + " to " + PoolUtils.getTrashPath( pool ),
                                    ex );
                        }
                        new ThreadedTrashCollector().emptyTrash( PoolUtils.getTrashPath( pool ) );
                    }
                    else
                    {
                        LOG.warn( "Unable to move " + bucketName + " to " + PoolUtils.getTrashPath( pool ) +
                                ", bucket name exists in database." );
                        return;
                    }
                }
                finally
                {
                    m_lockSupport.releaseLock( lockHolder );
                }

            }
        }
    }


    private PoolTask waitForDeleteLock( final Pool pool, final String action )
    {
        if ( !m_serviceManager.getService( DataPathBackendService.class ).isActivated() )
        {
            throw new DataPlannerException( GenericFailure.CONFLICT,
                    "Cannot delete data from disk because data path backend is not activated." );
        }
        final MonitoredWork waitingForLock = new MonitoredWork( MonitoredWork.StackTraceLogging.NONE,
                PoolBlobStoreImpl.class.getSimpleName() + ".waitForDeleteLock" );
        final PoolTask lockHolder =
                InterfaceProxyFactory.getProxy( PoolTask.class, MockInvocationHandler.forToString( action ) );
        try
        {
            m_lockSupport.acquireDeleteLockWait( pool.getId(), lockHolder );
        }
        finally
        {
            waitingForLock.completed();
        }
        return lockHolder;
    }


    @Override public void deleteObjects( final String bucketName, final Set< UUID > objectIds )
    {
        for ( final Pool pool : m_serviceManager.getRetriever( Pool.class )
                                                .retrieveAll()
                                                .toSet() )
        {
            if ( Files.exists( PoolUtils.getPath( pool, bucketName, null, null ) ) )
            {
                final Path trash = PoolUtils.getTrashPath( pool );
                try
                {
                    Files.createDirectories( trash );
                    new PoolObjectDeleter().deleteObjects( m_serviceManager, m_lockSupport, pool, bucketName,
                            objectIds );
                }
                catch ( final IOException ex )
                {
                    LOG.error( "Unable to create trash directory " + trash + ", pool objects not deleted.", ex );
                }
            }
        }
    }


    public PoolLockSupport< PoolTask > getLockSupport()
    {
        return m_lockSupport;
    }


    public void write(@NonNull final LocalWriteDirective writeDirective) {
        synchronized ( m_lockSupport )
        {
            addIoTask( new WriteChunkToPoolTask(writeDirective, m_serviceManager, m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager));
        }
    }


    public void read(@NonNull final ReadDirective readDirective) {
        //NOTE: this should not ever be called unless we need to support reading from pool into cache in the future.
        throw new UnsupportedOperationException("Pool should be read from directly instead of via a task.");
    }


    private void verify(@NonNull final ReadDirective readDirective) {
        synchronized ( m_lockSupport )
        {
            addIoTask( new VerifyChunkOnPoolTask(readDirective, m_serviceManager, false, false, m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager));
        }
    }


    private void addIoTask( final PoolTask ioTask )
    {
        m_ioTasks.add( ioTask );
        enqueued( ioTask );
    }


    private < T extends PoolTask > void dequeued( final T task, final String cause )
    {
        if ( BlobStoreTaskState.PENDING_EXECUTION == task.getState() ||
                BlobStoreTaskState.IN_PROGRESS == task.getState() )
        {
            throw new IllegalStateException(
                    "Cannot dequeue " + task + " while it is in state " + task.getState() + "." );
        }

        m_lockSupport.releaseLock( task );
        LOG.info( "Dequeued " + task.toString() + " from execution since " + cause + "." );
        task.dequeued();
    }


    private < T extends PoolTask > T enqueued( final T task )
    {
        task.addSchedulingListener( new PoolTaskSchedulingRequiredListener( task ) );
        LOG.info( "Enqueued " + task.toString() + " for execution at priority " + task.getPriority() + "." );
        return task;
    }


    private final class PoolTaskSchedulingRequiredListener implements BlobStoreTaskSchedulingListener
    {
        private PoolTaskSchedulingRequiredListener( final PoolTask task )
        {
            m_task = task;
        }

        public void taskSchedulingRequired( final BlobStoreTask task )
        {
            try
            {
                updatePoolCapacities();
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Failed to update pool capacities.", ex );
            }

            m_lockSupport.releaseLock( m_task );
            m_periodicPoolTaskStarter.run();
        }

        private void updatePoolCapacities()
        {
            final UUID poolId = m_task.getPoolId();
            final Pool pool = m_serviceManager.getRetriever( Pool.class ).attain( poolId );
            final PoolInformation poolInformation =
                    m_poolEnvironmentResource.getPool( pool.getGuid() ).get( Timeout.DEFAULT );
            BeanCopier.copy( pool, poolInformation );
            m_serviceManager.getService( PoolService.class ).update(
                    pool,
                    PoolObservable.AVAILABLE_CAPACITY,
                    PoolObservable.RESERVED_CAPACITY,
                    PoolObservable.TOTAL_CAPACITY,
                    PoolObservable.USED_CAPACITY );
        }

        private final PoolTask m_task;
    } // end inner class def


    private boolean ensurePhysicalPoolEnvironmentUpToDate( final boolean forceRefresh )
    {
        if ( !m_poolEnvironmentResource.isServiceable() )
        {
            LOG.warn( "Pool environment is not serviceable at this time." );
            return false;
        }

        synchronized ( m_poolEnvironmentStateLock )
        {
            final long poolEnvironmentGenerationNumber = m_poolEnvironmentResource.getPoolEnvironmentGenerationNumber()
                                                                                  .get( Timeout.DEFAULT );
            if ( -1 < m_poolEnvironmentGenerationNumber && !forceRefresh &&
                    m_poolEnvironmentGenerationNumber == poolEnvironmentGenerationNumber &&
                    !m_environmentManager.needsAnotherRun() &&
                    90 > m_durationSincePoolEnvironmentLastRefreshed.getElapsedMinutes() )
            {
                return true;
            }

            if ( -2 == m_poolEnvironmentGenerationNumber )
            {
                m_poolEnvironmentResource.quiesceState()
                                         .get( Timeout.VERY_LONG );
            }

            m_durationSincePoolEnvironmentLastRefreshed.reset();
            m_environmentManager.run();
            m_poolEnvironmentGenerationNumber = poolEnvironmentGenerationNumber;
            return true;
        }
    }


    public void compactPool( final BlobStoreTaskPriority priority, final UUID poolId )
    {
        if ( null == poolId )
        {
            for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll(
                    Query.where( Require.all(
                       Require.beanPropertyEquals(Pool.STATE, PoolState.NORMAL ),
                       Require.beanPropertyEquals(Pool.QUIESCED, Quiesced.NO )
                    ) ) ).toSet() )
            {
                compactPool( priority, pool.getId() );
            }
            return;
        }

        synchronized ( m_lockSupport )
        {
            if ( m_compactionTasks.containsKey( poolId ) )
            {
                if ( m_compactionTasks.get( poolId ).getPriority().ordinal() > priority.ordinal() )
                {
                    m_compactionTasks.get( poolId ).setPriority( priority );
                }
                return;
            }

            m_compactionTasks.put(
                    poolId,
                    enqueued( new CompactPoolTask( priority, poolId, m_serviceManager, m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager ) ) );
        }
    }


    public void formatPool( final UUID poolId )
    {
        if ( null == poolId )
        {
            for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll(
                    Pool.STATE, PoolState.FOREIGN ).toSet() )
            {
                formatPool( pool.getId() );
            }
            return;
        }

        final PoolTask lockHolder = InterfaceProxyFactory.getProxy(
                PoolTask.class,
                MockInvocationHandler.forToString( "Format pool" ) );
        try
        {
            m_lockSupport.acquireExclusiveLock( poolId, lockHolder );
            synchronized ( m_lockSupport )
            {
                final Pool pool = m_serviceManager.getRetriever( Pool.class ).attain( poolId );
                if ( PoolState.FOREIGN != pool.getState() )
                {
                    throw new DataPlannerException( GenericFailure.CONFLICT,
                            "Pools can only be formatted when in state " + PoolState.FOREIGN + ".  Pool "
                            + poolId + " is in state " + pool.getState() + "." );
                }
                m_poolEnvironmentResource.formatPool( pool.getGuid() ).get( Timeout.LONG );
                m_poolEnvironmentResource.takeOwnershipOfPool( pool.getGuid(), pool.getId() ).get(
                        Timeout.DEFAULT );
                m_serviceManager.getService( PoolService.class ).update(
                        pool.setState( PoolState.NORMAL ), Pool.STATE );
                m_poolEnvironmentGenerationNumber = -1;
            }
        }
        finally
        {
            m_lockSupport.releaseLock( lockHolder );
        }
    }


    public void destroyPool( final UUID poolId )
    {
        final PoolTask lockHolder = InterfaceProxyFactory.getProxy(
                PoolTask.class,
                MockInvocationHandler.forToString( "Destroy pool" ) );
        try
        {
            m_lockSupport.acquireExclusiveLock( poolId, lockHolder );
            synchronized ( m_lockSupport )
            {
                final Pool pool = m_serviceManager.getRetriever( Pool.class ).attain( poolId );
                if ( null != pool.getStorageDomainMemberId() )
                {
                    if ( null != m_serviceManager.getRetriever( Pool.class ).retrieve( Require.all(
                            Require.beanPropertyEquals( Identifiable.ID, poolId ),
                            Require.exists( BlobPool.class, BlobPool.POOL_ID, Require.nothing() ) ) ) )
                    {
                        throw new DataPlannerException(
                                GenericFailure.CONFLICT,
                                "Pool contains data on it for storage domain member"
                                + pool.getStorageDomainMemberId() + "." );
                    }
                }
                m_poolEnvironmentResource.destroyPool( pool.getGuid() ).get( Timeout.LONG );
                m_serviceManager.getService( PoolService.class ).delete( pool.getId() );
                m_poolEnvironmentGenerationNumber = -1;
            }
        }
        finally
        {
            if ( null != m_serviceManager.getRetriever( Pool.class ).retrieve( poolId ) )
            {
                m_lockSupport.releaseLock( lockHolder );
            }
        }
    }


    private List< BlobPool > getBlobPoolsFor( final UUID blobId )
    {
        return m_serviceManager.getRetriever( BlobPool.class )
                               .retrieveAll( Require.all( Require.beanPropertyEquals( BlobObservable.BLOB_ID, blobId ),
                                       Require.not( Require.exists( SuspectBlobPool.class, Identifiable.ID,
                                               Require.nothing() ) ) ) )
                               .toList();
    }


    public void importPool( final BlobStoreTaskPriority priority, final ImportPoolDirective directive )
    {
        if ( null == directive.getPoolId() )
        {
            for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll(
                    Pool.STATE, PoolState.FOREIGN ).toSet() )
            {
                final ImportPoolDirective newDirective = BeanFactory.newBean( ImportPoolDirective.class );
                BeanCopier.copy( newDirective, directive );
                newDirective.setPoolId( pool.getId() );
                importPool( priority, newDirective );
            }
            return;
        }

        synchronized ( m_lockSupport )
        {
            final Pool pool = m_serviceManager.getRetriever( Pool.class ).attain( directive.getPoolId() );
            if ( PoolState.FOREIGN != pool.getState() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Only foreign pools can be imported.  Pool "
                        + directive.getPoolId() + " is " + pool.getState() + "." );
            }

            WhereClause storageDomainFilter = Require.nothing();
            WhereClause dataPersistenceRuleFilter = Require.nothing();
            if ( null != directive.getStorageDomainId() )
            {
                storageDomainFilter = Require.beanPropertyEquals(
                        StorageDomain.ID,
                        directive.getStorageDomainId() );

            }
            if ( null != directive.getDataPolicyId() )
            {
                dataPersistenceRuleFilter = Require.beanPropertyEquals(
                        DataPersistenceRule.DATA_POLICY_ID,
                        directive.getDataPolicyId() );

            }
            final int validStorageDomainMembers =
                    m_serviceManager.getRetriever( StorageDomainMember.class ).getCount(
                            Require.all(
                                    Require.beanPropertyEquals(
                                            StorageDomainMember.POOL_PARTITION_ID,
                                            pool.getPartitionId() ),
                                    Require.exists(
                                            StorageDomainMember.STORAGE_DOMAIN_ID,
                                            Require.all(
                                                    storageDomainFilter,
                                                    Require.exists(
                                                            DataPersistenceRule.class,
                                                            DataPersistenceRule.STORAGE_DOMAIN_ID,
                                                            dataPersistenceRuleFilter ) ) ) ) );
            if ( 0 == validStorageDomainMembers )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Could not determine a valid import configuration for pool "
                        + pool.getName() + ". Please ensure data persistence rules"
                        + " and storage domain members are configured correctly for"
                        + " the data policy and storage domain you wish to import to." );
            }

            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                transaction.getService( PoolService.class ).update(
                        pool.setState( PoolState.IMPORT_PENDING ), Pool.STATE );
                transaction.getService( ImportPoolDirectiveService.class ).deleteByEntityToImport(
                        directive.getPoolId() );
                transaction.getService( ImportPoolDirectiveService.class ).create( directive );
                if ( m_importTasks.containsKey( directive.getPoolId() ) )
                {
                    deleteTask( m_importTasks.get( directive.getPoolId() ), "scheduling new import task" );
                }
                m_importTasks.put(
                        directive.getPoolId(),
                        enqueued( new ImportPoolTask( priority, directive, this, m_serviceManager, m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager ) ) );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        }
    }


    public boolean cancelImportPool( final UUID poolId )
    {
        synchronized ( m_lockSupport )
        {
            final PoolTask task = m_importTasks.get( poolId );
            if ( null == task
                    || ImportPoolTask.class != task.getClass()
                    || BlobStoreTaskState.READY != task.getState() )
            {
                return false;
            }

            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                transaction.getService( ImportPoolDirectiveService.class ).deleteByEntityToImport( poolId );
                transaction.getService( PoolService.class ).update(
                        (Pool)BeanFactory.newBean( Pool.class )
                            .setState( PoolState.FOREIGN ).setId( poolId ),
                        Pool.STATE );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
            deleteTask( task, "cancel requested" );
            return true;
        }
    }


    public boolean cancelVerifyPool( final UUID poolId )
    {
        synchronized ( m_lockSupport )
        {
            final PoolTask task = m_verifyTasks.get( poolId );
            if ( null == task || VerifyPoolTask.class != task.getClass() )
            {
                return false;
            }
            if ( BlobStoreTaskState.READY != task.getState() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Verify is in progress and cannot be canceled.  Please try again later." );
            }

            deleteTask( task, "cancel requested" );
            return true;
        }
    }


    public void verify(final @NonNull BlobStoreTaskPriority priority, final UUID poolId )
    {
        if ( null == poolId )
        {
            for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll(
                    Pool.STATE, PoolState.NORMAL ).toSet() )
            {
                verify( priority, pool.getId() );
            }
            return;
        }

        synchronized ( m_lockSupport )
        {
            if ( m_verifyTasks.containsKey( poolId ) )
            {
                if ( m_verifyTasks.get( poolId ).getPriority().ordinal() > priority.ordinal() )
                {
                    m_verifyTasks.get( poolId ).setPriority( priority );
                }
                return;
            }

            m_verifyTasks.put(
                    poolId,
                    enqueued( new VerifyPoolTask( priority, poolId, m_serviceManager, m_poolEnvironmentResource, m_lockSupport, m_diskManager, m_jobProgressManager ) ) );
        }
    }


    private final class PeriodicPoolTaskStarter implements Runnable
    {
        public void run()
        {
            compactPoolsIfNecessary();
            m_poolTaskStarterExecutor.add( m_poolTaskStarter );
        }

        private void compactPoolsIfNecessary()
        {
            if ( HOURS_BETWEEN_POOL_COMPACTION * 60 < m_durationSinceCompactedPools.getElapsedMinutes() )
            {
                compactPool( BlobStoreTaskPriority.BACKGROUND, null );
                m_durationSinceCompactedPools.reset();
            }
        }

        private final Duration m_durationSinceCompactedPools = new Duration();
    } // end inner class def

    private void discoverWork() {
        for (final IODirective directive : PoolWorkAggregationUtils.discoverPoolWorkAggregated(m_serviceManager)) {
            if (directive instanceof LocalWriteDirective) {
                final LocalWriteDirective wd = (LocalWriteDirective) directive;
                WorkAggregationUtils.markWriteChunksInProgress(wd, m_serviceManager);
                WorkAggregationUtils.markLocalDestinationsInProgress(wd.getDestinations(), m_serviceManager);
                write(wd);
            } else if (directive instanceof ReadDirective) {
                final ReadDirective rd = (ReadDirective) directive;
                WorkAggregationUtils.markReadChunksInProgress(rd, m_serviceManager);
                verify(rd);
            }
        }
    }


    private final class PoolTaskStarter implements ThrottledRunnable
    {
        public void run( final RunnableCompletionNotifier completionNotifier )
        {
            final Duration duration = new Duration();
            try
            {
                Thread.currentThread().setName( PoolTaskStarter.class.getSimpleName() );
                if ( !ensurePhysicalPoolEnvironmentUpToDate( false ) )
                {
                    return;
                }
                if ( !m_serviceManager.getService( DataPathBackendService.class ).isActivated() )
                {
                    LOG.info( "Will not attempt to start any tasks since backend isn't activated." );
                    return;
                }

                m_lockSupport.fullyQuiesceUnlockedPoolsThatAreQuiescePending( m_serviceManager );
                m_reclaimPoolProcessor.run();
                discoverWork();
                final List< PoolTask > tasks = getSortedPoolTasks();
                if ( tasks.isEmpty() )
                {
                    LOG.info( "There are no outstanding pool tasks." );
                }
                else
                {
                    LOG.info( "Will attempt to start tasks.  There are " +
                              getIoTaskCount() + " I/O tasks, "
                              + m_importTasks.size() + " import tasks, "
                              + m_compactionTasks.size() + " compaction tasks, and "
                              + m_verifyTasks.size() + " verify tasks." );
                }

                verifyPoolsCanServiceGetsAndVerifies();
                for ( final PoolTask task : tasks )
                {
                    if ( BlobStoreTaskState.COMPLETED == task.getState() )
                    {
                        deleteTask( task, "task has completed" );
                    }
                    else if ( BlobStoreTaskState.READY == task.getState() )
                    {
                        if ( m_taskWorkPool.isFull() )
                        {
                            LOG.info( "Task work pool is full.  "
                                      + "Will not attempt to execute any more tasks at this time." );
                            return;
                        }
                        attemptToExecute( task );
                    }
                }
            }
            finally
            {
                LOG.info( "Completed running " + PoolTaskStarter.class.getSimpleName() + " in "
                        + duration + "." );
                completionNotifier.completed();
            }
        }
    } // end inner class def


    private void verifyPoolsCanServiceGetsAndVerifies()
    {
        for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll().toSet() )
        {
            m_offlinePoolTracker.update(
                    pool.getId(),
                    PoolState.NORMAL != pool.getState() || Quiesced.NO != pool.getQuiesced() );
        }

        synchronized ( this )
        {
            final Set< PoolTask > chunkReadingTasks = new HashSet<>();
            for ( final PoolTask ioTask : m_ioTasks )
            {
                if ( ChunkReadingTask.class.isAssignableFrom( ioTask.getClass() ) )
                {
                    chunkReadingTasks.add( ioTask );
                }
            }

            final Set< UUID > poolIds = new HashSet<>();
            for ( final PoolTask t : chunkReadingTasks )
            {
            	final UUID poolId = ( (ChunkReadingTask)t ).getEntries().iterator().next().getReadFromPoolId();
            	poolIds.add( poolId );
            }

            final Map< UUID, Pool > pools = BeanUtils.toMap(
                    m_serviceManager.getRetriever( Pool.class ).retrieveAll( poolIds ).toSet() );
            for ( final PoolTask t : chunkReadingTasks )
            {
            	final UUID poolId = ( (ChunkReadingTask)t ).getEntries().iterator().next().getReadFromPoolId();
                final Pool pool = pools.get( poolId );
                if ( Quiesced.NO != pool.getQuiesced() )
                {
                    partitionCannotServiceGetOrVerify( t, pool, Pool.QUIESCED );
                }
                if ( PoolState.NORMAL != pool.getState() )
                {
                    partitionCannotServiceGetOrVerify( t, pool, Pool.STATE );
                }
            }
        }
    }


    private void partitionCannotServiceGetOrVerify(
            final PoolTask t,
            final Pool pool,
            final String beanProperty )
    {
        final DataPathBackend dpb =
                m_serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() );
        final Duration durationOffline = m_offlinePoolTracker.getOfflineDuration( pool.getId() );
        final boolean fail = ( null != durationOffline && durationOffline.getElapsedMinutes()
                > dpb.getUnavailablePoolMaxJobRetryInMins() );
        final String suffix = ( fail ) ?
                "Must re-chunk since pool has been unavailable for " + durationOffline
                : "Will wait to see if re-chunking can be avoided since pool has only been unavailable for "
                  + durationOffline;
        try
        {
            LOG.warn( "Pool " + pool.getName() + " is " + beanProperty + "="
                    + BeanUtils.getReader( Pool.class, beanProperty ).invoke( pool )
                    + ", so cannot service " + t.getName() + ".  "
                    + suffix + "." );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        if ( fail )
        {
            deleteTask( t, "re-chunking required" );
        }
    }


    private int getIoTaskCount()
    {
        synchronized ( m_lockSupport )
        {
            return m_ioTasks.size();
        }
    }


    private void deleteTask( final PoolTask task, final String cause )
    {
        synchronized ( m_lockSupport )
        {
            if ( m_compactionTasks.containsValue( task ) )
            {
                dequeued( task, cause );
                removeTaskFromMap( m_compactionTasks, task );
                return;
            }
            if ( m_importTasks.containsValue( task ) )
            {
                dequeued( task, cause );
                removeTaskFromMap( m_importTasks, task );
                return;
            }
            if ( m_verifyTasks.containsValue( task ) )
            {
                dequeued( task, cause );
                removeTaskFromMap( m_verifyTasks, task );
                return;
            }
            if ( m_ioTasks.remove( task ) )
            {
                dequeued( task, cause ) ;
            }
        }
    }


    private void removeTaskFromMap( final Map< UUID, PoolTask > map, final PoolTask task )
    {
        for ( final Map.Entry< UUID, PoolTask > e : new HashSet<>( map.entrySet() ) )
        {
            if ( e.getValue() == task )
            {
                map.remove( e.getKey() );
                return;
            }
        }
        throw new IllegalStateException( "Task " + task + " not in map: " + map );
    }


    public DiskFileInfo getPoolFileFor(final UUID blobId )
    {
        DiskFileInfo retval = null;
        final List< BlobPool > blobPools = getBlobPoolsFor( blobId );
        if ( !blobPools.isEmpty() )
        {
            final Set< UUID > poolIds = BeanUtils.extractPropertyValues( blobPools, BlobPool.POOL_ID );
            final Map< UUID, Pool > pools = BeanUtils.toMap( m_serviceManager.getRetriever( Pool.class )
                                                                             .retrieveAll( Require.all(
                                                                                     Require.beanPropertyEquals(
                                                                                             Pool.STATE,
                                                                                             PoolState.NORMAL ),
                                                                                     Require.beanPropertyEqualsOneOf(
                                                                                             Identifiable.ID,
                                                                                             poolIds ) ) )
                                                                             .toSet() );
            for ( final BlobPool bp : new HashSet<>( blobPools ) )
            {
                if ( !pools.containsKey( bp.getPoolId() ) )
                {
                    LOG.info( "Cannot service GET from pool " + bp.getPoolId() + "." );
                    blobPools.remove( bp );
                }
            }
            blobPools.sort( new BlobPoolComparator( pools ) );

            final Blob blob = m_serviceManager.getRetriever( Blob.class )
                                              .attain( blobId );
            for ( final BlobPool blobPool : blobPools )
            {
                try
                {
                    m_lockSupport.acquireReadLock( blobPool.getPoolId(), blob.getId() );
                }
                catch ( final PoolLockingException e )
                {
                    LOG.info( "Cannot read from pool " + blobPool.getPoolId() + " at this time: " +
                            ExceptionUtil.getRootCauseReadableMessage( e ) );
                    continue;
                }

                if ( null == m_serviceManager.getRetriever( BlobPool.class )
                                             .retrieve( blobPool.getId() ) )
                {
                    LOG.info( "By the time a read lock was acquired on pool " + blobPool.getPoolId() +
                            ", blob was whacked from the pool: " + blobId );
                    continue;
                }

                final Path path = PoolUtils.getPath( pools.get( blobPool.getPoolId() ),
                        m_serviceManager.getRetriever( Bucket.class )
                                        .attain( blobPool.getBucketId() )
                                        .getName(), blob.getObjectId(), blobPool.getBlobId() );

                retval = BeanFactory.newBean( DiskFileInfo.class )
                        .setFilePath( path.toString() )
                        .setBlobPoolId(blobPool.getId());

                boolean fileInvalid = false;

                if (!Files.exists( path )) {
                    LOG.error( "Blob " + blobPool.getBlobId() + " was expected on pool " + blobPool.getPoolId() + " but file doesn't exist.");
                    fileInvalid = true;
                }

                try {
                    final long actualSize = Files.size( path );
                    if (actualSize != blob.getLength()) {
                        LOG.error( "Blob " + blobPool.getBlobId() + " on pool " + blobPool.getPoolId() + " was expected to be "
                                + blob.getLength() + " bytes bytes but is " + actualSize + ".");
                        fileInvalid = true;
                    }
                } catch (final IOException e) {
                    LOG.error( "Could not determine size of blob " + blobPool.getBlobId() + " on pool " + blobPool.getPoolId() + ".");
                    fileInvalid = true;
                }

                if (fileInvalid) {
                    m_serviceManager.getService(BlobPoolService.class).registerFailureToRead(retval);
                    retval = null;
                    continue;
                }

                break;
            }
        }
        return retval;
    }


    private void attemptToExecute( final PoolTask task )
    {
        LOG.info( "Preparing " + task + " for execution..." );
        synchronized ( m_lockSupport )
        {
            task.prepareForExecutionIfPossible();
        }
        if ( null == task.getPoolId() )
        {
            LOG.info( "Cannot execute " + task
                      + " at this time since it didn't select a pool to work with." );
            return;
        }

        final Pool pool = m_serviceManager.getRetriever( Pool.class ).attain( task.getPoolId() );
        String partitionName = "";
        if ( pool.getPartitionId() != null )
        {
            partitionName = m_serviceManager.getRetriever( PoolPartition.class )
                    .attain( pool.getPartitionId() ).getName();
        }

        LOG.info( Platform.NEWLINE + LogUtil.getLogMessageHeaderBlock( "Execute " + task )
                + Platform.NEWLINE + "        Pool: " + task.getPoolId()
                + " (" + pool.getName() + ")"
                + Platform.NEWLINE + "   Partition: "
                + ( ( pool.getPartitionId() == null ) ? "N/A" :
                    pool.getPartitionId() + " (" + partitionName + ")" )
                + Platform.NEWLINE + "    Priority: " + task.getPriority() );

        m_taskWorkPool.submit( task );
    }


    synchronized private List< PoolTask > getSortedPoolTasks()
    {
        final List< PoolTask > retval = new ArrayList<>();
        retval.addAll( m_compactionTasks.values() );
        retval.addAll( m_importTasks.values() );
        retval.addAll( m_verifyTasks.values() );
        retval.addAll( m_ioTasks );

        retval.sort( new PoolTaskComparator() );
        return retval;
    }


    public Object getEnvironmentStateLock()
    {
        return m_poolEnvironmentStateLock;
    }


    public Set< BlobStoreTask > getTasks()
    {
        return new HashSet<>( getSortedPoolTasks() );
    }

    @Override
    public Set<? extends BlobStoreTask> getTasksForJob(final UUID jobId)
    {
        final Set< BlobStoreTask > retval = new HashSet<>();
        for ( final BlobStoreTask task : getSortedPoolTasks() )
        {
            final UUID[] jobIds = task.getJobIds();
            if ( null != jobIds )
            {
                for ( final UUID id : jobIds )
                {
                    if ( jobId.equals( id ) )
                    {
                        retval.add( task );
                        break;
                    }
                }
            }
        }
        return retval;
    }


    public boolean isAvailableOnPool( final UUID blobId )
    {
        final List< BlobPool > blobPools = getBlobPoolsFor( blobId );
        if ( !blobPools.isEmpty() )
        {
            final Set< UUID > poolIds = BeanUtils.extractPropertyValues(
                    blobPools, BlobPool.POOL_ID );
            final Map< UUID, Pool > pools = BeanUtils.toMap(
                    m_serviceManager.getRetriever( Pool.class ).retrieveAll( Require.all(
                            Require.beanPropertyEquals( Pool.STATE, PoolState.NORMAL ),
                            Require.beanPropertyEquals( Pool.QUIESCED, Quiesced.NO ),
                            Require.beanPropertyEqualsOneOf( Identifiable.ID, poolIds ) ) ).toSet() );
            for ( final BlobPool bp : new HashSet<>( blobPools ) )
            {
                if ( !pools.containsKey( bp.getPoolId() ) )
                {
                    LOG.info( "Cannot service GET from pool " + bp.getPoolId() + "." );
                    blobPools.remove( bp );
                }
            }
        }
        return !blobPools.isEmpty();
    }


    public void refreshEnvironmentNow()
    {
        ensurePhysicalPoolEnvironmentUpToDate( true );
    }


    private void restartPendingWork()
    {
        for ( final Pool pool : m_serviceManager.getRetriever( Pool.class )
                                                .retrieveAll( Require.beanPropertyEqualsOneOf( Pool.STATE,
                                                        PoolState.IMPORT_IN_PROGRESS, PoolState.IMPORT_PENDING ) )
                                                .toSet() )
        {
            m_serviceManager.getService( PoolService.class )
                            .update( pool.setState( PoolState.FOREIGN ), Pool.STATE );
            importPool( BlobStoreTaskPriority.NORMAL, m_serviceManager.getRetriever( ImportPoolDirective.class )
                                                                      .attain( ImportPoolDirective.POOL_ID,
                                                                              pool.getId() ) );
            new ThreadedTrashCollector().emptyTrash( PoolUtils.getTrashPath( pool ) );
        }
    }


    private final static class PoolTaskComparator implements Comparator< PoolTask >
    {
        public int compare( final PoolTask t1, final PoolTask t2 )
        {
            if ( t1.getPriority() != t2.getPriority() )
            {
                return t2.getPriority()
                         .ordinal() - t1.getPriority()
                                        .ordinal();
            }
            return ( int ) ( t2.getId() - t1.getId() );
        }
    } // end inner class def


    public boolean isChunkEntirelyAvailableOnPool( final UUID chunkId )
    {
        final JobEntry chunk = m_serviceManager.getRetriever(JobEntry.class).retrieve(chunkId);
        return isAvailableOnPool( chunk.getBlobId() );
    }


    private static final class BlobPoolComparator implements Comparator< BlobPool >
    {
        private BlobPoolComparator( final Map< UUID, Pool > pools )
        {
            m_pools = pools;
        }

        public int compare( final BlobPool bd1, final BlobPool bd2 )
        {
            final Pool p1 = m_pools.get( bd1.getPoolId() );
            final Pool p2 = m_pools.get( bd2.getPoolId() );
            if ( p1.getType() != p2.getType() )
            {
                return p2.getType().ordinal() - p1.getType().ordinal();
            }
            if ( p1.isPoweredOn() != p2.isPoweredOn() )
            {
                return ( p1.isPoweredOn() ) ? -1 : 1;
            }
            return 0;
        }

        private final Map< UUID, Pool > m_pools;
    } // end inner class def

    @Override
    public void taskSchedulingRequired() {
        m_poolTaskStarterExecutor.add( m_poolTaskStarter );
    }


    public void scheduleBlobReadLockReleaser()
    {
        m_blobReadLockReleaser.schedule();
    }


    public DiskManager getDiskManager()
    {
        return m_diskManager;
    }


    private volatile long m_poolEnvironmentGenerationNumber = -2;
    private final Duration m_durationSincePoolEnvironmentLastRefreshed = new Duration();
    private final Map< UUID, PoolTask > m_compactionTasks = new HashMap<>();
    private final Map< UUID, PoolTask > m_importTasks = new HashMap<>();
    private final Map< UUID, PoolTask > m_verifyTasks = new HashMap<>();
    //private final BasicIoTasks< PoolTask > m_ioTasks = new BasicIoTasks<>();
    private final HashSet<PoolTask> m_ioTasks = new HashSet<>();

    private final DiskManager m_diskManager;
    private final BeansServiceManager m_serviceManager;
    private final JobProgressManager m_jobProgressManager;
    private final PoolEnvironmentManager m_environmentManager;
    private final PoolLockSupport< PoolTask > m_lockSupport;
    private final Object m_poolEnvironmentStateLock = new Object();
    private final PoolEnvironmentResource m_poolEnvironmentResource;
    private final OfflineDurationTracker m_offlinePoolTracker = new OfflineDurationTracker();
    private final WorkPool m_taskWorkPool = WorkPoolFactory.createWorkPool( 20, "PoolTaskExecutor" );

    private final PoolTaskStarter m_poolTaskStarter = new PoolTaskStarter();
    private final ThrottledRunnableExecutor< PoolTaskStarter > m_poolTaskStarterExecutor =
            new ThrottledRunnableExecutor<>( 20, null, WhenAggregating.DELAY_EXECUTION );

    private final PeriodicPoolTaskStarter m_periodicPoolTaskStarter = new PeriodicPoolTaskStarter();
    private final ReclaimPoolProcessor m_reclaimPoolProcessor;
    private final BlobPoolReadLockReleaser m_blobReadLockReleaser;

    private final static Logger LOG = Logger.getLogger( PoolBlobStoreImpl.class );
    private final static long HOURS_BETWEEN_POOL_COMPACTION = 4;
}
