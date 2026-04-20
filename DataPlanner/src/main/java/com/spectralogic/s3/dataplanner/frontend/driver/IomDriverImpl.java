package com.spectralogic.s3.dataplanner.frontend.driver;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.ds3.JobCreationFailedService;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.common.rpc.dataplanner.domain.PersistenceTargetInfo;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.frontend.dataorder.Ds3TargetBlobPhysicalPlacementImpl;

import com.spectralogic.s3.dataplanner.frontend.dataorder.GetByPhysicalPlacementDataOrderingStrategy;
import com.spectralogic.s3.dataplanner.frontend.dataorder.PublicCloudBlobSupport;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import org.apache.log4j.Logger;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.orm.DataPolicyRM;
import com.spectralogic.s3.common.dao.service.composite.IomService;
import com.spectralogic.s3.common.dao.service.composite.IomServiceImpl;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.domain.BlobApiBean;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsCachedNotificationPayload;
import com.spectralogic.s3.common.platform.notification.domain.payload.S3ObjectsPersistedNotificationPayload;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreateGetJobParams;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.frontend.api.IomDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.iterate.CloseableIterable;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.notification.dispatch.NotificationListener;
import com.spectralogic.util.notification.domain.Notification;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

import static com.spectralogic.s3.common.dao.service.composite.IomService.MAX_BLOBS_IN_IOM_JOB;


public class IomDriverImpl extends BaseShutdownable implements IomDriver, NotificationListener {

    public IomDriverImpl(
            final BeansServiceManager serviceManager,
            final DataPlannerResource plannerResource,
            final CacheManager cacheManager,
            final JobProgressManager jobProgressManager,
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final int intervalInMillisToCheckForNewWork) {
        m_serviceManager = serviceManager;

        m_cacheManager = cacheManager;
        m_ds3ConnectionFactory = ds3ConnectionFactory;
        m_iomService = new IomServiceImpl(m_serviceManager);
        m_plannerResource = plannerResource;
        m_jobProgressManager = jobProgressManager;

        m_worker = new IomDriverWorker();
        m_executor = new RecurringRunnableExecutor(
                m_worker,
                intervalInMillisToCheckForNewWork);
        m_serviceManager.getNotificationEventDispatcher()
                .registerListener(this, S3ObjectPersistedNotificationRegistration.class);
        m_serviceManager.getNotificationEventDispatcher()
                .registerListener(this, S3ObjectCachedNotificationRegistration.class);
        m_executor.start();
    }


    public void driveNewWork() {
        m_worker.run();
    }


    private final class IomDriverWorker implements Runnable {
        public void run() {
            if (workerLock.tryLock()) {
                try {
                    LOG.info("Running IOM driver worker...");
                    m_iomService.cleanupOldMigrations((d) -> {
                    });
                    handleMigrationsInError();
                    if (m_iomService.isIomEnabled()) {
                        m_iomService.markTapesForAutoCompaction();
                    }
                    m_iomService.markTapesFinishedAutoCompacting();
                    if (m_iomService.isIomEnabled() && m_iomService.isNewJobCreationAllowed()) {
                        createIomJobs();
                    }
                    m_iomService.deleteBlobRecordsThatHaveFinishedMigratingOrHealing();
                    m_iomService.removeDegradedBlobsThatHaveBeenHealed(null);
                    m_iomService.handleDataPlacementRulesFinishedPendingInclusion();
                    m_iomService.handleStorageDomainMembersFinishedPendingExclusion();
                } finally {
                    workerLock.unlock();
                }
            }

        }

        private final Lock workerLock = new ReentrantLock();
    } // end inner class def


    public void fire(final Notification event) {
        if (event.getEvent() instanceof S3ObjectsPersistedNotificationPayload) {
            final S3ObjectsPersistedNotificationPayload payload =
                    (S3ObjectsPersistedNotificationPayload) event.getEvent();
            final UUID getJobId = m_serviceManager.getService(JobService.class)
                    .getGetJobComponentOfDataMigration(payload.getJobId());
            final Collection<UUID> blobIds = new HashSet<>();
            if (null != getJobId) {
                for (final BlobApiBean blob : payload.getObjects()) {
                    m_plannerResource.blobReadCompleted(getJobId, blob.getId());
                    blobIds.add(blob.getId());
                }
                try {
                    m_iomService.removeDegradedBlobsThatHaveBeenHealed(blobIds);
                    m_iomService.handleDataPlacementRulesFinishedPendingInclusion();
                    m_iomService.handleStorageDomainMembersFinishedPendingExclusion();
                } catch (final RuntimeException e) {
                    LOG.warn("Failed to perform routine IOM actions in response to"
                            + "	Objects Persisted Notification.", e);
                }
            }
        } else if (event.getEvent() instanceof S3ObjectsCachedNotificationPayload) {
            final S3ObjectsCachedNotificationPayload payload =
                    (S3ObjectsCachedNotificationPayload) event.getEvent();
            final UUID putJobId = m_serviceManager.getService(JobService.class)
                    .getPutJobComponentOfDataMigration(payload.getJobId());
            if (null != putJobId) {
                for (final BlobApiBean blob : payload.getObjects()) {
                    m_jobProgressManager.blobLoadedToCache(putJobId, blob.getLength());
                }
                return;
            }
        }
    }


    private void handleMigrationsInError() {
        for (final DataMigration d : m_iomService.getMigrationsInError()) {
            //NOTE: we only cancel one or the other since canceling one job should automatically
            //cancel the other 
            if (null != d.getGetJobId()) {
                m_plannerResource.cancelJobInternal(d.getGetJobId(), false);
            } else if (null != d.getPutJobId()) {
                m_plannerResource.cancelJobInternal(d.getPutJobId(), false);
            }
        }
    }


    private void cancelJobsForMigration(final DataMigration migration) {
        if (null != migration.getGetJobId()) {
            LOG.warn("Corresponding PUT job for the IOM GET job " + migration.getGetJobId() + " appears to have been "
                    + "canceled. Canceling GET job.");
            m_plannerResource.cancelJobInternal(migration.getGetJobId(), false);
        }
        if (null != migration.getPutJobId()) {
            LOG.warn("Corresponding GET job for the IOM PUT job " + migration.getPutJobId() + " appears to have been "
                    + "canceled. Canceling PUT job.");
            m_plannerResource.cancelJobInternal(migration.getPutJobId(), false);
        }
    }


    private void createIomJobs() {
        //NOTE: we check once here to avoid unnecessary querying, and once later to stop looping once we
        //hit the max number of migrations
        if (MAX_CONCURRENT_DATA_MIGRATIONS <=
                m_serviceManager.getRetriever(DataMigration.class).getCount()) {
            return;
        }
        final User admin = m_serviceManager.getRetriever(User.class)
                .attain(Require.beanPropertyEquals(NameObservable.NAME, ADMIN_USER_NAME));
        final double iomCacheLimitationPercent = m_serviceManager.getRetriever(DataPathBackend.class)
                .attain(Require.nothing())
                .getIomCacheLimitationPercent();
        final boolean iomCacheLimited = (iomCacheLimitationPercent < 1.0);
        LOG.info("IOM iomCacheLimitationPercent=" + iomCacheLimitationPercent
                + " iomCacheLimited=" + iomCacheLimited);
        long iomCacheLimitInBytes = 0;
        long iomCacheInUseInBytes = 0;
        boolean iomCacheLimitReached = false;
        boolean allowFirstBlob = true;
        if (iomCacheLimited) {
            long cacheSizeInBytes = m_cacheManager.getCacheSizeInBytes();
            iomCacheLimitInBytes = (long) (cacheSizeInBytes * iomCacheLimitationPercent);

            RetrieveBeansResult<DataMigration> activeIomJobs = m_serviceManager.getRetriever(DataMigration.class).retrieveAll();
            int iomActiveJobs = 0;
            for (DataMigration dataMigration : activeIomJobs.toSet()) {
                if (dataMigration.getPutJobId() != null) {
                    final Job putJob = m_serviceManager.getService(JobService.class)
                            .retrieve(dataMigration.getPutJobId());
                    if (putJob != null) {
                        iomActiveJobs++;
                        iomCacheInUseInBytes += putJob.getOriginalSizeInBytes();
                    }
                }
            }
            iomCacheLimitReached = (iomCacheInUseInBytes >= iomCacheLimitInBytes);
            allowFirstBlob = (iomActiveJobs == 0);
            LOG.info("IOM cache limitation: " + iomCacheLimitInBytes + " of " + cacheSizeInBytes
                    + " there are " + iomActiveJobs + " active PUT jobs using " + iomCacheInUseInBytes + " bytes.");
        }

        final RetrieveBeansResult<Bucket> buckets;
        try {
            buckets = m_serviceManager.getRetriever(Bucket.class)
                    .retrieveAll();
        } catch (RuntimeException e) {
            LOG.warn("IOM is currently not supported for systems with over 500k buckets.", e);
            return;
        }
        long totalBlobs = 0;
        for (final Bucket bucket : buckets.toSet()) {
            final Set<DataPersistenceRule> dataPersistenceRules =
                    m_iomService.getPermanentPersistenceRulesForBucket(bucket.getId());
            for (final DataPersistenceRule rule : dataPersistenceRules) {
                try (EnhancedIterable<Blob> blobsItr = m_iomService.getBlobsRequiringLocalIOMWork(
                        bucket.getId(),
                        rule)) {
                    Set<Blob> blobs = new HashSet<>();
                    for (Blob blob : blobsItr) {
                        if (iomCacheLimited) {
                            long blobSize = blob.getLength();
                            long cacheRemaining = iomCacheLimitInBytes - iomCacheInUseInBytes;
                            if (allowFirstBlob || blobSize < cacheRemaining) {
                                iomCacheInUseInBytes += blobSize;
                                cacheRemaining -= blobSize;
                            } else {
                                iomCacheLimitReached = true;
                                LOG.info("IOM cache limit reached.");
                                break;
                            }
                            if (blobs.size() > 0 && blobs.size() % 10000 == 0) {
                                LOG.info("IOM " + blobs.size()
                                        + " blobs for bucket " + bucket.getName()
                                        + " : cache status: " + iomCacheInUseInBytes
                                        + " of " + iomCacheLimitInBytes
                                        + " leaving " + cacheRemaining);
                            }
                            allowFirstBlob = false;
                        }

                        if (!iomCacheLimitReached) {

                            if (blobs.size() >= MAX_BLOBS_IN_IOM_JOB) {
                                if (!createIomGetJob(blobs, bucket, admin, rule)) {
                                    return;
                                }
                                blobs = new HashSet<>();
                            }
                            blobs.add(blob);
                            totalBlobs++;
                        }
                    }

                    if (blobs.size() > 0) {
                        if (!createIomGetJob(blobs, bucket, admin, rule)) {
                            return;
                        }
                    }
                }

                if (iomCacheLimitReached) {
                    break;
                }
            }

            final Set<Ds3DataReplicationRule> ds3ReplicationRules =
                    new DataPolicyRM(bucket.getDataPolicyId(), m_serviceManager)
                            .getDs3DataReplicationRules().toSet();
            final Set<S3DataReplicationRule> s3ReplicationRules =
                    new DataPolicyRM(bucket.getDataPolicyId(), m_serviceManager)
                            .getS3DataReplicationRules().toSet();
            final Set<AzureDataReplicationRule> azureReplicationRules =
                    new DataPolicyRM(bucket.getDataPolicyId(), m_serviceManager)
                            .getAzureDataReplicationRules().toSet();
            for (final Ds3DataReplicationRule rule : ds3ReplicationRules) {
                createMigrationsForTarget(
                        BlobDs3Target.class, SuspectBlobDs3Target.class, bucket, rule.getTargetId(), admin);
            }
            for (final S3DataReplicationRule rule : s3ReplicationRules) {
                createMigrationsForTarget(
                        BlobS3Target.class, SuspectBlobS3Target.class, bucket, rule.getTargetId(), admin);
            }
            for (final AzureDataReplicationRule rule : azureReplicationRules) {
                createMigrationsForTarget(
                        BlobAzureTarget.class, SuspectBlobAzureTarget.class, bucket, rule.getTargetId(), admin);
            }

            if (iomCacheLimitReached) {
                break;
            }
        }
        LOG.info("IOM createIomJobs done, processed blobs " + totalBlobs);
    }


    private boolean createIomGetJob(Set<Blob> batch, Bucket bucket, User admin, DataPersistenceRule rule) {
        int dataMigrations = m_serviceManager.getRetriever(DataMigration.class).getCount();
        if (MAX_CONCURRENT_DATA_MIGRATIONS <= dataMigrations) {
            LOG.info("IOM too many data migrations running: " + dataMigrations);
            return false;
        }
        removeUnavailableBlobs(batch);
        final UUID[] batchIds = CollectionFactory.toArray(
                UUID.class,
                BeanUtils.extractPropertyValues(batch, Identifiable.ID));
        if (0 < batchIds.length) {
            LOG.info("IOM queuing GET job for bucket " + bucket.getName()
                    + " with batch of " + batch.size() + " blobs");
            final CreateGetJobParams getJobParams = BeanFactory.newBean(CreateGetJobParams.class)
                    .setName("IOM")
                    .setUserId(admin.getId())
                    .setPriority(BlobStoreTaskPriority.LOW)
                    .setChunkOrderGuarantee(JobChunkClientProcessingOrderGuarantee.NONE)
                    .setAggregating(false)
                    .setNaked(false)
                    .setImplicitJobIdResolution(false)
                    .setBlobIds(batchIds)
                    .setIomType(IomType.STANDARD_IOM);
            final PersistenceTargetInfo pti = BeanFactory.newBean(PersistenceTargetInfo.class)
                    .setStorageDomainIds(new UUID[]{rule.getStorageDomainId()})
                    .setDs3TargetIds(new UUID[0])
                    .setS3TargetIds(new UUID[0])
                    .setAzureTargetIds(new UUID[0]);
            m_plannerResource.createIomJob(getJobParams, pti)
                    .get(Timeout.LONG);
        }
        return true;
    }


    private <T extends DatabasePersistable & BlobTarget<?>, S extends T> void createMigrationsForTarget(
            final Class<T> blobTargetType,
            final Class<S> suspectBlobTargetType,
            final Bucket bucket,
            final UUID targetId,
            final User user) {
        try (final CloseableIterable<Set<Blob>> blobs = m_iomService.getBlobsRequiringIOMWorkOnTarget(
                blobTargetType,
                suspectBlobTargetType,
                bucket.getId(),
                targetId,
                bucket.getDataPolicyId())) {

            for (final Set<Blob> batch : blobs) {
                if (MAX_CONCURRENT_DATA_MIGRATIONS <=
                        m_serviceManager.getRetriever(DataMigration.class).getCount()) {
                    break;
                }
                removeUnavailableBlobs(batch);
                final UUID[] batchIds = CollectionFactory.toArray(
                        UUID.class,
                        BeanUtils.extractPropertyValues(batch, Identifiable.ID));
                if (0 < batchIds.length) {
                    final CreateGetJobParams getJobParams = BeanFactory.newBean(CreateGetJobParams.class)
                            .setName("IOM")
                            .setUserId(user.getId())
                            .setPriority(BlobStoreTaskPriority.LOW)
                            .setChunkOrderGuarantee(JobChunkClientProcessingOrderGuarantee.NONE)
                            .setAggregating(false)
                            .setNaked(false)
                            .setImplicitJobIdResolution(false)
                            .setBlobIds(batchIds)
                            .setIomType(IomType.STANDARD_IOM);
                    final PersistenceTargetInfo pti = BeanFactory.newBean(PersistenceTargetInfo.class)
                            .setStorageDomainIds(new UUID[0])
                            .setDs3TargetIds(new UUID[0])
                            .setS3TargetIds(new UUID[0])
                            .setAzureTargetIds(new UUID[0]);
                    if (blobTargetType.equals(BlobDs3Target.class)) {
                        pti.setDs3TargetIds(new UUID[]{targetId});
                    } else if (blobTargetType.equals(BlobS3Target.class)) {
                        pti.setS3TargetIds(new UUID[]{targetId});
                    } else if (blobTargetType.equals(BlobAzureTarget.class)) {
                        pti.setAzureTargetIds(new UUID[]{targetId});
                    } else {
                        throw new IllegalStateException("Illegal blob target type: " + blobTargetType);
                    }
                    m_plannerResource.createIomJob(getJobParams, pti)
                            .get(Timeout.LONG);
                }
            }
        }
    }



    private Set<UUID> findObjectsOnPublicCloud(Set<Blob> blobs) {
        if (blobs.isEmpty()) {
            return Collections.emptySet();
        }
        return findAndUpdatePublicCloudBlobs(blobs);
    }

    private Set<UUID> findAndUpdatePublicCloudBlobs(Set<Blob> blobs) {
        Set<UUID> blobIds = BeanUtils.extractPropertyValues(blobs, Identifiable.ID);
        final Set<UUID> blobIdsTarget = new HashSet<>();
        PublicCloudBlobSupport<?, ?, ?> azureSupport = new PublicCloudBlobSupport<>(
                AzureTarget.class,
                BlobAzureTarget.class,
                AzureTargetReadPreference.class,
                blobIds,
                m_serviceManager);
        PublicCloudBlobSupport<?, ?, ?> s3Support = new PublicCloudBlobSupport<>(
                S3Target.class,
                BlobS3Target.class,
                S3TargetReadPreference.class,
                blobIds,
                m_serviceManager);

        Set<TargetReadPreferenceType>  readPreferences = Set.of(TargetReadPreferenceType.AFTER_ONLINE_POOL ,TargetReadPreferenceType.AFTER_NEARLINE_POOL ,TargetReadPreferenceType.AFTER_NON_EJECTABLE_TAPE,TargetReadPreferenceType.LAST_RESORT);
        for (TargetReadPreferenceType pref : readPreferences) {
            addExistingBlobsPublicCloud(blobIdsTarget, azureSupport, pref);
            addExistingBlobsPublicCloud(blobIdsTarget, s3Support, pref);
        }
        return blobIdsTarget;
    }

    private void addExistingBlobsPublicCloud(Set<UUID> blobIdsTarget, PublicCloudBlobSupport<?, ?, ?> cloudSupport, TargetReadPreferenceType pref) {
        Map<UUID, Set<UUID>> cloudBlobs;
        cloudBlobs = cloudSupport.getBlobs(pref);
        for (final Map.Entry<UUID, Set<UUID>> e : cloudBlobs.entrySet()) {
            for (UUID blobId : e.getValue()) {
                if (!isCloudBlobSuspect(blobId, e.getKey())) {
                    blobIdsTarget.add(blobId);
                }
            }

        }
    }

    private boolean isCloudBlobSuspect(UUID blobId, UUID targetId) {
        Set<SuspectBlobS3Target> suspectS3Blobs = m_serviceManager.getRetriever(SuspectBlobS3Target.class).retrieveAll(Require.all(
                Require.beanPropertyEqualsOneOf(
                        BlobTarget.TARGET_ID, targetId),
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobId))).toSet();
        Set<SuspectBlobAzureTarget> suspectAzureBlobs = m_serviceManager.getRetriever(SuspectBlobAzureTarget.class).retrieveAll(Require.all(
                Require.beanPropertyEqualsOneOf(
                        BlobTarget.TARGET_ID, targetId),
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobId))).toSet();
        return !suspectS3Blobs.isEmpty() || !suspectAzureBlobs.isEmpty() ;

    }


    private Set< UUID > findObjectsOnDs3Target(  Set<Blob> blobs )
    {
        if (blobs.isEmpty()) {
            return Collections.emptySet();
        }
        Set<UUID> blobIds = BeanUtils.extractPropertyValues( blobs, Identifiable.ID );
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try {
            Ds3TargetBlobPhysicalPlacementImpl ds3TargetBlobPhysicalPlacement = new Ds3TargetBlobPhysicalPlacementImpl(
                    blobIds,
                    transaction,
                    m_ds3ConnectionFactory);
            final Set< UUID > blobIdsTarget = new HashSet<>();
            for ( final UUID targetId : ds3TargetBlobPhysicalPlacement.getCandidateTargets() )
            {
                blobIdsTarget.addAll( ds3TargetBlobPhysicalPlacement.getBlobsOnPool( targetId ) );
                blobIdsTarget.addAll( ds3TargetBlobPhysicalPlacement.getBlobsOnTape( targetId ) );
            }

            return blobIdsTarget;
        } finally {
            transaction.closeTransaction();
        }
    }

    private Set<UUID> addObjectsOnTape( Set<Blob> availableBlobs, boolean useUnavailableMedia, final boolean searchOnEjectableMedia ) {
        if (availableBlobs.isEmpty()) {
            return Collections.emptySet();
        }
        final Set< UUID > blobIds = BeanUtils.extractPropertyValues( availableBlobs, Identifiable.ID );
        Set<UUID> tapeBlobIds = new HashSet<>();
        Set<BlobTape> blobs = PersistenceTargetUtil.findBlobTapesAvailableNow(
                m_serviceManager.getRetriever(BlobTape.class),
                blobIds,
                useUnavailableMedia,
                Boolean.valueOf(searchOnEjectableMedia));
        for (final BlobTape blobTape : blobs)
        {
            tapeBlobIds.add(blobTape.getBlobId());
        }
        return tapeBlobIds;
    }

    private Set<UUID> findBlobsOnPool ( Set<Blob> blobs, boolean useUnavailableMedia, PoolType poolType) {
        if (blobs.isEmpty()) {
            return Collections.emptySet();
        }
        final Set< UUID > blobIds = BeanUtils.extractPropertyValues( blobs, Identifiable.ID );
        Set<BlobPool> objectsOnPool = PersistenceTargetUtil.findBlobPoolsAvailableNow(
                m_serviceManager.getRetriever(BlobPool.class),
                blobIds,
                poolType,
                useUnavailableMedia);
        return BeanUtils.extractPropertyValues( objectsOnPool, BlobObservable.BLOB_ID );
    }

    public void removeUnavailableBlobs( final Set< Blob > blobs )
    {
        //allBlobs gets updated based on the availability of blobs in target.
       // final Set<Blob> allBlobs = new HashSet<>(blobs);
        Set<UUID> blobIds = BeanUtils.extractPropertyValues( blobs, Identifiable.ID );
        final GetByPhysicalPlacementDataOrderingStrategy placement = getPhysicalPlacementOrderingStrategy(blobIds, false);
        Set<UUID> unavailableBlobIds = placement.getUnavailableBlobs();

        final Set< Blob > missingBlobs = new HashSet<>();
        for ( final Blob b : blobs )
        {
            if ( unavailableBlobIds.contains( b.getId() ) )
            {
                missingBlobs.add( b );
            }
        }
        for ( final Blob b : missingBlobs )
        {
            blobs.remove( b );
        }

        if (!unavailableBlobIds.isEmpty()) {
            // The blobs here are not available in any read targets.
            if ( m_missingBlobIdsDuration.getElapsedMinutes() > KNOWN_MISSING_BLOBS_RESET_IN_MINUTES )
            {
                m_missingBlobIdsDuration.reset();
                m_knownMissingBlobIds.clear();
            }
            unavailableBlobIds.removeAll(m_knownMissingBlobIds);
            if (!unavailableBlobIds.isEmpty()) {
                try {
                    getPhysicalPlacementOrderingStrategy( unavailableBlobIds, true ).getReadOrdering();
                }catch ( final RuntimeException e)
                {
                    LOG.warn( unavailableBlobIds.size() + " blobs require IOM work but are not available ", e);
                    m_serviceManager.getService(JobCreationFailedService.class).create(
                            ADMIN_USER_NAME,
                            JobCreationFailedType.TAPES_MUST_BE_ONLINED,
                            new ArrayList<>(),
                            "IOM is unable to progress on work for " + unavailableBlobIds.size() + " blobs because: "
                                    + e.getMessage(),
                            60
                    );
                }
            }
            blobs.removeAll(unavailableBlobIds);
        }
    }

    private GetByPhysicalPlacementDataOrderingStrategy getPhysicalPlacementOrderingStrategy(
            final Set< UUID > blobIds,
            final boolean failIfBlobsMissing )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final User admin = m_serviceManager.getRetriever(User.class)
                    .attain(Require.beanPropertyEquals(NameObservable.NAME, ADMIN_USER_NAME));
            final GetByPhysicalPlacementDataOrderingStrategy placement = new GetByPhysicalPlacementDataOrderingStrategy(blobIds,
                    transaction,
                    m_cacheManager,
                    JobRequestType.GET,
                    JobChunkClientProcessingOrderGuarantee.NONE,
                    new Ds3TargetBlobPhysicalPlacementImpl(
                            blobIds,
                            m_serviceManager,
                            m_ds3ConnectionFactory),
                    failIfBlobsMissing,
                    false,
                    admin.getName());
            placement.getReadOrdering();

            transaction.commitTransaction();
            return placement;
        }
        finally
        {
            transaction.closeTransaction();
        }
    }

    private final BeansServiceManager m_serviceManager;
    private final Ds3ConnectionFactory m_ds3ConnectionFactory;
    private final CacheManager m_cacheManager;
    private final IomService m_iomService;
	private final DataPlannerResource m_plannerResource;
	private final RecurringRunnableExecutor m_executor;
	private final IomDriverWorker m_worker;
	private final JobProgressManager m_jobProgressManager;
	private final static Logger LOG = Logger.getLogger( IomDriverImpl.class );
	private final static String ADMIN_USER_NAME = "Administrator";
	private final static int MAX_CONCURRENT_DATA_MIGRATIONS = 10;
    private final Set<UUID> m_knownMissingBlobIds = new HashSet<>();
    private final Duration m_missingBlobIdsDuration = new Duration();
    private final static int KNOWN_MISSING_BLOBS_RESET_IN_MINUTES = 60 * 12;
}
