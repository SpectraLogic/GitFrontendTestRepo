package com.spectralogic.s3.common.platform.persistencetarget;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.S3ObjectsPersistedNotificationPayloadGenerator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.iterate.CloseableIterable;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
public final class BlobDestinationUtils {

    public static boolean createLocalBlobDestinations(final Collection<JobEntry> entries, final Collection<DataPersistenceRule> rules, UUID bucketId, final BeansServiceManager transaction) {
        final Map<UUID, UUID> isolatedBucketIdsByRule = new HashMap<>();
        for (DataPersistenceRule rule : rules) {
            final UUID isolatedBucketId = rule.getIsolationLevel() == DataIsolationLevel.BUCKET_ISOLATED ? bucketId : null;
            isolatedBucketIdsByRule.put(rule.getId(), isolatedBucketId);
        };
        final Set<LocalBlobDestination> persistenceTargets = entries.stream()
                .flatMap(entry -> rules.stream()
                        .map(rule -> BeanFactory.newBean(LocalBlobDestination.class)
                                .setEntryId(entry.getId())
                                .setStorageDomainId(rule.getStorageDomainId())
                                .setIsolatedBucketId(isolatedBucketIdsByRule.get(rule.getId()))
                                .setPersistenceRuleId(rule.getId())

                        )
                ).collect(Collectors.toSet());
        transaction.getService(LocalBlobDestinationService.class).create(persistenceTargets);
        return !persistenceTargets.isEmpty();
    }

    public static boolean createDs3BlobDestinations(final Collection<JobEntry> entries, final Collection<Ds3DataReplicationRule> rules, final BeansServiceManager transaction) {
        final Set<Ds3BlobDestination> chunkTargetsForDs3 = entries.stream()
                .flatMap(entry -> rules.stream()
                        .map(rule -> BeanFactory.newBean(Ds3BlobDestination.class)
                                .setEntryId(entry.getId())
                                .setTargetId(rule.getTargetId())
                                .setRuleId(rule.getId())
                        )
                ).collect(Collectors.toSet());
        transaction.getService(Ds3BlobDestinationService.class).create(chunkTargetsForDs3);
        return !chunkTargetsForDs3.isEmpty();
    }

    public static boolean createAzureBlobDestinations(final Collection<JobEntry> entries, final Collection<AzureDataReplicationRule> rules, final BeansServiceManager transaction) {
        final Set<AzureBlobDestination> chunkTargetsForAzure = entries.stream()
                .flatMap(entry -> rules.stream()
                        .map(rule -> BeanFactory.newBean(AzureBlobDestination.class)
                                .setEntryId(entry.getId())
                                .setTargetId(rule.getTargetId())
                                .setRuleId(rule.getId())
                        )
                ).collect(Collectors.toSet());
        transaction.getService(AzureBlobDestinationService.class).create(chunkTargetsForAzure);
        return !chunkTargetsForAzure.isEmpty();
    }

    public static boolean createS3BlobDestinations(final Collection<JobEntry> entries, final Collection<S3DataReplicationRule> rules, final BeansServiceManager transaction) {
        final Set<S3BlobDestination> chunkTargetsForS3 = entries.stream()
                .flatMap(entry -> rules.stream()
                        .map(rule -> BeanFactory.newBean(S3BlobDestination.class)
                                .setEntryId(entry.getId())
                                .setTargetId(rule.getTargetId())
                                .setRuleId(rule.getId())
                        )
                ).collect(Collectors.toSet());
        transaction.getService(S3BlobDestinationService.class).create(chunkTargetsForS3);
        return !chunkTargetsForS3.isEmpty();
    }

    public static void cleanupCompletedEntriesAndDestinations(final BeansServiceManager serviceManager, final JobProgressManager jobProgressManager) {
        final WhereClause persistedSomewhere = Require.any(
                Require.exists(
                        LocalBlobDestination.class,
                        LocalBlobDestination.ENTRY_ID,
                        Require.nothing()
                ),
                Require.exists(
                        AzureBlobDestination.class,
                        AzureBlobDestination.ENTRY_ID,
                        Require.nothing()
                ),
                Require.exists(
                        Ds3BlobDestination.class,
                        Ds3BlobDestination.ENTRY_ID,
                        Require.nothing()
                ),
                Require.exists(
                        S3BlobDestination.class,
                        S3BlobDestination.ENTRY_ID,
                        Require.nothing()
                ));
        final WhereClause persistenceComplete = Require.all(
                Require.every(
                        LocalBlobDestination.class,
                        LocalBlobDestination.ENTRY_ID,
                        Require.beanPropertyEquals(
                                LocalBlobDestination.BLOB_STORE_STATE,
                                JobChunkBlobStoreState.COMPLETED
                        )
                ),
                Require.every(
                        AzureBlobDestination.class,
                        AzureBlobDestination.ENTRY_ID,
                        Require.beanPropertyEquals(
                                AzureBlobDestination.BLOB_STORE_STATE,
                                JobChunkBlobStoreState.COMPLETED
                        )
                ),
                Require.every(
                        Ds3BlobDestination.class,
                        Ds3BlobDestination.ENTRY_ID,
                        Require.beanPropertyEquals(
                                Ds3BlobDestination.BLOB_STORE_STATE,
                                JobChunkBlobStoreState.COMPLETED
                        )
                ),
                Require.every(
                        S3BlobDestination.class,
                        S3BlobDestination.ENTRY_ID,
                        Require.beanPropertyEquals(
                                S3BlobDestination.BLOB_STORE_STATE,
                                JobChunkBlobStoreState.COMPLETED
                        )
                ));

        // This is for staged jobs.
        final WhereClause isIomStaged =  Require.beanPropertyEquals(
                DetailedJobEntry.IOM_TYPE,
                IomType.STAGE
        );

        final WhereClause persistedOrIomStaged = Require.any(persistedSomewhere, isIomStaged);

        try (final CloseableIterable<Set<DetailedJobEntry>> batches = serviceManager.getService(DetailedJobEntryService.class).retrieveAll(
                Require.all(
                        persistedOrIomStaged,
                        persistenceComplete
                )).toSetsOf(MAX_BATCH_SIZE)) {
            for (final Set<DetailedJobEntry> batch : batches) {
                final Map<UUID, DetailedJobEntry> writeEntriesWithNoWorkRemaining = BeanUtils.toMap(batch);
                final Set<UUID> objectIds = BeanUtils.extractPropertyValues(writeEntriesWithNoWorkRemaining.values(), DetailedJobEntry.OBJECT_ID);
                for (final DetailedJobEntry entry : writeEntriesWithNoWorkRemaining.values()) {
                    jobProgressManager.workCompleted(entry.getJobId(), entry.getLength());
                }
                final WhereClause entryHasNoWorkRemaining = Require.beanPropertyEqualsOneOf(Identifiable.ID, writeEntriesWithNoWorkRemaining.keySet());
                //NOTE: in most cases this will be an update followed by a delete
                serviceManager.getService(JobEntryService.class).update(
                        entryHasNoWorkRemaining,
                        (chunk) -> chunk.setPendingTargetCommit(true),
                        JobEntry.PENDING_TARGET_COMMIT);
                serviceManager.getService(JobEntryService.class).delete(entryHasNoWorkRemaining);
                if (!writeEntriesWithNoWorkRemaining.isEmpty()) {
                    final Map<UUID, Set<JobEntry>> entriesByJobId = new HashMap<>();
                    for (final JobEntry entry : writeEntriesWithNoWorkRemaining.values()) {
                        final UUID jobId = entry.getJobId();
                        if (!entriesByJobId.containsKey(jobId)) {
                            entriesByJobId.put(jobId, new HashSet<>());
                        }
                        entriesByJobId.get(jobId).add(entry);
                    }
                    for (final UUID jobId : entriesByJobId.keySet()) {
                        serviceManager.getNotificationEventDispatcher().fire(new JobNotificationEvent(
                                serviceManager.getRetriever(Job.class).attain(jobId),
                                serviceManager.getRetriever(S3ObjectPersistedNotificationRegistration.class),
                                new S3ObjectsPersistedNotificationPayloadGenerator(
                                        entriesByJobId.get(jobId),
                                        serviceManager.getRetriever(S3Object.class),
                                        serviceManager.getRetriever(Blob.class))));
                    }
                }
                serviceManager.getService(S3ObjectService.class).deleteLegacyObjectsIfEntirelyPersisted(objectIds);
                final Set<JobEntry> errorEntries = serviceManager.getService(JobEntryService.class).retrieveAll(
                        Require.all(
                                Require.not(persistedSomewhere),
                                persistenceComplete
                        )).toSet();
                if (!errorEntries.isEmpty()) {
                    final Set<UUID> blobs = BeanUtils.extractPropertyValues(errorEntries, JobEntry.BLOB_ID);
                    LOG.error(errorEntries.size() + " entries were found for " + blobs.size() + " blobs were found marked complete but have no completed destinations." +
                            " Blob IDs: " + blobs);
                }
            }
        }
    }
    private final static int MAX_BATCH_SIZE = 10000;
    private final static Logger LOG = Logger.getLogger( BlobDestinationUtils.class );
}
