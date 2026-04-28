package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class TapeWorkAggregationUtils {

    public static boolean anyTapeIOWorkOfPriority(final BeansServiceManager serviceManager, final BlobStoreTaskPriority exactPriority) {
        final WhereClause priorityFilter = Require.beanPropertyEquals(LocalJobEntryWork.PRIORITY, exactPriority);
        return serviceManager.getRetriever(LocalJobEntryWork.class).any(
                Require.all(
                        priorityFilter,
                        Require.any( getReadOrVerifyFilter(null, null), getWriteFilter(null))
                )
        );
    }

    public static boolean anyRegularPriorityTapeIOWork(final BeansServiceManager serviceManager) {
        final WhereClause priorityFilter = Require.beanPropertyEqualsOneOf(LocalJobEntryWork.PRIORITY, BlobStoreTaskPriority.prioritiesLessThan(BlobStoreTaskPriority.URGENT));
        return serviceManager.getRetriever(LocalJobEntryWork.class).any(
                Require.all(
                        priorityFilter,
                        Require.any( getReadOrVerifyFilter(null, null), getWriteFilter(null))
                )
        );
    }

    public static List<IODirective> discoverTapeWorkAggregated(
            final BeansServiceManager serviceManager,
            final BlobStoreTaskPriority minPriority,
            final UUID tapePartitionId,
            final UUID tapeId) {
        final Map<TapeWorkAggregationKey, Set<LocalJobEntryWork>> workByKey = new LinkedHashMap<>();

        final WhereClause priorityFilter = Require.beanPropertyEqualsOneOf(LocalJobEntryWork.PRIORITY, BlobStoreTaskPriority.prioritiesOfAtLeast(minPriority));

        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add(LocalJobEntryWork.ORDER_INDEX, SortBy.Direction.ASCENDING);

        try(final EnhancedIterable<LocalJobEntryWork> allTapeIOWork = serviceManager.getRetriever(LocalJobEntryWork.class)
                .retrieveAll(
                        Query.where(Require.all(
                            Require.any( getReadOrVerifyFilter(tapeId, tapePartitionId), getWriteFilter(tapePartitionId)),
                            priorityFilter
                        )).orderBy(ordering)).toIterable()) {
            for (final LocalJobEntryWork work : allTapeIOWork) {
                Job job = serviceManager.getRetriever(Job.class).attain(work.getJobId());
                final TapeWorkAggregationKey key = new TapeWorkAggregationKey(
                        work.getRequestType(),
                        work.getPriority(),
                        work.getBucketId(),
                        work.getRequestType() != JobRequestType.PUT ? work.getReadFromTapeId() : null,
                        work.getRequestType() == JobRequestType.PUT ? work.getStorageDomainId() : null,
                        work.getRequestType() == JobRequestType.PUT ? work.getIomType() : null,
                        job.isMinimizeSpanningAcrossMedia()
                );

                workByKey.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(work);
            }
        }

        return workByKey.entrySet().stream()
                .map(entry -> createIODirectiveFromAggregatedWork(entry.getKey(), entry.getValue(), serviceManager))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static IODirective createIODirectiveFromAggregatedWork(TapeWorkAggregationKey key, Set<LocalJobEntryWork> work, BeansServiceManager serviceManager) {
        final List<JobEntry> entries = new ArrayList<>();
        final Set<UUID> localBlobDestinationIds = new HashSet<>();
        long taskSize = 0;

        for (final LocalJobEntryWork workEntry : work) {
            long maxTaskSize = (key.minimizeSpanningAcrossMedia) ? MINSPANNING_TASK_SIZE : MAX_BYTES_PER_TASK;

            if (!entries.isEmpty() && (taskSize + workEntry.getLength() > maxTaskSize || entries.size() >= MAX_ENTRIES_PER_TASK)) {
                break;
            }
            entries.add(workEntry);
            taskSize += workEntry.getLength();

            if (workEntry.getLocalBlobDestinationId() != null) {
                localBlobDestinationIds.add(workEntry.getLocalBlobDestinationId());
            }
        }

        if (entries.isEmpty()) {
            return null;
        }

        if (key.requestType() == JobRequestType.GET) {
            return new ReadIntoCacheDirective(
                    key.priority(),
                    key.readFromTapeId(),
                    PersistenceType.TAPE,
                    entries);
        } else if (key.requestType() == JobRequestType.VERIFY) {
            return new VerifyDirective(
                    key.priority(),
                    key.readFromTapeId(),
                    PersistenceType.TAPE,
                    entries);
        } else if (key.requestType() == JobRequestType.PUT) {
            return createTapeWriteDirective(key, entries, localBlobDestinationIds, taskSize, serviceManager);
        } else {
            LOG.error("Unexpected request type for tape work: " + key.requestType());
            return null;
        }
    }

    private static LocalWriteDirective createTapeWriteDirective(TapeWorkAggregationKey key, List<JobEntry> entries,
                                                              Set<UUID> localBlobDestinationIds, long sizeInBytes,
                                                              BeansServiceManager serviceManager) {
        if (localBlobDestinationIds.isEmpty()) {
            LOG.warn("No local blob destination IDs found for tape write work");
            return null;
        }

        final Set<LocalBlobDestination> destinations = serviceManager.getRetriever(LocalBlobDestination.class)
                .retrieveAll(Require.beanPropertyEqualsOneOf(LocalBlobDestination.ID, localBlobDestinationIds))
                .toSet();

        if (destinations.isEmpty()) {
            LOG.warn("No local blob destinations found for tape write work");
            return null;
        }

        final UUID storageDomainId = destinations.iterator().next().getStorageDomainId();
        final StorageDomain storageDomain = serviceManager.getRetriever(StorageDomain.class).attain(storageDomainId);
        final Bucket bucket = serviceManager.getRetriever(Bucket.class).attain(key.bucketId());

        final boolean isStageJob = key.iomType() == IomType.STAGE;

        return new LocalWriteDirective(
                destinations,
                storageDomain,
                key.priority(),
                entries,
                sizeInBytes,
                bucket,
                isStageJob,
                false
        );
    }

    private static WhereClause getReadOrVerifyFilter(final UUID tapeId, UUID tapePartitionId) {
        return Require.all(
                Require.beanPropertyEquals(LocalJobEntryWork.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING),
                getReadSourceFilter(tapeId, tapePartitionId));
    }

    private static WhereClause getReadSourceFilter(UUID tapeId, UUID tapePartitionId) {
        return tapeId != null
                ? Require.beanPropertyEquals(LocalJobEntryWork.READ_FROM_TAPE_ID, tapeId)
                : tapePartitionId != null
                    ? sourceIsInPartition(tapePartitionId)
                    : Require.beanPropertyNotNull(LocalJobEntryWork.READ_FROM_TAPE_ID);
    }

    private static WhereClause sourceIsInPartition(UUID tapePartitionId) {
        return Require.exists(
                LocalJobEntryWork.READ_FROM_TAPE_ID,
                Require.beanPropertyEquals(
                        Tape.PARTITION_ID,
                        tapePartitionId)
        );
    }

    private static WhereClause getWriteFilter(UUID tapePartitionId) {
        return Require.all(
                Require.beanPropertyEquals(LocalJobEntryWork.REQUEST_TYPE, JobRequestType.PUT),
                WorkAggregationUtils.blobIsOnDisk(),
                getNeedsToBeWrittenToTapeFilter(tapePartitionId));
    }

    private static WhereClause getNeedsToBeWrittenToTapeFilter(UUID tapePartitionId) {
        return Require.all(
                        Require.exists(
                                LocalJobEntryWork.STORAGE_DOMAIN_ID,
                                isTapeStorageDomain(tapePartitionId)),
                        Require.beanPropertyEquals(
                                LocalJobEntryWork.DESTINATION_STATE,
                                JobChunkBlobStoreState.PENDING
                        ));
    }

    private static WhereClause isTapeStorageDomain(UUID tapePartitionId) {
        return Require.exists(
                StorageDomainMember.class,
                StorageDomainMember.STORAGE_DOMAIN_ID,
                tapePartitionId != null
                        ? Require.beanPropertyEquals(StorageDomainMember.TAPE_PARTITION_ID, tapePartitionId)
                        : Require.beanPropertyNotNull(StorageDomainMember.TAPE_PARTITION_ID));
    }

    private record TapeWorkAggregationKey(
            JobRequestType requestType,
            BlobStoreTaskPriority priority,
            UUID bucketId,
            UUID readFromTapeId,
            UUID storageDomainId,
            IomType iomType,
            boolean minimizeSpanningAcrossMedia
    ) {}

    private static final int MAX_ENTRIES_PER_TASK = 100000;
    private static final long MAX_BYTES_PER_TASK = 100L * 1024L * 1024L * 1024L; //100GB
    private static final Logger LOG = Logger.getLogger(TapeWorkAggregationUtils.class);
    private static final long MINSPANNING_TASK_SIZE =  1024L * 1024L * 1024L * 1024L;
}