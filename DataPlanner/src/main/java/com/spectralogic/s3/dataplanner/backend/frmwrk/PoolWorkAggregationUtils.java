package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ObsoleteBlobPool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class PoolWorkAggregationUtils {
    public static List<IODirective> discoverPoolWorkAggregated(final BeansServiceManager serviceManager) {
        final Map<PoolWorkAggregationKey, Set<LocalJobEntryWork>> workByKey = new LinkedHashMap<>();

        try(final EnhancedIterable<LocalJobEntryWork> allPoolIOWork = serviceManager.getRetriever(LocalJobEntryWork.class)
                .retrieveAll(Query.where(Require.any(getVerifyFilter(), getWriteFilter()))
                        .orderBy(new BeanSQLOrdering())).toIterable()) {

            for (final LocalJobEntryWork work : allPoolIOWork) {
                final PoolWorkAggregationKey key = new PoolWorkAggregationKey(
                        work.getRequestType(),
                        work.getPriority(),
                        work.getBucketId(),
                        work.getRequestType() == JobRequestType.VERIFY ? work.getReadFromPoolId() : null,
                        work.getRequestType() == JobRequestType.PUT ? work.getStorageDomainId() : null,
                        work.getRequestType() == JobRequestType.PUT ? work.getIomType() : null
                );

                workByKey.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(work);
            }
        }

        return workByKey.entrySet().stream()
                .map(entry -> createIODirectiveFromAggregatedWork(entry.getKey(), entry.getValue(), serviceManager))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static IODirective createIODirectiveFromAggregatedWork(PoolWorkAggregationKey key, Set<LocalJobEntryWork> work, BeansServiceManager serviceManager) {
        final List<JobEntry> entries = new ArrayList<>();
        final Set<UUID> localBlobDestinationIds = new HashSet<>();
        long taskSize = 0;

        for (final LocalJobEntryWork workEntry : work) {
            if (taskSize + workEntry.getLength() > MAX_BYTES_PER_TASK || entries.size() >= MAX_ENTRIES_PER_TASK) {
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

        if (key.requestType() == JobRequestType.VERIFY) {
            return new ReadDirective(
                    key.priority(),
                    key.readFromPoolId(),
                    PersistenceType.POOL,
                    entries);
        } else if (key.requestType() == JobRequestType.PUT) {
            return createPoolWriteDirective(key, entries, localBlobDestinationIds, taskSize, serviceManager);
        } else {
            LOG.error("Unexpected request type for pool work: " + key.requestType());
            return null;
        }
    }

    private static LocalWriteDirective createPoolWriteDirective(PoolWorkAggregationKey key, List<JobEntry> entries,
                                                              Set<UUID> localBlobDestinationIds, long sizeInBytes,
                                                              BeansServiceManager serviceManager) {
        if (localBlobDestinationIds.isEmpty()) {
            LOG.warn("No local blob destination IDs found for pool write work");
            return null;
        }

        final Set<LocalBlobDestination> destinations = serviceManager.getRetriever(LocalBlobDestination.class)
                .retrieveAll(Require.beanPropertyEqualsOneOf(LocalBlobDestination.ID, localBlobDestinationIds))
                .toSet();

        if (destinations.isEmpty()) {
            LOG.warn("No local blob destinations found for pool write work");
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

    private static WhereClause getVerifyFilter() {
        return Require.all(
                Require.beanPropertyEquals(LocalJobEntryWork.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING),
                Require.beanPropertyEquals(LocalJobEntryWork.REQUEST_TYPE, JobRequestType.VERIFY),
                Require.beanPropertyNotNull(LocalJobEntryWork.READ_FROM_POOL_ID));
    }

    private static WhereClause getWriteFilter() {
        return Require.all(
                Require.beanPropertyEquals(LocalJobEntryWork.REQUEST_TYPE, JobRequestType.PUT),
                WorkAggregationUtils.blobIsOnDisk(),
                getNeedsToBeWrittenToPoolFilter());
    }

    private static WhereClause getNeedsToBeWrittenToPoolFilter() {
        return Require.all(
                        Require.exists(
                                LocalJobEntryWork.STORAGE_DOMAIN_ID,
                                Require.exists(
                                        StorageDomainMember.class,
                                        StorageDomainMember.STORAGE_DOMAIN_ID,
                                        Require.beanPropertyNotNull(StorageDomainMember.POOL_PARTITION_ID))),
                        Require.beanPropertyEquals(
                                LocalJobEntryWork.DESTINATION_STATE,
                                JobChunkBlobStoreState.PENDING
                        ));
    }

    private record PoolWorkAggregationKey(
            JobRequestType requestType,
            BlobStoreTaskPriority priority,
            UUID bucketId,
            UUID readFromPoolId,
            UUID storageDomainId,
            IomType iomType
    ) {}

    private final static int MAX_ENTRIES_PER_TASK = 100000;
    private final static long MAX_BYTES_PER_TASK = 100L * 1024L * 1024L * 1024L; //100GB
    private final static Logger LOG = Logger.getLogger(PoolWorkAggregationUtils.class);
}
