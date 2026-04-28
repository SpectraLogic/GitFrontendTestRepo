package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
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

public class S3TargetWorkAggregationUtils {
    public static List<IODirective> discoverS3TargetWorkAggregated(final BeansServiceManager serviceManager) {
        final Map<TargetWorkAggregationKey, Set<S3JobEntryWork>> workByKey = new LinkedHashMap<>();

        try(final EnhancedIterable<S3JobEntryWork> allS3IOWork = serviceManager.getRetriever(S3JobEntryWork.class)
                .retrieveAll(Query.where(Require.any(getReadFilter(), getWriteFilter()))
                        .orderBy(new BeanSQLOrdering())).toIterable()) {

            for (final S3JobEntryWork work : allS3IOWork) {
                final TargetWorkAggregationKey key = new TargetWorkAggregationKey(
                        work.getRequestType(),
                        work.getPriority(),
                        work.getBucketId(),
                        work.getRequestType() == JobRequestType.GET ? work.getReadFromS3TargetId() : null,
                        work.getRequestType() == JobRequestType.PUT ? work.getTargetId() : null,
                        work.getRequestType() == JobRequestType.PUT ? work.getRuleId() : null
                );

                workByKey.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(work);
            }
        }

        return workByKey.entrySet().stream()
                .map(entry -> createIODirectiveFromAggregatedWork(entry.getKey(), entry.getValue(), serviceManager))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static IODirective createIODirectiveFromAggregatedWork(TargetWorkAggregationKey key, Set<S3JobEntryWork> work, BeansServiceManager serviceManager) {
        final List<JobEntry> entries = new ArrayList<>();
        final Set<UUID> s3BlobDestinationIds = new HashSet<>();
        long taskSize = 0;

        for (final S3JobEntryWork workEntry : work) {
            if (!entries.isEmpty() && (taskSize + workEntry.getLength() > MAX_BYTES_PER_TASK || entries.size() >= MAX_ENTRIES_PER_TASK)) {
                break;
            }
            entries.add(workEntry);
            taskSize += workEntry.getLength();

            if (workEntry.getS3BlobDestinationId() != null) {
                s3BlobDestinationIds.add(workEntry.getS3BlobDestinationId());
            }
        }

        if (entries.isEmpty()) {
            return null;
        }

        if (key.requestType() == JobRequestType.GET) {
            return new ReadDirective(
                    key.priority(),
                    key.readFromTargetId(),
                    PersistenceType.S3,
                    entries);
        } else if (key.requestType() == JobRequestType.PUT) {
            return createS3WriteDirective(key, entries, s3BlobDestinationIds, taskSize, serviceManager);
        } else {
            LOG.error("Unexpected request type for S3 target work: " + key.requestType());
            return null;
        }
    }

    private static TargetWriteDirective<S3Target, S3BlobDestination> createS3WriteDirective(TargetWorkAggregationKey key, List<JobEntry> entries,
                                                              Set<UUID> s3BlobDestinationIds, long sizeInBytes,
                                                              BeansServiceManager serviceManager) {
        if (s3BlobDestinationIds.isEmpty()) {
            LOG.warn("No S3 blob destination IDs found for S3 target write work");
            return null;
        }

        final Set<S3BlobDestination> destinations = serviceManager.getRetriever(S3BlobDestination.class)
                .retrieveAll(Require.beanPropertyEqualsOneOf(S3BlobDestination.ID, s3BlobDestinationIds))
                .toSet();

        if (destinations.isEmpty()) {
            LOG.warn("No S3 blob destinations found for S3 target write work");
            return null;
        }

        final UUID targetId = destinations.iterator().next().getTargetId();
        final S3Target target = serviceManager.getRetriever(S3Target.class).attain(targetId);
        final Bucket bucket = serviceManager.getRetriever(Bucket.class).attain(key.bucketId());

        return new TargetWriteDirective<>(
                S3Target.class,
                destinations,
                target,
                key.priority(),
                entries,
                sizeInBytes,
                bucket
        );
    }

    private static WhereClause getReadFilter() {
        return Require.all(
                Require.beanPropertyEquals(S3JobEntryWork.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING),
                Require.beanPropertyEquals(S3JobEntryWork.REQUEST_TYPE, JobRequestType.GET),
                Require.beanPropertyNotNull(S3JobEntryWork.READ_FROM_S3_TARGET_ID));
    }

    private static WhereClause getWriteFilter() {
        return Require.all(
                Require.beanPropertyEquals(S3JobEntryWork.REQUEST_TYPE, JobRequestType.PUT),
                WorkAggregationUtils.blobIsOnDisk(),
                Require.beanPropertyEquals(S3JobEntryWork.DESTINATION_STATE, JobChunkBlobStoreState.PENDING)
        );
    }

    private record TargetWorkAggregationKey(
            JobRequestType requestType,
            BlobStoreTaskPriority priority,
            UUID bucketId,
            UUID readFromTargetId,
            UUID targetId,
            UUID ruleId
    ) {}

    private final static int MAX_ENTRIES_PER_TASK = 100000;
    private final static long MAX_BYTES_PER_TASK = 100L * 1024L * 1024L * 1024L; //100GB
    private final static Logger LOG = Logger.getLogger(S3TargetWorkAggregationUtils.class);
}