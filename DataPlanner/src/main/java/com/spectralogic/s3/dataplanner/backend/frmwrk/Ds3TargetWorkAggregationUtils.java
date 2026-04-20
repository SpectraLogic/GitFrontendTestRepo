package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
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

public class Ds3TargetWorkAggregationUtils {
    public static List<IODirective> discoverDs3TargetWorkAggregated(final BeansServiceManager serviceManager) {
        final Map<TargetWorkAggregationKey, Set<Ds3JobEntryWork>> workByKey = new LinkedHashMap<>();

        try(final EnhancedIterable<Ds3JobEntryWork> allDs3IOWork = serviceManager.getRetriever(Ds3JobEntryWork.class)
                .retrieveAll(Query.where(Require.any(getReadFilter(), getWriteFilter()))
                        .orderBy(new BeanSQLOrdering())).toIterable()) {

            for (final Ds3JobEntryWork work : allDs3IOWork) {
                final TargetWorkAggregationKey key = new TargetWorkAggregationKey(
                        work.getJobId(),
                        work.getRequestType(),
                        work.getPriority(),
                        work.getBucketId(),
                        work.getRequestType() == JobRequestType.PUT ? work.getTargetId() : work.getReadFromDs3TargetId()
                );

                workByKey.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(work);
            }
        }

        return workByKey.entrySet().stream()
                .map(entry -> createIODirectiveFromAggregatedWork(entry.getKey(), entry.getValue(), serviceManager))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static IODirective createIODirectiveFromAggregatedWork(TargetWorkAggregationKey key, Set<Ds3JobEntryWork> work, BeansServiceManager serviceManager) {
        final List<JobEntry> entries = new ArrayList<>();
        final Set<UUID> ds3BlobDestinationIds = new HashSet<>();
        long taskSize = 0;

        for (final Ds3JobEntryWork workEntry : work) {
            if (taskSize + workEntry.getLength() > MAX_BYTES_PER_TASK || entries.size() >= MAX_ENTRIES_PER_TASK) {
                break;
            }
            entries.add(workEntry);
            taskSize += workEntry.getLength();

            if (workEntry.getDs3BlobDestinationId() != null) {
                ds3BlobDestinationIds.add(workEntry.getDs3BlobDestinationId());
            }
        }

        if (entries.isEmpty()) {
            return null;
        }

        if (key.requestType() == JobRequestType.GET) {
            return new ReadDirective(
                    key.priority(),
                    key.targetId(),
                    PersistenceType.DS3,
                    entries);
        } else if (key.requestType() == JobRequestType.PUT) {
            return createDs3WriteDirective(key, entries, ds3BlobDestinationIds, taskSize, serviceManager);
        } else {
            LOG.error("Unexpected request type for DS3 target work: " + key.requestType());
            return null;
        }
    }

    private static TargetWriteDirective<Ds3Target, Ds3BlobDestination> createDs3WriteDirective(TargetWorkAggregationKey key, List<JobEntry> entries,
                                                              Set<UUID> ds3BlobDestinationIds, long sizeInBytes,
                                                              BeansServiceManager serviceManager) {
        if (ds3BlobDestinationIds.isEmpty()) {
            LOG.warn("No DS3 blob destination IDs found for DS3 target write work");
            return null;
        }

        final Set<Ds3BlobDestination> destinations = serviceManager.getRetriever(Ds3BlobDestination.class)
                .retrieveAll(Require.beanPropertyEqualsOneOf(Ds3BlobDestination.ID, ds3BlobDestinationIds))
                .toSet();

        if (destinations.isEmpty()) {
            LOG.warn("No DS3 blob destinations found for DS3 target write work");
            return null;
        }

        final UUID targetId = destinations.iterator().next().getTargetId();
        final Ds3Target target = serviceManager.getRetriever(Ds3Target.class).attain(targetId);
        final Bucket bucket = serviceManager.getRetriever(Bucket.class).attain(key.bucketId());

        return new TargetWriteDirective<>(
                Ds3Target.class,
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
                Require.beanPropertyEquals(Ds3JobEntryWork.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING),
                Require.beanPropertyEquals(Ds3JobEntryWork.REQUEST_TYPE, JobRequestType.GET),
                Require.beanPropertyNotNull(Ds3JobEntryWork.READ_FROM_DS3_TARGET_ID));
    }

    private static WhereClause getWriteFilter() {
        return Require.all(
                Require.beanPropertyEquals(Ds3JobEntryWork.REQUEST_TYPE, JobRequestType.PUT),
                Require.beanPropertyEquals(Ds3JobEntryWork.CACHE_STATE, CacheEntryState.IN_CACHE),
                Require.beanPropertyEquals(Ds3JobEntryWork.DESTINATION_STATE, JobChunkBlobStoreState.PENDING)
        );
    }

    private record TargetWorkAggregationKey(
            UUID jobId,
            JobRequestType requestType,
            BlobStoreTaskPriority priority,
            UUID bucketId,
            UUID targetId
    ) {}

    private final static int MAX_ENTRIES_PER_TASK = 100000;
    private final static long MAX_BYTES_PER_TASK = 100L * 1024L * 1024L * 1024L; //100GB
    private final static Logger LOG = Logger.getLogger(Ds3TargetWorkAggregationUtils.class);
}