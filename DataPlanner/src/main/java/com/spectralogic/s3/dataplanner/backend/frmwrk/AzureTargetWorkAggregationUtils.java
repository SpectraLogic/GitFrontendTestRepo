package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
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

public class AzureTargetWorkAggregationUtils {
    public static List<IODirective> discoverAzureTargetWorkAggregated(final BeansServiceManager serviceManager) {
        final Map<TargetWorkAggregationKey, Set<AzureJobEntryWork>> workByKey = new LinkedHashMap<>();

        try(final EnhancedIterable<AzureJobEntryWork> allAzureIOWork = serviceManager.getRetriever(AzureJobEntryWork.class)
                .retrieveAll(Query.where(Require.any(getReadFilter(), getWriteFilter()))
                        .orderBy(new BeanSQLOrdering())).toIterable()) {

            for (final AzureJobEntryWork work : allAzureIOWork) {
                final TargetWorkAggregationKey key = new TargetWorkAggregationKey(
                        work.getRequestType(),
                        work.getPriority(),
                        work.getBucketId(),
                        work.getRequestType() == JobRequestType.GET ? work.getReadFromAzureTargetId() : null,
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

    private static IODirective createIODirectiveFromAggregatedWork(TargetWorkAggregationKey key, Set<AzureJobEntryWork> work, BeansServiceManager serviceManager) {
        final List<JobEntry> entries = new ArrayList<>();
        final Set<UUID> azureBlobDestinationIds = new HashSet<>();
        long taskSize = 0;

        for (final AzureJobEntryWork workEntry : work) {
            if (!entries.isEmpty() && (taskSize + workEntry.getLength() > MAX_BYTES_PER_TASK || entries.size() >= MAX_ENTRIES_PER_TASK)) {
                break;
            }
            entries.add(workEntry);
            taskSize += workEntry.getLength();

            if (workEntry.getAzureBlobDestinationId() != null) {
                azureBlobDestinationIds.add(workEntry.getAzureBlobDestinationId());
            }
        }

        if (entries.isEmpty()) {
            return null;
        }

        if (key.requestType() == JobRequestType.GET) {
            return new ReadDirective(
                    key.priority(),
                    key.readFromTargetId(),
                    PersistenceType.AZURE,
                    entries);
        } else if (key.requestType() == JobRequestType.PUT) {
            return createAzureWriteDirective(key, entries, azureBlobDestinationIds, taskSize, serviceManager);
        } else {
            LOG.error("Unexpected request type for Azure target work: " + key.requestType());
            return null;
        }
    }

    private static TargetWriteDirective<AzureTarget, AzureBlobDestination> createAzureWriteDirective(TargetWorkAggregationKey key, List<JobEntry> entries,
                                                              Set<UUID> azureBlobDestinationIds, long sizeInBytes,
                                                              BeansServiceManager serviceManager) {
        if (azureBlobDestinationIds.isEmpty()) {
            LOG.warn("No Azure blob destination IDs found for Azure target write work");
            return null;
        }

        final Set<AzureBlobDestination> destinations = serviceManager.getRetriever(AzureBlobDestination.class)
                .retrieveAll(Require.beanPropertyEqualsOneOf(AzureBlobDestination.ID, azureBlobDestinationIds))
                .toSet();

        if (destinations.isEmpty()) {
            LOG.warn("No Azure blob destinations found for Azure target write work");
            return null;
        }

        final UUID targetId = destinations.iterator().next().getTargetId();
        final AzureTarget target = serviceManager.getRetriever(AzureTarget.class).attain(targetId);
        final Bucket bucket = serviceManager.getRetriever(Bucket.class).attain(key.bucketId());

        return new TargetWriteDirective<>(
                AzureTarget.class,
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
                Require.beanPropertyEquals(AzureJobEntryWork.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING),
                Require.beanPropertyEquals(AzureJobEntryWork.REQUEST_TYPE, JobRequestType.GET),
                Require.beanPropertyNotNull(AzureJobEntryWork.READ_FROM_AZURE_TARGET_ID));
    }

    private static WhereClause getWriteFilter() {
        return Require.all(
                Require.beanPropertyEquals(AzureJobEntryWork.REQUEST_TYPE, JobRequestType.PUT),
                WorkAggregationUtils.blobIsOnDisk(),
                Require.beanPropertyEquals(AzureJobEntryWork.DESTINATION_STATE, JobChunkBlobStoreState.PENDING)
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
    private final static Logger LOG = Logger.getLogger(AzureTargetWorkAggregationUtils.class);
}