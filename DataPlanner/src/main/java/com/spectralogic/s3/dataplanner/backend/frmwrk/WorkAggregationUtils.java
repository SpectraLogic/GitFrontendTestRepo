package com.spectralogic.s3.dataplanner.backend.frmwrk;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ObsoleteBlobPool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.service.ds3.AzureBlobDestinationService;
import com.spectralogic.s3.common.dao.service.ds3.Ds3BlobDestinationService;
import com.spectralogic.s3.common.dao.service.ds3.LocalBlobDestinationService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.dao.service.ds3.S3BlobDestinationService;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.S3ObjectsCachedNotificationPayloadGenerator;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import org.apache.log4j.Logger;

import java.util.*;

public class WorkAggregationUtils {


    public static int markReadChunksInProgress(final Collection<JobEntry> entries, final BeansServiceManager serviceManager ) {
        serviceManager.getService( JobEntryService.class ).update(
                Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(entries).keySet()),
                e -> e.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                JobEntry.BLOB_STORE_STATE );
        return entries.size();
    }


    public static int markReadChunksInProgress(final ReadDirective rd, final BeansServiceManager serviceManager ) {
        return markReadChunksInProgress(rd.getEntries(), serviceManager);
    }


    public static int markWriteChunksInProgress(final WriteDirective wd, final BeansServiceManager serviceManager ) {
        return markWriteChunksInProgress(wd.getEntries(), serviceManager);
    }

    public static int markWriteChunksInProgress(final Collection<JobEntry> entries, final BeansServiceManager serviceManager ) {
        final Map<UUID, List<JobEntry>> entriesByJobId = new HashMap<>();
        for (final JobEntry entry : entries) {
            final UUID jobId = entry.getJobId();
            if (!entriesByJobId.containsKey(jobId)) {
                entriesByJobId.put(jobId, new ArrayList<>());
            }
            entriesByJobId.get(jobId).add(entry);
        }
        for (final UUID jobId : entriesByJobId.keySet()) {
            final List<JobEntry> entriesForJob = entriesByJobId.get(jobId);
            serviceManager.getService(JobEntryService.class).update(
                    Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(entriesForJob).keySet()),
                    e -> e.setBlobStoreState(JobChunkBlobStoreState.IN_PROGRESS),
                    JobEntry.BLOB_STORE_STATE);
            serviceManager.getNotificationEventDispatcher()
                    .fire(new JobNotificationEvent(serviceManager.getService(JobService.class)
                            .attain(jobId),
                            serviceManager.getRetriever(S3ObjectCachedNotificationRegistration.class),
                            new S3ObjectsCachedNotificationPayloadGenerator(
                                    jobId,
                                    Set.copyOf(entriesForJob),
                                    serviceManager.getRetriever(S3Object.class),
                                    serviceManager.getRetriever(Blob.class))));
        }
        return entries.size();
    }

    public static int markLocalDestinationsInProgress(final Collection<? extends LocalBlobDestination> destinations, final BeansServiceManager serviceManager ) {
        serviceManager.getService( LocalBlobDestinationService.class ).update(
                Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(destinations).keySet()),
                d -> d.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                LocalBlobDestination.BLOB_STORE_STATE );
        return destinations.size();
    }

    public static int markS3DestinationsInProgress(final Collection<? extends S3BlobDestination> destinations, final BeansServiceManager serviceManager ) {
        serviceManager.getService( S3BlobDestinationService.class ).update(
                Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(destinations).keySet()),
                d -> d.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                S3BlobDestination.BLOB_STORE_STATE );
        return destinations.size();
    }

    public static int markAzureDestinationsInProgress(final Collection<? extends AzureBlobDestination> destinations, final BeansServiceManager serviceManager ) {
        serviceManager.getService( AzureBlobDestinationService.class ).update(
                Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(destinations).keySet()),
                d -> d.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                AzureBlobDestination.BLOB_STORE_STATE );
        return destinations.size();
    }

    public static int markDs3DestinationsInProgress(final Collection<? extends Ds3BlobDestination> destinations, final BeansServiceManager serviceManager ) {
        serviceManager.getService( Ds3BlobDestinationService.class ).update(
                Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(destinations).keySet()),
                d -> d.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                Ds3BlobDestination.BLOB_STORE_STATE );
        return destinations.size();
    }


    public static WhereClause blobIsOnDisk() {
        return Require.any(
                blobIsInCache(),
                blobIsOnPool());
    }

    private static WhereClause blobIsInCache() {
        return Require.beanPropertyEquals(LocalJobEntryWork.CACHE_STATE, CacheEntryState.IN_CACHE);
    }

    private static WhereClause blobIsOnPool() {
        return Require.exists(
                LocalJobEntryWork.BLOB_ID,
                Require.exists(
                        BlobPool.class,
                        BlobPool.BLOB_ID,
                        Require.not(
                                Require.any(
                                        Require.exists(
                                                SuspectBlobPool.class,
                                                SuspectBlobPool.ID,
                                                Require.nothing()
                                        ),
                                        Require.exists(
                                                ObsoleteBlobPool.class,
                                                ObsoleteBlobPool.ID,
                                                Require.nothing()
                                        )
                                )
                        )
                ));
    }


    private final static Logger LOG = Logger.getLogger( WorkAggregationUtils.class );
}
