/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.orm.BlobRM;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.dao.orm.DataPolicyRM;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.platform.persistencetarget.BlobDestinationUtils;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreateGetJobParams;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.frontend.dataorder.GetByPhysicalPlacementDataOrderingStrategy;
import com.spectralogic.s3.dataplanner.frontend.dataorder.Ds3TargetBlobPhysicalPlacementImpl;
import com.spectralogic.util.db.service.api.NestableTransaction;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BaseCreateJobParams;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.cache.JobCreatedListener;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.s3.dataplanner.frontend.api.S3ObjectCreator;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;

public final class JobCreatorImpl implements JobCreator
{
    public JobCreatorImpl(
            final DiskManager diskManager,
            final BeansServiceManager serviceManager,
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final JobProgressManager jobProgressManager,
            final Map<PersistenceType, BlobStore> blobStoresByPersistenceType,
            final long preferredBlobSizeInMb,
            final Long staticPreferredChunkSizeInMb )
    {
        Validations.verifyNotNull( "Cache manager", diskManager );
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "DS3 connection factory", ds3ConnectionFactory );
        Validations.verifyNotNull( "Job progress manager", jobProgressManager );
        Validations.verifyInRange( 
                "Preferred blob size in MB", 1, Integer.MAX_VALUE, preferredBlobSizeInMb );
        m_diskManager = diskManager;
        m_serviceManager = serviceManager;
        m_ds3ConnectionFactory = ds3ConnectionFactory;
        m_jobProgressManager = jobProgressManager;
        m_blobStoresByPersistenceType = blobStoresByPersistenceType;
        m_preferredBlobSizeInBytes = preferredBlobSizeInMb * 1024 * 1024;
        m_staticPreferredChunkSizeInBytes = ( null == staticPreferredChunkSizeInMb ) ?
                null
                : Long.valueOf( staticPreferredChunkSizeInMb.longValue() * 1024 * 1024 );
        LOG.info( "Preferred blob size: " + new BytesRenderer().render( m_preferredBlobSizeInBytes ) );
        if ( null == m_staticPreferredChunkSizeInBytes )
        {
            LOG.info( "Preferred chunk sizes will be computed dynamically on a per-job basis." );
        }
        else
        {
            LOG.info( "Preferred chunk size: "
                      + new BytesRenderer().render( m_staticPreferredChunkSizeInBytes.longValue() ) );
        }
    }


    @Override
    public UUID createGetOrVerifyJob(
            final BaseCreateJobParams<?> params,
            final UUID jobId,
            final JobRequestType requestType,
            final JobChunkClientProcessingOrderGuarantee chunkClientProcessingOrderGuarantee,
            final List<JobEntry> jobEntries)
    {
        // We can assume that all objects in the job belong to the same bucket since the S3 server ensures it
        final UUID bucketId = new BlobRM(jobEntries.get( 0 ).getBlobId(), m_serviceManager).getObject().getBucket().getId();
        final Job job = prepareJobForCreation(params, jobId, requestType, jobEntries, chunkClientProcessingOrderGuarantee, bucketId);
        // We can assume that all objects in the job belong to the same bucket since the S3 server ensures it
        try (final NestableTransaction transaction = m_serviceManager.startNestableTransaction())
        {
            transaction.getService(JobService.class).create(job);
            final UUID nodeId = transaction.getService(NodeService.class).getThisNode().getId();
            final Set<JobEntry> entriesLoadedToCache = new HashSet<>();
            final Set<JobEntry> zeroLengthEntriesToCreate = new HashSet<>();
            int index =0;
            final Map<UUID, JobEntry> entriesNeedingReadSources = BeanUtils.mapBeansByProperty(jobEntries, JobEntry.BLOB_ID);
            // Batch retrieve all blobs to avoid individual database calls
            final Set<UUID> blobIds = BeanUtils.extractPropertyValues(jobEntries, JobEntry.BLOB_ID);
            final Map<UUID, Blob> blobsById = BeanUtils.toMap(m_serviceManager.getRetriever(Blob.class).retrieveAll(blobIds).toSet());

            // First, collect all current chunk numbers
            List<Integer> existingChunkNumbers = jobEntries.stream()
                    .map(JobEntry::getChunkNumber)
                    .sorted()
                    .toList();

            long bytesAlreadyLoaded = 0;
            for (final JobEntry entry : jobEntries) {
                final Blob blob = blobsById.get(entry.getBlobId());
                if (blob == null) {
                    throw new IllegalStateException("Blob " + entry.getBlobId() + " not found for job entry in job " + job.getId());
                }
                final boolean zeroLengthBlob = blob.getLength() == 0;
                if (zeroLengthBlob) {
                    if (JobRequestType.GET == job.getRequestType()) {
                        entry.setBlobStoreState(JobChunkBlobStoreState.COMPLETED);
                    }
                    zeroLengthEntriesToCreate.add(entry);
                }

                if (m_diskManager.isOnDisk(entry.getBlobId())) {
                    if (JobRequestType.GET == job.getRequestType()) {
                        entry.setBlobStoreState(JobChunkBlobStoreState.COMPLETED);
                    }
                    if (m_diskManager.isInCache(entry.getBlobId())) {
                        bytesAlreadyLoaded += blob.getLength();
                        entriesLoadedToCache.add(entry);
                    }
                }
            }

            for (final JobEntry zeroLengthEntry : zeroLengthEntriesToCreate) {
                zeroLengthEntry.setChunkNumber(existingChunkNumbers.get(index++));
                entriesNeedingReadSources.remove(zeroLengthEntry.getBlobId());
            }
            if (JobRequestType.VERIFY != job.getRequestType()) {
                //For verify jobs, we don't care if it's on cache, we want to verify the persisted copy
                for (final JobEntry entryAlreadyInCache : entriesLoadedToCache) {
                    if (zeroLengthEntriesToCreate.contains(entryAlreadyInCache)) {
                        continue;
                    }
                    entryAlreadyInCache.setChunkNumber(existingChunkNumbers.get(index++));
                    entriesNeedingReadSources.remove(entryAlreadyInCache.getBlobId());
                }
            }

            final boolean jobAlreadyReplicated = (params instanceof CreateGetJobParams)
                    && ((CreateGetJobParams) params).getReplicatedJobId() != null;
            final String userName = m_serviceManager.getRetriever(User.class).attain(params.getUserId()).getName();
            Set<PersistenceType> readSources = Collections.emptySet();
            if (!entriesNeedingReadSources.isEmpty()) {
                for (final JobEntry entry : entriesNeedingReadSources.values()) {
                    entry.setChunkNumber(existingChunkNumbers.get(index++));
                }
                readSources = new GetByPhysicalPlacementDataOrderingStrategy(
                        entriesNeedingReadSources,
                        m_serviceManager,
                        requestType,
                        chunkClientProcessingOrderGuarantee,
                        ( jobAlreadyReplicated ) ? null : new Ds3TargetBlobPhysicalPlacementImpl(
                                entriesNeedingReadSources.keySet(),
                                m_serviceManager,
                                m_ds3ConnectionFactory),
                        m_diskManager,
                        userName,
                        job.getIomType() == IomType.STANDARD_IOM).setReadSources();
            }
            // Entries assigned to targets or tape need task processing, not direct serving from pool.
            // The isOnDisk check above may have prematurely marked them COMPLETED.
            if (JobRequestType.GET == job.getRequestType()) {
                for (final JobEntry entry : jobEntries) {
                    if (entry.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED
                            && (entry.getReadFromDs3TargetId() != null
                            || entry.getReadFromTapeId() != null
                            || entry.getReadFromAzureTargetId() != null
                            || entry.getReadFromS3TargetId() != null)) {
                        entry.setBlobStoreState(JobChunkBlobStoreState.PENDING);
                    }
                }
            }
            transaction.getService(JobEntryService.class).create(new HashSet<>(jobEntries));
            m_jobProgressManager.bytesLoadedToCache(transaction, bytesAlreadyLoaded, job.getId());
            transaction.commitTransaction();
            zeroLengthEntriesToCreate.removeAll(entriesLoadedToCache);
            for (final JobEntry entry : zeroLengthEntriesToCreate) {
                m_diskManager.createFilesForZeroLengthChunk(entry);
            }
            for (final PersistenceType type : readSources) {
                m_blobStoresByPersistenceType.get(type).taskSchedulingRequired();
            }
        }
        return job.getId();
    }


    @Override
    public UUID createPutJob(
            final BaseCreateJobParams<?> params,
            final UUID jobId,
            final S3ObjectCreator objectCreator,
            final List<JobEntry> jobEntries)
    {
        // We can assume that all objects in the job belong to the same bucket since the S3 server ensures it
        final UUID bucketId = objectCreator.getObjects().iterator().next().getBucketId();
        final Job job = prepareJobForCreation(params, jobId, JobRequestType.PUT, jobEntries, JobChunkClientProcessingOrderGuarantee.IN_ORDER, bucketId);
        job.setOriginalSizeInBytes(objectCreator.getTotalSize());

        try (final NestableTransaction transaction = m_serviceManager.startNestableTransaction())
        {
            objectCreator.commit(
                    transaction.getService(S3ObjectService.class),
                    transaction.getService(BlobService.class));
            if ( objectCreator.getObjects().isEmpty() )
            {
                transaction.commitTransaction(); //NOTE: can only happen when "ignore conflicts" is true for this job
                return null;
            }
            //create job
            transaction.getService(JobService.class).create(job);
            final DataPolicyRM policy = new BucketRM(job.getBucketId(), transaction).getDataPolicy();
            final List<DataPersistenceRule> localRules = m_serviceManager.getService(DataPersistenceRuleService.class)
                    .getRulesToWriteTo(policy.getId(), job.getIomType());
            final List<Ds3DataReplicationRule> ds3Rules = policy.getDs3DataReplicationRules().toList();
            final List<AzureDataReplicationRule> azureRules = policy.getAzureDataReplicationRules().toList();
            final List<S3DataReplicationRule> s3Rules = policy.getS3DataReplicationRules().toList();

            final List<JobEntry> entriesOnDisk = jobEntries.stream()
                    .filter(e -> m_diskManager.isOnDisk(e.getBlobId())).toList();
            transaction.getService(JobEntryService.class).create(new HashSet<>(jobEntries));
            m_jobProgressManager.entriesLoadedToCache(transaction, entriesOnDisk);
            //create destinations
            BlobDestinationUtils.createLocalBlobDestinations(jobEntries, localRules, job.getBucketId(), transaction);
            BlobDestinationUtils.createDs3BlobDestinations(jobEntries, ds3Rules, transaction);
            BlobDestinationUtils.createAzureBlobDestinations(jobEntries, azureRules, transaction);
            BlobDestinationUtils.createS3BlobDestinations(jobEntries, s3Rules, transaction);
            transaction.commitTransaction();
        }

        return job.getId();
    }


    //this function is for the tasks that are common to all job types like setting the job properties and associating the job entries.
    private Job prepareJobForCreation(BaseCreateJobParams<?> params, UUID jobId, JobRequestType requestType, List<JobEntry> jobEntries, JobChunkClientProcessingOrderGuarantee chunkClientProcessingOrderGuarantee, UUID bucketId) {
        final Job job = BeanFactory.newBean( Job.class );
        if (jobId == null) {
            jobId = UUID.randomUUID();
        }
        final String defaultJobName = job.getName();
        BeanCopier.copy( job, params );
        job.setId( jobId );
        job.setRequestType( requestType );
        job.setChunkClientProcessingOrderGuarantee( chunkClientProcessingOrderGuarantee );
        if ( null == params.getName() )
        {
            job.setName( defaultJobName );
        }
        job.setBucketId( bucketId );
        if (job.getOriginalSizeInBytes() == 0) {
            final Set<UUID> blobIds = BeanUtils.extractPropertyValues(jobEntries, JobEntry.BLOB_ID);
            Map<UUID, Blob> blobs = BeanUtils.toMap(m_serviceManager.getRetriever(Blob.class).retrieveAll(blobIds).toSet());
            final long jobSize = blobs.values().stream().mapToLong(Blob::getLength).sum();
            job.setOriginalSizeInBytes(jobSize);
        }
        int entryNumber = 0;
        for (final JobEntry entry : jobEntries) {
            entry.setId(UUID.randomUUID());
            entry.setJobId(job.getId());
            entry.setChunkNumber(entryNumber++);
        }
        return job;
    }


    public void addJobCreatedListener( final JobCreatedListener listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        synchronized ( m_jobCreatedListeners )
        {
            if ( m_jobCreatedListeners.contains( listener ) )
            {
                return;
            }
            m_jobCreatedListeners.add( listener );
        }
    }
    
    
    public void notifyJobCreatedListeners( final JobRequestType type, final UUID jobId )
    {
        for ( final JobCreatedListener listener : m_jobCreatedListeners )
        {
            listener.jobCreated( type, jobId );
        }
    }
    
    
    public long getPreferredBlobSizeInBytes()
    {
        return m_preferredBlobSizeInBytes;
    }
    
    
    public Object getJobReshapingLock()
    {
        return m_jobReshapingLock;
    }


    private final Map<PersistenceType, BlobStore> m_blobStoresByPersistenceType;
    private final long m_preferredBlobSizeInBytes;
    private final Long m_staticPreferredChunkSizeInBytes;
    private final List< JobCreatedListener > m_jobCreatedListeners = new CopyOnWriteArrayList<>();
    private final DiskManager m_diskManager;
    private final BeansServiceManager m_serviceManager;
    private final Ds3ConnectionFactory m_ds3ConnectionFactory;
    private final JobProgressManager m_jobProgressManager;
    private final Object m_jobReshapingLock = new Object();
    private final static Logger LOG = Logger.getLogger( JobCreatorImpl.class );
}
