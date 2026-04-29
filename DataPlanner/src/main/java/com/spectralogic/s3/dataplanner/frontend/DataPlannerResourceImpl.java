/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.frontend;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReadWriteLock;

import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEventType;
import com.spectralogic.s3.common.dao.orm.*;
import com.spectralogic.s3.common.platform.notification.domain.event.BucketNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.BucketChangesNotificationPayloadGenerator;
import com.spectralogic.s3.common.platform.replicationtarget.TargetInitializationUtil;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.frontend.api.S3ObjectCreator;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.JobCompletedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.DeleteS3ObjectsPreCommitListener;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.aws.S3Utils;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobChunkToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.JobCompletedNotificationPayloadGenerator;
import com.spectralogic.s3.common.platform.spectrads3.BaseQuiescableJobResource;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
import com.spectralogic.s3.common.rpc.dataplanner.CancelJobFailedException;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.*;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.s3.dataplanner.frontend.dataorder.Ds3TargetBlobPhysicalPlacement;
import com.spectralogic.s3.dataplanner.frontend.dataorder.Ds3TargetBlobPhysicalPlacementImpl;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanComparator.BeanPropertyComparisonSpecifiction;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.lang.HeapDumper;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.security.ChecksumType;


public final class DataPlannerResourceImpl extends BaseQuiescableJobResource implements DataPlannerResource
{
    public DataPlannerResourceImpl( 
            final DeadJobMonitor deadJobMonitor,
            final RpcServer rpcServer, 
            final BeansServiceManager serviceManager,
            final DiskManager diskManager,
            final JobCreator jobCreator,
            final JobProgressManager jobProgressManager,
            final TapeBlobStore tapeBlobStore,
            final PoolBlobStore poolBlobStore,
            final Ds3ConnectionFactory ds3ConnectionFactory )
    {
        super( serviceManager );
        Validations.verifyNotNull( "Dead job monitor", deadJobMonitor );
        Validations.verifyNotNull( "RPC server", rpcServer );
        Validations.verifyNotNull( "Cache manager", diskManager );
        Validations.verifyNotNull( "Job creator", jobCreator );
        Validations.verifyNotNull( "Job progress manager", jobProgressManager );
        Validations.verifyNotNull( "DS3 connection factory", ds3ConnectionFactory );
        Validations.verifyNotNull( "Tape blob store", tapeBlobStore );
        Validations.verifyNotNull( "Pool blob store", poolBlobStore );
        
        rpcServer.register( null, this );
        m_deadJobMonitor = deadJobMonitor;
        m_diskManager = diskManager;
        m_jobCreator = jobCreator;
        m_jobProgressManager = jobProgressManager;
        m_ds3ConnectionFactory = ds3ConnectionFactory;
        m_tapeBlobStore = tapeBlobStore;
        m_poolBlobStore = poolBlobStore;
        m_featureKeyValidator = new FeatureKeyValidator( m_serviceManager );
        m_bucketLogicalSizeCache = m_serviceManager.getService( BucketService.class ).getLogicalSizeCache();
        m_bucketLock = serviceManager.getService( BucketService.class ).getLock();
        m_objectLock = serviceManager.getService( S3ObjectService.class ).getLock();
    }
    
    
    public DataPlannerResourceImpl addTargetBlobStore( final TargetBlobStore store )
    {
        Validations.verifyNotNull( "Store", store );
        m_targetBlobStores.add( store );
        return this;
    }
    
    
    public RpcFuture< UUID > createPutJob( final CreatePutJobParams params )
    {
        BlobbingPolicy blobbingPolicy = params.getBlobbingPolicy();
        Long maxUploadSizeInBytes = params.getMaxUploadSizeInBytes();
        final S3ObjectToCreate [] objectsToCreate = params.getObjectsToCreate();
        if ( null == objectsToCreate || 0 == objectsToCreate.length )
        {
            throw new IllegalArgumentException( "You must specify at least one object to create." );
        }
        if ( null != maxUploadSizeInBytes )
        {
            Validations.verifyInRange(
                    "Max upload size in bytes", 10 * 1024 * 1024, Long.MAX_VALUE, maxUploadSizeInBytes );
        }
        
        final DataPolicy dataPolicy = 
                m_serviceManager.getRetriever( DataPolicy.class ).attain( Require.exists(
                        Bucket.class,
                        Bucket.DATA_POLICY_ID, 
                        Require.beanPropertyEquals( Identifiable.ID, params.getBucketId() ) ) );
        if ( !dataPolicy.isBlobbingEnabled() )
        {
            blobbingPolicy = BlobbingPolicy.DISABLED;
        }
        if ( null == maxUploadSizeInBytes )
        {
            maxUploadSizeInBytes = dataPolicy.getDefaultBlobSize();
        }
        final long preferredBlobSize = ( null == maxUploadSizeInBytes ) ?
                m_jobCreator.getPreferredBlobSizeInBytes()
                : maxUploadSizeInBytes;
        final long maxBlobSize = Math.min( 
                m_diskManager.getMaximumChunkSizeInBytes(),
                ( null == maxUploadSizeInBytes ) ? Long.MAX_VALUE : maxUploadSizeInBytes );

        long blobSize = 0;
        switch ( blobbingPolicy )
        {
            case ENABLED:
                blobSize = preferredBlobSize;
                break;
            case DISABLED:
                blobSize = maxBlobSize;
                break;
        }

        long blobsRequired = 0;

        for ( S3ObjectToCreate obj : objectsToCreate )
        {
            /*
             * If an object is smaller than the blobbing policy's blob size,
             * then instead of using the rounded value from the below division,
             * which may be a zero or a one, just use a one since a 0 byte object still takes 1 blob.
             */
            blobsRequired += (long) Math.max(1,Math.ceil( ((double)obj.getSizeInBytes())/blobSize ));
        }

        /*
         * The reason MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED is being used here is that this
         * code execution path will eventually create a Set of objects to create, which is restricted by this
         * constant to make sure we don't run out of memory creating a bazillion objects in said Set.
         */
        if ( EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED < blobsRequired )
        {
            throw new DataPlannerException( GenericFailure.BAD_REQUEST, blobsRequired +
                    " blobs were needed for " + objectsToCreate.length + " objects, but " +
                    EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED +
                    " is the maximum.  Please split your job up." );
        }

        final S3ObjectCreatorImpl objectCreator = new S3ObjectCreatorImpl(
                preferredBlobSize,
                maxBlobSize,
                blobbingPolicy,
                ( params.isIgnoreNamingConflicts() ) ?
                        m_serviceManager.getService( S3ObjectService.class )
                        : null,
                params.getBucketId(),
                CollectionFactory.toSet( objectsToCreate ) );
        if ( objectCreator.getObjects().isEmpty() )
        {
            throw new DataPlannerException(
                    GenericFailure.GONE,
                    "There is no work to perform (nothing needs to be uploaded given the request made)." );
        }

        final List<JobEntry> entries = objectCreator.getBlobs().stream().map(blob -> {
            final JobEntry chunk = BeanFactory.newBean(JobEntry.class);
            chunk.setBlobId(blob.getId());
            return chunk;
        }).toList();

        return startJob(
                params,
                null,
                JobRequestType.PUT,
                JobChunkClientProcessingOrderGuarantee.IN_ORDER,
                objectCreator,
                entries,
                null);
    }


    public RpcFuture< UUID > createGetJob( final CreateGetJobParams params )
    {
        final UUID [] blobIds = params.getBlobIds();
        if ( null == blobIds || 0 == blobIds.length )
        {
            throw new IllegalArgumentException( "You must specify at least one object to get." );
        }
        
        /*
         * For the duration we create the GET job, we cannot allow any cache reclaims since we're chunking
         * up the work based on what is or is not in cache.  Jason Stevens 4/14/15.
         */
        final UUID lockIdentifier = UUID.randomUUID();
        m_diskManager.lockBlobs( lockIdentifier, CollectionFactory.toSet( blobIds ) );
        try
        {
            final List<JobEntry> entries = Arrays.stream(blobIds).map(blobId -> {
                final JobEntry entry = BeanFactory.newBean(JobEntry.class);
                entry.setBlobId(blobId);
                return entry;
            }).toList();

            return startJob(
                    params,
                    params.getReplicatedJobId(),
                    JobRequestType.GET,
                    params.getChunkOrderGuarantee(),
                    null,
                    entries,
                    lockIdentifier );
        }
        catch ( final RuntimeException ex )
        {
            try
            {
                m_diskManager.unlockBlobs( lockIdentifier );
            }
            catch ( final RuntimeException ex2 )
            {
                LOG.debug( "It is normal to have already canceled out this lock by now.", ex2 );
            }
            throw ex;
        }
    }


    public RpcFuture< UUID > createVerifyJob( final CreateVerifyJobParams params )
    {
        final UUID [] blobIds = params.getBlobIds();
        if ( null == blobIds || 0 == blobIds.length )
        {
            throw new IllegalArgumentException( "You must specify at least one object to get." );
        }

        final List<JobEntry> entries = Arrays.stream(blobIds).map(blobId -> {
            final JobEntry chunk = BeanFactory.newBean(JobEntry.class);
            chunk.setBlobId(blobId);
            return chunk;
        }).toList();
        return startJob( 
                params,
                null,
                JobRequestType.VERIFY,
                JobChunkClientProcessingOrderGuarantee.NONE,
                null,
                entries,
                null);
    }
    
    
    private RpcFuture< UUID > startJob(
            final BaseCreateJobParams< ? > params,
            final UUID customJobId,
            final JobRequestType requestType,
            final JobChunkClientProcessingOrderGuarantee chunkClientProcessingOrderGuarantee,
            final S3ObjectCreatorImpl objectCreator,
            final List<JobEntry> jobEntries,
            final UUID lockIdentifier)
    {
        Validations.verifyNotNull( "Request type", requestType );
        verifyCanStartJob();
        final Object aggregateJobLock = ( params.isAggregating() ) ?
                m_jobCreator.getJobReshapingLock()
                : new Object();
        final Object putJobLock = ( null == objectCreator ) ? 
                new Object() 
                : PutJobLockManager.getLock( objectCreator.getObjects().iterator().next().getBucketId() );
        UUID jobId;
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized ( aggregateJobLock )
        {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized ( putJobLock )
            {
                m_objectLock.readLock().lock();
                LOG.info( "Locks acquired to create job.  Will proceed to do so." );
                try
                {
                    UUID result;
                    if ( JobRequestType.PUT == requestType)
                    {
                        result = m_jobCreator.createPutJob(params, customJobId, (S3ObjectCreator) objectCreator, jobEntries);
                    }
                    else
                    {
                        result = m_jobCreator.createGetOrVerifyJob(params, customJobId, requestType, chunkClientProcessingOrderGuarantee, jobEntries);
                    }
                    jobId = result;
                }
                finally
                {
                    m_objectLock.readLock().unlock();
                }
            }

            final boolean emulateChunks = m_serviceManager.getRetriever(DataPathBackend.class).attain(Require.nothing()).getEmulateChunks();
            // Chunk emulation only applies to PUTs; GETs are always reported one entry per
            // chunk by the request handler, so stamping chunkIds on GET entries would just
            // be dead state.
            if (emulateChunks && JobRequestType.PUT == requestType) {
                Long chunkSize = new JobRM(jobId, m_serviceManager).getBucket().getLastPreferredChunkSizeInBytes();
                if (chunkSize == null) {
                    chunkSize = BucketService.DEFAULT_PREFFERRED_CHUNK_SIZE;
                }
                long curChunkSize = 0;
                UUID curChunkId = UUID.randomUUID();
                final List<DetailedJobEntry> detailedJobEntryList = m_serviceManager.getRetriever(DetailedJobEntry.class)
                        .retrieveAll(Query.where(Require.beanPropertyEquals(DetailedJobEntry.JOB_ID, jobId))
                                .orderBy(new BeanSQLOrdering()))
                        .toList();
                for (final DetailedJobEntry detailedEntry : detailedJobEntryList) {
                    if (curChunkSize != 0 && curChunkSize + detailedEntry.getLength() > chunkSize) {
                        curChunkId = UUID.randomUUID();
                        curChunkSize = 0;
                    }
                    final UUID chunkId = curChunkId;
                    m_serviceManager.getUpdater(JobEntry.class)
                            .update(
                                    Require.beanPropertyEquals(Identifiable.ID, detailedEntry.getId()),
                                    bean -> bean.setChunkId(chunkId),
                                    JobEntry.CHUNK_ID);
                    curChunkSize += detailedEntry.getLength();
                }
            }

            if ( JobRequestType.GET == requestType )
            {
                m_diskManager.unlockBlobs( lockIdentifier );
                preAllocateGetJobIfNecessary( jobId );
            }
            if ( JobRequestType.PUT == requestType )
            {
                if ( params.isAggregating() )
                {
                    preAllocateAggregatingPutJob( jobId );
                }
                else if ( params.isPreAllocateJobSpace() )
                {
                    try
                    {
                        preAllocateJob( jobId );
                    }
                    catch ( final RuntimeException ex )
                    {
                        LOG.info( "Pre-allocation of job " + jobId +
                                " failed.  Will cancel the job and throw an allocation failure.", ex );
                        final Job job = m_serviceManager.getRetriever( Job.class )
                                                        .attain( jobId );
                        cancelJob( null, jobId, false );
                        throw new DataPlannerException( GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                                "PUT job needs pre-allocated space. Cannot pre-allocate this " +
                                        new BytesRenderer().render( job.getOriginalSizeInBytes() ) +
                                        " job at this time.  Please ensure you have adequate cache space to entirely " +
                                        "pre-allocate this job.", ex );
                    }
                }
            }
            
            if ( params.isAggregating() )
            {
                m_objectLock.readLock().lock();
                try
                {
                    jobId = appendToExistingJob( requestType, params.isNaked(), jobId );
                }
                finally
                {
                    m_objectLock.readLock().unlock();
                }
            }
        }
        if ( null != jobId )
        {
            m_jobCreator.notifyJobCreatedListeners( requestType, jobId );
        }
        
        return new RpcResponse<>( jobId );
    }
    
    
    private void preAllocateGetJobIfNecessary( final UUID jobId )
    {
        final Job job = m_serviceManager.getRetriever( Job.class ).attain( jobId );
        if ( JobChunkClientProcessingOrderGuarantee.IN_ORDER != job.getChunkClientProcessingOrderGuarantee() )
        {
            return;
        }
        
        LOG.info( "Since job " + job.getId() 
                + " requires in-order processing, it must be entirely pre-allocated in cache." );
        try
        {
            preAllocateJob( jobId );
        }
        catch ( final RuntimeException ex )
        {
            LOG.info( "Pre-allocation of job " + job.getId() 
                    + " failed.  Will cancel the job and throw an allocation failure.", ex );
            cancelJob( null, jobId, false );
            throw new DataPlannerException(
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                    "GET jobs that have a " + JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE 
                    + " of " + job.getChunkClientProcessingOrderGuarantee()
                    + " must be entirely pre-allocated.  Cannot pre-allocate this " 
                    + new BytesRenderer().render( job.getOriginalSizeInBytes() )
                    + " job at this time.  " +
                            "Please ensure you have adequate cache space to entirely pre-allocate this job.  "
                    + "You may also break this larger job into multiple smaller ones, use a different " 
                    + JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE 
                    + ", or try again later.", ex );
        }
    }
    
    
    private void preAllocateAggregatingPutJob( final UUID jobId )
    {
        try
        {
            preAllocateJob( jobId );
        }
        catch ( final RuntimeException ex )
        {
            LOG.info( "Pre-allocation of job " + jobId
                    + " failed.  Will cancel the job and throw an allocation failure.", ex );
            final Job job = m_serviceManager.getRetriever( Job.class ).attain( jobId );
            cancelJob( null, jobId, false );
            throw new DataPlannerException(
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                    "PUT jobs that are aggregating" 
                    + " must be entirely pre-allocated.  Cannot pre-allocate this " 
                    + new BytesRenderer().render( job.getOriginalSizeInBytes() )
                    + " job at this time.  " +
                            "Please ensure you have adequate cache space to entirely pre-allocate this job.", ex );
        }
    }
    
    
    private void preAllocateJob( final UUID jobId )
    {
        try
        {
            preAllocateJobInternal( jobId );
        }
        catch ( final RuntimeException ex )
        {
            LOG.debug( "Will retry allocation after forced full cache reclaim" +
                    " due to initial allocation failure.", ex );
            forceFullCacheReclaimNow();
            preAllocateJobInternal( jobId );
        }
        LOG.info( "Pre-allocation of job " + jobId + " successful." );
    }
    
    
    private void preAllocateJobInternal( final UUID jobId )
    {
        for ( final JobEntry chunk : m_serviceManager.getRetriever( JobEntry.class ).retrieveAll(
                JobEntry.JOB_ID, jobId ).toSet() )
        {
            m_diskManager.allocateChunk( chunk.getId() );
        }
    }
    
    
    public RpcFuture< UUID > replicatePutJob( final DetailedJobToReplicate job )
    {
        verifyCanStartJob();
        synchronized ( PutJobLockManager.getLock( job.getBucketId() ) )
        {
            m_objectLock.readLock().lock();
            LOG.info( "Locks acquired to create job.  Will proceed to do so." );
            try
            {
                final UUID dataPolicyId = new BucketRM(job.getBucketId(), m_serviceManager).unwrap().getDataPolicyId();
                final Set<StorageDomainMember> members = m_serviceManager.getService(StorageDomainMemberService.class)
                        .getStorageDomainMembersToWriteTo(dataPolicyId, IomType.NONE);
                final Set<UUID> storageDomainIds = BeanUtils.extractPropertyValues(members, StorageDomainMember.STORAGE_DOMAIN_ID);
                final Set< UUID > ds3TargetsToReplicateTo = TargetInitializationUtil.getInstance().getDs3TargetsToReplicateTo(
                    m_serviceManager, dataPolicyId );
                final Set< UUID > s3TargetsToReplicateTo = TargetInitializationUtil.getInstance()
                        .getPublicCloudTargetsToReplicateTo(
                                S3DataReplicationRule.class,
                                m_serviceManager,
                                dataPolicyId );
                final Set< UUID > azureTargetsToReplicateTo = TargetInitializationUtil.getInstance()
                        .getPublicCloudTargetsToReplicateTo(
                                AzureDataReplicationRule.class,
                                m_serviceManager,
                                dataPolicyId );

                final PersistenceTargetInfo pti = BeanFactory.newBean(PersistenceTargetInfo.class);
                pti.setStorageDomainIds(CollectionFactory.toArray(UUID.class, storageDomainIds));
                pti.setDs3TargetIds(CollectionFactory.toArray(UUID.class, ds3TargetsToReplicateTo));
                pti.setS3TargetIds(CollectionFactory.toArray(UUID.class, s3TargetsToReplicateTo));
                pti.setAzureTargetIds(CollectionFactory.toArray(UUID.class, azureTargetsToReplicateTo));

                final JobReplicationSupport support = new JobReplicationSupport(
                        m_serviceManager,
                        job );
                final UUID jobId = support.commit( m_diskManager, pti);

                m_jobCreator.notifyJobCreatedListeners( JobRequestType.PUT, job.getJob().getId() );
                return new RpcResponse<>( jobId );
            }
            finally
            {
                m_objectLock.readLock().unlock();
            }
        }
    }
    

    public RpcFuture< UUID > createIomJob(
            final CreateGetJobParams params,
            final PersistenceTargetInfo persistenceTargetInfo)
    {    		
		m_serviceManager.getRetriever( Blob.class ).attain( params.getBlobIds()[0] );
    		
    	final List< Blob > blobs = 
        		m_serviceManager.getRetriever( Blob.class ).retrieveAll(
        				CollectionFactory.toSet( params.getBlobIds() ) ).toList();
        				
		final UUID bucketId = new BlobRM( blobs.get( 0 ), m_serviceManager ).getObject().getBucket().getId();
		final User user = new UserRM( params.getUserId(), m_serviceManager ).unwrap();
        final RpcFuture< UUID > future = createGetJob( 
                BeanFactory.newBean( CreateGetJobParams.class )
                .setName( params.getName() + " GET Job" )
                .setReplicatedJobId( null )
                .setUserId( user.getId() )
                .setPriority( params.getPriority() )
                .setChunkOrderGuarantee( JobChunkClientProcessingOrderGuarantee.NONE )
                .setAggregating( false )
                .setDeadJobCleanupAllowed( false )
                .setNaked( false )
                .setImplicitJobIdResolution( false )
                .setBlobIds( params.getBlobIds() )
                .setIomType( params.getIomType() ) );
        
        final UUID getJobId = future.get( Timeout.LONG );
        
        final DataMigration migration = BeanFactory.newBean( DataMigration.class )
                .setGetJobId( getJobId );
        m_serviceManager.getCreator( DataMigration.class ).create( migration );
        
        
        m_jobProgressManager.flush();
        final Job getJob = m_serviceManager.getRetriever( Job.class ).attain( getJobId );
        final JobToReplicate jobToReplicate =
                createPutJobFromGetJob( getJob, params.getName(), blobs );
        final DetailedJobToReplicate req =
                BeanFactory.newBean( DetailedJobToReplicate.class )
                        .setJob( jobToReplicate )
                        .setBucketId( bucketId )
                        .setUserId( user.getId() )
                        .setPriority( params.getPriority() )
                        .setCachedSizeInBytes( getJob.getCachedSizeInBytes() );
        final UUID putJobId = createIomPutJob( req, params.getIomType(), persistenceTargetInfo).get( Timeout.LONG );
        m_serviceManager.getUpdater( DataMigration.class ).update(
                migration.setPutJobId( putJobId ),
                DataMigration.PUT_JOB_ID );
        return new RpcResponse<>( putJobId );
    }
    
    
    private JobToReplicate createPutJobFromGetJob(
            final Job job,
            final String name,
            final Collection< Blob > blobs )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final JobToReplicate jobToReplicate = BeanFactory.newBean( JobToReplicate.class );
            jobToReplicate.setId( UUID.randomUUID() );
            final List< JobChunkToReplicate > putJobChunks = new ArrayList<>();
            final List<JobEntry> getJobEntries = transaction.getRetriever( JobEntry.class )
                    .retrieveAll( Require.beanPropertyEquals( JobEntry.JOB_ID, job.getId() ) ).toList();
            for ( JobEntry getJobEntry : getJobEntries)
            {
                final JobChunkToReplicate putJobChunk = BeanFactory.newBean(JobChunkToReplicate.class);
                putJobChunk.setChunkNumber( getJobEntry.getChunkNumber() );
                putJobChunk.setId( UUID.randomUUID() );
                putJobChunk.setOriginalChunkId( getJobEntry.getId() );
                putJobChunk.setBlobId( getJobEntry.getBlobId() );
                putJobChunks.add( putJobChunk );
            }
            
            jobToReplicate.setChunks( CollectionFactory.toArray( JobChunkToReplicate.class, putJobChunks ) );
            jobToReplicate.setBlobs( CollectionFactory.toArray( Blob.class, blobs ) );
            final Set< UUID > objectIds = BeanUtils.extractPropertyValues( blobs, Blob.OBJECT_ID);
            final List< S3Object > objects =
                    transaction.getRetriever( S3Object.class ).retrieveAll( objectIds ).toList();
            jobToReplicate.setObjects( CollectionFactory.toArray( S3Object.class, objects ) );
            jobToReplicate.setName( name + " PUT Job" );
            transaction.commitTransaction();
            return jobToReplicate;
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    
    
    private RpcFuture< UUID > createIomPutJob(final DetailedJobToReplicate job, final IomType restore, final PersistenceTargetInfo persistenceTargetInfo)
    {
        verifyCanStartJob();
        synchronized ( PutJobLockManager.getLock( job.getBucketId() ) )
        {
            final JobReplicationSupport support = new JobReplicationSupport(
                    m_serviceManager,
                    job,
                    restore );
            final UUID jobId = support.commit( m_diskManager, persistenceTargetInfo );
            m_jobCreator.notifyJobCreatedListeners( JobRequestType.PUT, job.getJob().getId() );
            return new RpcResponse<>( jobId );
        }
    }
    
    private void verifyCanStartJob()
    {
        verifyNotQuiesced();
        final int maxNumberOfConcurrentJobs = getMaxNumberOfConcurrentJobs();
        if ( maxNumberOfConcurrentJobs <= m_serviceManager.getRetriever( Job.class ).getCount() )
        {
            throw new DataPlannerException( 
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, 
                    "The maximum number of concurrent jobs is " + maxNumberOfConcurrentJobs + "." );
        }
    }
    
    
    public RpcFuture< ? > closeAggregatingJob( final UUID jobId )
    {
        synchronized ( m_jobCreator.getJobReshapingLock() )
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                transaction.getService( JobService.class ).closeAggregatingJob( jobId );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        }
        return null;
    }
    
    
    private UUID appendToExistingJob( final JobRequestType type, final boolean naked, final UUID srcJobId )
    {
        synchronized ( m_jobCreator.getJobReshapingLock() )
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                return appendToExistingJobInternal(
                        type,
                        naked,
                        transaction.getRetriever( Job.class ).attain( srcJobId ),
                        transaction );
            }
            finally
            {
                transaction.closeTransaction();
            }
        }
    }
    
    
    private UUID appendToExistingJobInternal( 
            final JobRequestType type,
            final boolean naked, 
            final Job srcJob, 
            final BeansServiceManager transaction )
    {
        final Set<JobEntry> srcJobEntries = transaction.getRetriever( JobEntry.class ).retrieveAll(
                JobEntry.JOB_ID, srcJob.getId() ).toSet();
        final Set< UUID > srcJobBlobIds = 
                BeanUtils.extractPropertyValues( srcJobEntries, BlobObservable.BLOB_ID );
        final Set< String > srcObjectNames = new HashSet<>();
        for ( UUID blobId : srcJobBlobIds )
        {
        	srcObjectNames.add( new BlobRM( blobId , m_serviceManager).getObject().getName() );
        }
        final JobService jobService = transaction.getService( JobService.class );
        final Job destJob = jobService.retrieveAll( Require.all( 
                Require.not( Require.beanPropertyEquals( Identifiable.ID, srcJob.getId() ) ),
                Require.beanPropertyEquals( Job.AGGREGATING, Boolean.TRUE ),
                Require.beanPropertyEquals( 
                        JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE,
                        srcJob.getChunkClientProcessingOrderGuarantee() ),
                Require.beanPropertyEquals( JobObservable.REQUEST_TYPE, type ),
                Require.beanPropertyEquals( JobObservable.NAKED, naked ),
                Require.beanPropertyEquals( JobObservable.BUCKET_ID, srcJob.getBucketId() ),
                Require.not( Require.exists( 
                        JobEntry.class,
                        JobEntry.JOB_ID,
                        Require.any( 
                        		Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, srcJobBlobIds ),
                        		Require.exists(
                        				BlobObservable.BLOB_ID,
                        				Require.exists(
                        						Blob.OBJECT_ID,
                        						Require.beanPropertyEqualsOneOf(
                        								S3Object.NAME,
                        								srcObjectNames ) ) ) ) ) ) ) ).getFirst();
        if ( null == destJob )
        {
            LOG.info( "There are no eligible aggregating jobs this job can be appended to." );
            return srcJob.getId();
        }
        
        LOG.info( "Will append job to existing job: " + destJob.getId() );
        final Set<JobEntry> entriesToMove = transaction.getRetriever( JobEntry.class ).retrieveAll(
                JobEntry.JOB_ID, srcJob.getId() ).toSet();
        jobService.migrate( destJob.getId(), srcJob.getId() );
        transaction.commitTransaction();
        
        final Set< Blob > movedBlobs = m_serviceManager.getRetriever( Blob.class ).retrieveAll( 
                BeanUtils.extractPropertyValues( entriesToMove, BlobObservable.BLOB_ID ) ).toSet();
        long bytesAdded = 0;
        for ( final Blob movedBlob : movedBlobs )
        {
            bytesAdded += movedBlob.getLength();
            if ( m_diskManager.isOnDisk( movedBlob.getId() ) )
            {
                m_jobProgressManager.blobLoadedToCache( destJob.getId(), movedBlob.getLength() );
            }
        }
        m_serviceManager.getService( JobService.class ).update( 
                destJob.setMinimizeSpanningAcrossMedia(
                        destJob.isMinimizeSpanningAcrossMedia() || srcJob.isMinimizeSpanningAcrossMedia() ),
                Job.MINIMIZE_SPANNING_ACROSS_MEDIA );
        m_serviceManager.getService( JobService.class ).update( 
                destJob.setOriginalSizeInBytes( destJob.getOriginalSizeInBytes() + bytesAdded ), 
                JobObservable.ORIGINAL_SIZE_IN_BYTES );
        
        return destJob.getId();
    }

    
    public RpcFuture< ? > cancelJob( final UUID userId, final UUID jobId, final boolean force )
    {
        cancelJobInternal( jobId, force, true );
        return null;
    }


    public RpcFuture< ? > cancelJobQuietly( final UUID userId, final UUID jobId, final boolean force )
    {
        cancelJobInternal( jobId, force, false );
        return null;
    }


    public Set< UUID > cancelJobInternal( final UUID jobId, final boolean force) {
        return cancelJobInternal( jobId, force, true );
    }


    public Set< UUID > cancelJobQuietlyInternal( final UUID jobId, final boolean force) {
        return cancelJobInternal( jobId, force, false );
    }


    private Set< UUID > cancelJobInternal( final UUID jobId, final boolean force, final boolean createCanceledJob )
    {
        m_objectLock.writeLock().lock();
        try
        {
            final JobService jobService = m_serviceManager.getService( JobService.class );
            if ( jobService.isIomJob( jobId ) )
            {
                final DataMigration dataMigration = jobService.getDataMigration( jobId );
                UUID counterpartJobId = jobService.getPutJobComponentOfDataMigration( jobId );
                if ( null == counterpartJobId )
                {
                    counterpartJobId = jobService.getGetJobComponentOfDataMigration( jobId );
                }
                if ( null != counterpartJobId )
                {
                    cancelJobInternalInternal( counterpartJobId, force, createCanceledJob );
                }
                if (dataMigration != null) {
                    m_serviceManager.getService(DataMigrationService.class).delete(dataMigration.getId());
                }
            }
            return cancelJobInternalInternal( jobId, force, createCanceledJob );
        }
        finally
        {
            m_objectLock.writeLock().unlock();
        }
    }

    
    private Set< UUID > cancelJobInternalInternal( final UUID jobId, final boolean force, final boolean createCanceledJob  )
    {
        final JobService jobService = m_serviceManager.getService( JobService.class );
        final Job job = jobService.attain( jobId );

        final Set< UUID > retval;
        GenericFailure forceFlagRequiredCode = null;
        String forceFlagRequiredMessage = null;
        if ( JobRequestType.PUT == job.getRequestType() && IomType.NONE == job.getIomType() )
        {
            final Set< UUID > blobIds = new HashSet<>();
            for ( final JobEntry entry: m_serviceManager.getRetriever( JobEntry.class ).retrieveAll(
                    Require.beanPropertyEquals( JobEntry.JOB_ID, jobId ) ).toSet() )
            {
                blobIds.add( entry.getBlobId() );
            }
            if (blobIds.isEmpty() && !force) {
                throw new CancelJobFailedException(
                        GenericFailure.FORCE_FLAG_REQUIRED,
                        "There is no outstanding work still associated with job " + jobId + " and it is in the process of completing.  " +
                                "If you wish to force cancel this job anyway, please specify the force flag.",
                        Collections.emptySet());
            }
            final Set< Blob > blobs =
                    m_serviceManager.getRetriever( Blob.class ).retrieveAll( blobIds ).toSet();
            final Set< UUID > objectIds = BeanUtils.extractPropertyValues( blobs, Blob.OBJECT_ID );
            final Set< S3Object > objectsFullyUploaded =
                    m_serviceManager.getRetriever( S3Object.class ).retrieveAll( Require.all( 
                            Require.beanPropertyEqualsOneOf( Identifiable.ID, objectIds ),
                            Require.not(
                                    Require.beanPropertyEquals( S3Object.CREATION_DATE, null ) ) ) ).toSet();
            if ( objectsFullyUploaded.isEmpty() || force )
            {
                m_serviceManager.getNotificationEventDispatcher()
                        .fire( new JobNotificationEvent( job,
                                m_serviceManager.getRetriever( JobCompletedNotificationRegistration.class ),
                                new JobCompletedNotificationPayloadGenerator( jobId,
                                        true,
                                        m_serviceManager.getRetriever( S3Object.class ),
                                        m_serviceManager.getRetriever( Blob.class ) ) ) );
                if ( force )
                {
                    LOG.warn( "There are " + objectsFullyUploaded.size() +
                            " objects fully uploaded to cache that are part of job " + jobId +
                            "that will be lost by force-deleting this job." );
                }
                retval = objectIds;
                deleteObjectsInternal( objectIds, PreviousVersions.DELETE_SPECIFIC_VERSION, job );
            }
            else
            {
                objectIds.removeAll( BeanUtils.extractPropertyValues(
                        objectsFullyUploaded, Identifiable.ID ) );
                if ( objectIds.isEmpty() )
                {
                    forceFlagRequiredCode = GenericFailure.FORCE_FLAG_REQUIRED;
                    forceFlagRequiredMessage = "there are " + objectsFullyUploaded.size()
                        + " objects fully uploaded to cache that are part of this job that would be lost "
                        + "if this job were canceled.  " 
                        + "Job " + jobId + " contains only objects fully uploaded to cache, "
                        + "which means that this job does not require additional data transmission " 
                        + "from clients and will eventually complete if left alone";
                }
                else
                {
                    forceFlagRequiredCode = GenericFailure.FORCE_FLAG_REQUIRED_OK;
                    forceFlagRequiredMessage = "there are " + objectsFullyUploaded.size()
                        + " objects fully uploaded to cache that are part of job " + jobId 
                        + " that would be lost "
                        + "if this job were canceled.  " 
                        + "The " + objectIds.size() + " objects that were part of this job that have not "
                        + "been entirely uploaded to cache have been canceled.  "
                        + "Thus, this job now contains only objects fully uploaded to cache, "
                        + "which means that this job does not require additional data transmission " 
                        + "from clients and will eventually complete if left alone";
                    
                    long deletedWork = 0;
                    for ( final Blob blob : blobs )
                    {
                        if ( objectIds.contains( blob.getObjectId() ) )
                        {
                            deletedWork += blob.getLength();
                        }
                    }
                    deleteObjectsInternal( objectIds, PreviousVersions.DELETE_SPECIFIC_VERSION, null );
                    m_serviceManager.getService( JobService.class ).update(
                            job.setOriginalSizeInBytes( job.getOriginalSizeInBytes() - deletedWork )
                            .setTruncated( true ),
                            ( 0 == deletedWork ) ?
                                    new String [] { JobObservable.ORIGINAL_SIZE_IN_BYTES }
                            : new String [] { JobObservable.ORIGINAL_SIZE_IN_BYTES, 
                                            JobObservable.TRUNCATED } );
                }
                retval = objectIds;
            }
        }
        else if ( JobRequestType.GET == job.getRequestType() 
                || JobRequestType.VERIFY == job.getRequestType()
                || IomType.NONE != job.getIomType() )
        {
            retval = new HashSet<>();
            m_jobProgressManager.flush();
            m_serviceManager.getNotificationEventDispatcher()
                    .fire( new JobNotificationEvent( job,
                            m_serviceManager.getRetriever( JobCompletedNotificationRegistration.class ),
                            new JobCompletedNotificationPayloadGenerator( jobId,
                                    true,
                                    m_serviceManager.getRetriever( S3Object.class ),
                                    m_serviceManager.getRetriever( Blob.class ) ) ) );
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                stageJobCancellation( transaction, job, createCanceledJob );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        }
        else
        {
            throw new UnsupportedOperationException( "No code to support " + job.getRequestType() + "." );
        }
        
        if ( null != forceFlagRequiredCode )
        {
            throw new CancelJobFailedException( forceFlagRequiredCode, forceFlagRequiredMessage, retval );
        }

        LOG.info( "Job " + jobId + " has been cancelled successfully." );
    
        return retval;
    }
    
    
    public RpcFuture< ? > jobStillActive( final UUID jobId, final UUID blobId )
    {
        m_deadJobMonitor.activityOccurred( jobId, blobId );
        return null;
    }
    
    
    public RpcFuture< BlobsInCacheInformation > getBlobsInCache( final UUID [] blobIds )
    {
        final List< UUID > idsInCache = new ArrayList<>();
        for ( final UUID id : blobIds )
        {
            if ( m_diskManager.isInCache( id ) )
            {
                idsInCache.add( id );
            }
        }
        
        final BlobsInCacheInformation retval = BeanFactory.newBean( BlobsInCacheInformation.class );
        retval.setBlobsInCache( CollectionFactory.toArray( UUID.class, idsInCache ) );
        return new RpcResponse<>( retval );
    }
    
    
    public RpcFuture< ? > allocateEntry(final UUID entryId )
    {
        m_diskManager.allocateChunk( entryId );
        return null;
    }


    public RpcFuture<EntriesInCacheInformation> allocateEntries(final boolean allowPartial, final UUID[] entryIds )
    {
        final EntriesInCacheInformation retval = BeanFactory.newBean( EntriesInCacheInformation.class );
        if ( allowPartial )
        {
            final List< UUID > allocated = new ArrayList<>( entryIds.length );
            for ( final UUID entryId : entryIds )
            {
                try
                {
                    m_diskManager.allocateChunk( entryId );
                    allocated.add( entryId );
                }
                catch ( final Exception ex )
                {
                    LOG.info( "Cache full allocating entry " + entryId + "; stopping batch after " + allocated.size() + " allocations.", ex );
                    break;
                }
            }
            retval.setEntriesInCache( CollectionFactory.toArray( UUID.class, allocated ) );
            return new RpcResponse<>( retval );
        }
        else
        {
            try
            {
                m_diskManager.allocateChunks( CollectionFactory.toSet( entryIds ) );
                retval.setEntriesInCache( entryIds );
                return new RpcResponse<>( retval );
            }
            catch ( final Exception ex )
            {
                LOG.info( "Cannot atomically allocate " + entryIds.length + " entries.", ex );
                retval.setEntriesInCache( new UUID[0] );
                return new RpcResponse<>( retval );
            }
        }
    }
    
    
    public RpcFuture< Boolean > isChunkEntirelyInCache( final UUID chunkId )
    {
        return new RpcResponse<>( m_diskManager.isChunkEntirelyOnDisk( chunkId ) );
    }

    
    public RpcFuture<DiskFileInfo> startBlobRead(final UUID jobId, final UUID blobId )
    {
        m_deadJobMonitor.activityOccurred( jobId, blobId );

        m_objectLock.readLock().lock();
        try
        {
            return new RpcResponse<>( m_diskManager.getDiskFileFor( blobId ) );
        }
        finally
        {
            m_objectLock.readLock().unlock();
        }
    }

    
    public RpcFuture< ? > blobReadCompleted( final UUID jobId, final UUID blobId )
    {
        m_deadJobMonitor.activityOccurred( jobId, blobId );
        
        m_objectLock.readLock().lock();
        try
        {
            blobReadCompletedInternal( jobId, blobId );
        }
        finally
        {
            m_objectLock.readLock().unlock();
        }
        
        return null;
    }

    
    private void blobReadCompletedInternal( final UUID jobId, final UUID blobId )
    {
        final JobEntryService jobEntryService = m_serviceManager.getService( JobEntryService.class );
        
        final Set<JobEntry> jobEntries;
        if ( null == jobId )
        {
            LOG.debug( "Read has completed for " + blobId 
                    + ", but the client did not specify the job that this read completion is for." );
            jobEntries = jobEntryService.retrieveAll( Require.all( 
                    Require.beanPropertyEquals( BlobObservable.BLOB_ID, blobId ),
                    Require.exists(
                            JobEntry.JOB_ID,
                            Require.beanPropertyEquals(
                                    Job.IMPLICIT_JOB_ID_RESOLUTION, Boolean.TRUE ) ) ) ).toSet();
        }
        else
        {
            jobEntries = jobEntryService.retrieveAll( Require.all( 
                    Require.beanPropertyEquals( JobEntry.JOB_ID, jobId ),
                    Require.beanPropertyEquals( BlobObservable.BLOB_ID, blobId ) ) )
                            .toSet();
        }

        final Map< UUID, Job > jobs = BeanUtils.toMap(
                m_serviceManager.getRetriever( Job.class ).retrieveAll( 
                        BeanUtils.extractPropertyValues( jobEntries, JobEntry.JOB_ID ) ).toSet() );
        for ( final JobEntry e : new HashSet<>( jobEntries ) )
        {
            final Job job = jobs.get( e.getJobId() );
            if ( null == job )
            {
                jobEntries.remove( e );
            }
            else if ( job.isNaked() )
            {
                jobEntries.clear();
                jobEntries.add( e );
                break;
            }
        }
        
        blobReadCompletedInternal(
                jobs,
                jobEntries, 
                m_serviceManager.getRetriever( Blob.class ).attain( blobId ).getLength() );
    }
    
    
    private void blobReadCompletedInternal( 
            final Map< UUID, Job > jobs,
            final Set<JobEntry> jobEntries,
            final long bytesOfWorkCompleted )
    {

        for ( final JobEntry entry : new HashSet<>( jobEntries ) )
        {
            final Job job = jobs.get( entry.getJobId() );
            if ( JobRequestType.GET == job.getRequestType() )
            {
                final JobEntry chunk =
                        m_serviceManager.getRetriever( JobEntry.class ).attain( entry.getId() );
                if ( JobChunkBlobStoreState.COMPLETED != chunk.getBlobStoreState() )
                {
                    m_jobProgressManager.blobLoadedToCache( job.getId(), bytesOfWorkCompleted );
                }
                m_jobProgressManager.workCompleted( job.getId(), bytesOfWorkCompleted );
            }
            else
            {
                jobEntries.remove( entry );
            }
        }
        
        if ( jobEntries.isEmpty() )
        {
            throw new IllegalStateException( 
                    "No Job entry found that could be removed for the object read completion." );
        }
        if ( 1 != jobEntries.size() )
        {
            LOG.warn( "Client did not provide sufficient information " 
                    + "to narrow the Job entry to remove to a single entry.  "
                    + "This may adversely impact performance on subsequent gets already jobd." );
        }
        m_serviceManager.getDeleter(JobEntry.class).delete( BeanUtils.toMap( jobEntries ).keySet() );
        m_poolBlobStore.scheduleBlobReadLockReleaser();
    }
    

    public RpcFuture< String > startBlobWrite( final UUID jobId, final UUID blobId )
    {
        m_deadJobMonitor.activityOccurred( jobId, blobId );
        
        m_objectLock.readLock().lock();
        try
        {
            final Blob blob = m_serviceManager.getRetriever( Blob.class ).attain( blobId );
            verifyBlobNotAlreadyWritten( blob );
            return new RpcResponse<>( m_diskManager.allocateChunksForBlob( blobId ) );
        }
        finally
        {
            m_objectLock.readLock().unlock();
        }
    }


    public RpcFuture< Boolean > blobWriteCompleted(
            final UUID jobId, 
            final UUID blobId, 
            final ChecksumType checksumType,
            final String checksum,
            final Long creationDate,
            final S3ObjectProperty [] arrayObjectMetadata )
    {
        final Set< S3ObjectProperty > objectMetadata = ( null == arrayObjectMetadata ) ? new HashSet<>()
                : CollectionFactory.toSet( arrayObjectMetadata );
        m_deadJobMonitor.activityOccurred( jobId, blobId );

        m_objectLock.readLock().lock();
        try
        {
            synchronized ( m_blobWriteCompletedLock )
            {
                final BeansServiceManager transaction =
                        m_serviceManager.startTransaction();
                try
                {
                    final BlobService blobService = transaction.getService( BlobService.class );
                    final S3ObjectService objectService = transaction.getService( S3ObjectService.class );
                    final Blob blob = blobService.attain( blobId );
                    final S3Object o = objectService.attain( blob.getObjectId() );
                    verifyBlobNotAlreadyWritten( blob );
                    m_diskManager.blobLoadedToCache( blobId );  // EMPROD-2008: check byte counts before setting metadata
                    processObjectMetadata( transaction, o, objectMetadata );
                    m_bucketLogicalSizeCache.blobCreated( o.getBucketId(), blob.getLength() );
                    
                    blob.setChecksumType( checksumType );
                    blob.setChecksum( checksum );
                    blobService.update( blob, ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
                    if ( null != creationDate )
                    {
                        final Long ccd = m_customObjectCreationDates.get( o.getId() );
                        if ( null != ccd && !ccd.equals( creationDate ) )
                        {
                            throw new DataPlannerException( 
                                    GenericFailure.BAD_REQUEST,
                                    "Multiple creation dates specified for object.  Was " 
                                    + ccd + ", but is now " + creationDate + "." );
                        }
                        m_customObjectCreationDates.put( o.getId(), creationDate );
                    }
                    final boolean objectCompletedWithCustomCreationDate = 
                            checkIfObjectCompleted( o, transaction );
                    transaction.commitTransaction();
                    blobLoadedToCache( jobId, blob );
                    if ( objectCompletedWithCustomCreationDate )
                    {
                        m_customObjectCreationDates.remove( o.getId() );
                    }
                    if ( null == o.getCreationDate() )
                    {
                        return null;
                    }
                    return new RpcResponse<>( objectCompletedWithCustomCreationDate );
                }
                finally
                {
                    transaction.closeTransaction();
                }
            }
        }
        finally
        {
            m_objectLock.readLock().unlock();
        }
    }
    
    
    private void verifyBlobNotAlreadyWritten( final Blob blob )
    {
        if ( null != blob.getChecksum() )
        {
            throw new DataPlannerException( 
                    GenericFailure.CONFLICT, 
                    "Blob " + blob.getId() + " has already been written." );
        }
    }
    
    
    private void processObjectMetadata( 
            final BeansServiceManager transaction, 
            final S3Object o,
            final Set< S3ObjectProperty > objectMetadata )
    {
        // The object id on the S3ObjectProperty is random - set it to the correct value
        for ( S3ObjectProperty property : objectMetadata )
        {
            property.setObjectId( o.getId() );
        }

        final Map< String, String > metadataInDb = toMetadataMap(
                transaction.getRetriever( S3ObjectProperty.class ).retrieveAll( 
                        S3ObjectProperty.OBJECT_ID, o.getId() ).toSet() );

        final List< S3ObjectProperty > overlappingMetadata = new ArrayList<>();
        final List< S3ObjectProperty > newMetadata = new ArrayList<>();

        for (S3ObjectProperty property : objectMetadata) {
            if (metadataInDb.containsKey(property.getKey())) {
                overlappingMetadata.add(property);
            } else {
                newMetadata.add(property);
            }
        }
        
        final List< String > conflictingKeys = new ArrayList<>();
        for ( S3ObjectProperty property : overlappingMetadata )
        {
            final String existingValue = metadataInDb.get( property.getKey() );
            if ( !existingValue.equals( property.getValue() ) )
            {
                conflictingKeys.add( property.getKey() );
            }
        }

        if ( !conflictingKeys.isEmpty() )
        {
            final Map< String, String > incomingMetadata = toMetadataMap( objectMetadata );
            throw new DataPlannerException( 
                    GenericFailure.CONFLICT,
                    "Object metadata conflicts found. " +
                    "Existing metadata: " + metadataInDb + ", " +
                    "incoming metadata: " + incomingMetadata + ", " +
                    "conflicting keys: " + conflictingKeys + "." );
        }
        
        // Create new metadata properties in the database
        if ( !newMetadata.isEmpty() )
        {
            transaction.getService( S3ObjectPropertyService.class ).createProperties( 
                    o.getId(), newMetadata );
        }
    }
    
    
    private Map< String, String > toMetadataMap( final Set< S3ObjectProperty > metadata )
    {
        final Map< String, String > retval = new HashMap<>();
        for ( final S3ObjectProperty m : metadata )
        {
            retval.put( m.getKey(), m.getValue() );
        }
        
        return retval;
    }
    
    
    private void blobLoadedToCache( final UUID jobId, final Blob blob )
    {
        if ( null != jobId )
        {
            m_jobProgressManager.blobLoadedToCache( jobId, blob.getLength() );
            return;
        }
        
        for ( final Job job : m_serviceManager.getRetriever( Job.class ).retrieveAll( Require.exists( 
                JobEntry.class,
                JobEntry.JOB_ID,
                Require.beanPropertyEquals( BlobObservable.BLOB_ID, blob.getId() ) ) ).toSet() )
        {
            m_jobProgressManager.blobLoadedToCache( job.getId(), blob.getLength() );
        }
    }
    
    
    /**
     * @return TRUE if the object has completed with a custom creation date; FALSE otherwise
     */
    private boolean checkIfObjectCompleted(
            final S3Object object,
            final BeansServiceManager transaction )
    {
        final S3ObjectService objectService = transaction.getService( S3ObjectService.class );
        if ( !objectService.isEveryBlobReceived( object.getId() ) )
        {
            return false;
        }
        
        final Set< Blob > blobs = BeanUtils.sort( transaction.getRetriever( Blob.class ).retrieveAll( 
                Blob.OBJECT_ID, object.getId() ).toSet() );
        final List< String > checksums = new ArrayList<>();
        for ( final Blob blob : blobs )
        {
            checksums.add( blob.getChecksum() );
        }
        
        final boolean customEtag = 
                ( transaction.getRetriever( S3ObjectProperty.class ).any( Require.all(
                        Require.beanPropertyEquals(
                                S3ObjectProperty.OBJECT_ID, object.getId() ),
                        Require.beanPropertyEquals(
                                KeyValueObservable.KEY, S3HeaderType.ETAG.getHttpHeaderName() ) ) ) );
        if ( !customEtag )
        {
            final S3ObjectProperty etagProperty = BeanFactory.newBean( S3ObjectProperty.class );
            etagProperty.setKey( S3HeaderType.ETAG.getHttpHeaderName() );
            etagProperty.setValue( S3Utils.getObjectETag( checksums ) );
            transaction.getService( S3ObjectPropertyService.class ).createProperties( 
                    object.getId(), CollectionFactory.toList( etagProperty ) );
        }
        if ( null != object.getCreationDate() )
        {
            throw new IllegalStateException(
                    "Object " + object.getId() + " already has a creation date, so will not set it." );
        }
        
        final Long customCreationDate = m_customObjectCreationDates.get( object.getId() );
        if ( ( null != customCreationDate ) != customEtag )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST,
                    "Either a custom creation date AND custom ETag must be specified or neither." );
        }
        if ( null == customCreationDate )
        {
        	objectService.markObjectReceived( object );
        }
        else
        {
        	objectService.markObjectReceived( object, new Date( customCreationDate ) );
        }
        return ( null != customCreationDate );
    }

    
    public RpcFuture< DeleteObjectsResult > deleteObjects( 
            final UUID userId,
            final PreviousVersions previousVersions,
            final UUID [] objectIds )
    {
        Validations.verifyNotNull( "Object id", objectIds );
        verifyNotQuiesced();

        m_objectLock.writeLock().lock();
        try
        {
            return new RpcResponse<>( deleteObjectsInternal(
                    CollectionFactory.toSet( objectIds ),
                    previousVersions,
                    null ) );
        }
        finally
        {
            m_objectLock.writeLock().unlock();
        }
    }
    
    
    private DeleteObjectsResult deleteObjectsInternal(
            final Set< UUID > objectIds, 
            final PreviousVersions previousVersions,
            final Job jobToCancel )
    {
        final DeleteS3ObjectsPreCommitListener listener = ( null == jobToCancel ) ?
                null 
                : transaction -> stageJobCancellation( transaction, jobToCancel, true );

        final S3ObjectService.DeleteResult deleteResult = m_serviceManager.getService( S3ObjectService.class )
                                                                          .delete( previousVersions, objectIds,
                                                                                  listener );
        final DeleteObjectsResult retval = deleteResult.toDeleteObjectsResult();
        m_poolBlobStore.scheduleBlobReadLockReleaser();
        if ( previousVersions == PreviousVersions.DELETE_ALL_VERSIONS
                || previousVersions == PreviousVersions.DELETE_SPECIFIC_VERSION ) {
            m_diskManager.processDeleteResults(deleteResult);
        }
        return retval;
    }
    
    
    private void stageJobCancellation( final BeansServiceManager transaction, final Job staleJob, final boolean createCanceledJob )
    {
        m_jobProgressManager.flush();
        final Job job = transaction.getRetriever( Job.class ).attain( staleJob.getId() );
        final CanceledJob cjBean = BeanFactory.newBean( CanceledJob.class );
        BeanCopier.copy( cjBean, job );
        if ( createCanceledJob ) {
            transaction.getService( CanceledJobService.class ).delete( job.getId() );
            transaction.getService( CanceledJobService.class ).create( cjBean );
        }
        transaction.getService( JobService.class ).delete( job.getId() );
        transaction.getService( JobService.class ).autoEjectTapes( 
                job.getBucketId(), StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION, m_tapeBlobStore );
    }


    public RpcFuture< ? > deleteBucket( final UUID userId, final UUID bucketId, final boolean deleteObjects )
    {
        Validations.verifyNotNull( "Bucket id", bucketId );
        verifyNotQuiesced();
        
        m_bucketLock.writeLock().lock();
        m_objectLock.writeLock().lock();
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            deleteBucketInternal( transaction, bucketId, deleteObjects );
        }
        finally
        {
            transaction.closeTransaction();
            m_objectLock.writeLock().unlock();
            m_bucketLock.writeLock().unlock();
        }
        
        return null;
    }
    
    
    public RpcFuture< ? > undeleteObject( final UUID userId, final S3Object object )
    {
    	final BeansServiceManager transaction = m_serviceManager.startTransaction();
    	try
    	{
    		final S3Object mostRecentVersion = transaction.getService( S3ObjectService.class )
    				.getMostRecentVersion( object.getBucketId(), object.getName() );
            if ( !mostRecentVersion.getId().equals( object.getId() ) )
            {
                throw new DataPlannerException( 
                        GenericFailure.CONFLICT, 
                        "Version " + object.getId() + " of object " + object.getName() + " cannot be undeleted"
                        		+ " because it is not the most recent version available. To undelete this version,"
                        		+ " please either reupload it, or permanently delete all newer versions from the"
                        		+ " system using the versionId query parameter.");
            }
            else if ( mostRecentVersion.isLatest() )
            {
            	throw new DataPlannerException( 
                        GenericFailure.CONFLICT, 
                        "Version " + object.getId() + " of object " + object.getName() + " is already the latest copy"
                        		+ " and does not need to be undeleted." );
                        
            }
    		transaction.getUpdater( S3Object.class ).update( object.setLatest( true ), S3Object.LATEST );
            final BucketHistoryEvent event = BeanFactory.newBean( BucketHistoryEvent.class )
                    .setObjectName( object.getName() )
                    .setObjectCreationDate( object.getCreationDate() )
                    .setBucketId( object.getBucketId() )
                    .setVersionId( object.getId() )
                    .setType(BucketHistoryEventType.MARK_LATEST);
            event.setId(UUID.randomUUID());
            transaction.getCreator(BucketHistoryEvent.class).create( event );
    		transaction.commitTransaction();
    		long minSequenceNumber = new BucketHistoryEventRM(event.getId(), m_serviceManager).getSequenceNumber();
            m_serviceManager.getNotificationEventDispatcher().queueFire( new BucketNotificationEvent(
                    new BucketRM( object.getBucketId(), m_serviceManager ).unwrap(),
                    m_serviceManager.getRetriever(BucketChangesNotificationRegistration.class ),
                    new BucketChangesNotificationPayloadGenerator( minSequenceNumber, m_serviceManager.getRetriever( BucketHistoryEvent.class ) ) ) );
    	}
    	finally
    	{
    		transaction.closeTransaction();
    	}
    	return null;
    }
    
    
    private void deleteBucketInternal(
            final BeansServiceManager transaction,
            final UUID bucketId,
            final boolean deleteObjects )
    {
        final S3ObjectService objectService = transaction.getService( S3ObjectService.class );
        final BucketService bucketService = transaction.getService( BucketService.class );
        final String bucketName = m_serviceManager.getRetriever( Bucket.class )
                                                  .attain( bucketId )
                                                  .getName();
    
        if ( deleteObjects )
        {
            objectService.deleteByBucketId( bucketId );
        }
        else if ( 0 < objectService.retrieveNumberOfObjectsInBucket( bucketId ) )
        {
            throw new DataPlannerException(
                    AWSFailure.BUCKET_NOT_EMPTY,
                    "Tried to delete a non-empty bucket without the force delete objects flag." );
        }
    
        bucketService.delete( bucketId );
        transaction.commitTransaction();
        m_diskManager.deleteBucket( bucketId, bucketName );
        m_bucketLogicalSizeCache.bucketDeleted( bucketId );
    }

    private int getMaxNumberOfConcurrentJobs()
    {
        return m_serviceManager
                .getRetriever( DataPathBackend.class ).attain( Require.nothing() ).getMaxNumberOfConcurrentJobs();
    }

    
    public RpcFuture< String > dumpHeap()
    {
        return new RpcResponse<>( Objects.requireNonNull( HeapDumper.dumpAndZipHeap(false) )
                                         .getAbsolutePath() );
    }
    
    
    public RpcFuture< ? > forceFullCacheReclaimNow()
    {
        m_diskManager.forceFullCacheReclaimNow();
        return null;
    }
    
    
    public RpcFuture< ? > forceTargetEnvironmentRefresh()
    {
        for ( final TargetBlobStore store : m_targetBlobStores )
        {
            store.refreshEnvironmentNow();
        }
        return null;
    }
    
    
    public RpcFuture< CacheInformation > getCacheState( final boolean includeCacheEntries )
    {
        return new RpcResponse<>( m_diskManager.getCacheState( includeCacheEntries ) );
    }


    public RpcFuture< BlobStoreTasksInformation > getBlobStoreTasks( final BlobStoreTaskState [] states )
    {
        return getBlobStoreTasksForJob( null, states );
    }


    private Set< ? extends BlobStoreTask > getTasks(final BlobStore blobStore, final UUID jobId) {
        if (jobId == null) {
            return blobStore.getTasks();
        }
        return blobStore.getTasksForJob(jobId);
    }


    public RpcFuture< BlobStoreTasksInformation > getBlobStoreTasksForJob(
            final UUID jobId,
            final BlobStoreTaskState [] states ) {
        final Set< BlobStoreTaskState > statesSet = CollectionFactory.toSet( states );
        final List< BlobStoreTask > blobStoreTasks = new ArrayList<>();
        for ( final BlobStoreTask task : getTasks(m_tapeBlobStore, jobId) )
        {
            if ( statesSet.contains( task.getState() ) )
            {
                blobStoreTasks.add( task );
            }
        }
        for ( final BlobStoreTask task : getTasks(m_poolBlobStore, jobId) )
        {
            if ( statesSet.contains( task.getState() ) )
            {
                blobStoreTasks.add( task );
            }
        }
        for ( final TargetBlobStore store : m_targetBlobStores )
        {
            for ( final BlobStoreTask task : getTasks(store, jobId) )
            {
                if ( statesSet.contains( task.getState() ) )
                {
                    blobStoreTasks.add( task );
                }
            }
        }

        blobStoreTasks.sort( new BeanComparator<>( BlobStoreTask.class,
                new BeanPropertyComparisonSpecifiction( BlobStoreTask.STATE, Direction.DESCENDING, null ),
                new BeanPropertyComparisonSpecifiction( BlobStoreTask.PRIORITY, Direction.ASCENDING, null ),
                new BeanPropertyComparisonSpecifiction( BlobStoreTask.ID, Direction.ASCENDING, null ) ) );

        final List< BlobStoreTaskInformation > tasks = new ArrayList<>();
        for ( final BlobStoreTask t : blobStoreTasks )
        {
            final BlobStoreTaskInformation bsti = BeanFactory.newBean( BlobStoreTaskInformation.class );
            BeanCopier.copy( bsti, t );
            tasks.add( bsti );
        }

        final BlobStoreTasksInformation retval = BeanFactory.newBean( BlobStoreTasksInformation.class );
        retval.setTasks( CollectionFactory.toArray( BlobStoreTaskInformation.class, tasks ) );
        return new RpcResponse<>( retval );
    }
    
    
    public RpcFuture< LogicalUsedCapacityInformation > getLogicalUsedCapacity( final UUID [] bucketIds )
    {
        Validations.verifyNotNull( "Bucket ids", bucketIds );
        final long [] retval = new long[ bucketIds.length ];
        int i = -1;
        for ( final UUID bucketId : bucketIds )
        {
            retval[ ++i ] = m_bucketLogicalSizeCache.getSize( bucketId );
        }
        
        return new RpcResponse<>( BeanFactory.newBean( LogicalUsedCapacityInformation.class )
                .setCapacities( retval ) );
    }
    
    
    @Override
    public RpcFuture< ? > cleanUpCompletedJobsAndJobChunks()
    {
        final JobService jobService = m_serviceManager.getService( JobService.class );
        jobService.cleanUpCompletedJobsAndJobChunks( m_jobProgressManager, m_tapeBlobStore,
                m_jobCreator.getJobReshapingLock() );
        return null;
    }
    
    
    public RpcFuture< ? > validateFeatureKeysNow()
    {
        m_featureKeyValidator.run();
        return null;
    }


    @Override
    public RpcFuture< ? > modifyJob(final UUID jobId, final BlobStoreTaskPriority priority)
    {
        modifyPriorityForTasks( jobId, priority, m_tapeBlobStore );
        modifyPriorityForTasks( jobId, priority, m_poolBlobStore );
        for ( final TargetBlobStore store : m_targetBlobStores )
        {
            modifyPriorityForTasks( jobId, priority, store );
        }
        return null;
    }


    private static void modifyPriorityForTasks( final UUID jobId, final BlobStoreTaskPriority priority, final BlobStore blobStore ) {
        for ( final BlobStoreTask task : blobStore.getTasksForJob( jobId ) ) {
            task.setPriority( priority );
        }
    }
    

    private final DiskManager m_diskManager;
    private final JobCreator m_jobCreator;
    private final TapeBlobStore m_tapeBlobStore;
    private final PoolBlobStore m_poolBlobStore;
    private final Set< TargetBlobStore > m_targetBlobStores = new CopyOnWriteArraySet<>();
    private final DeadJobMonitor m_deadJobMonitor;
    private final BucketLogicalSizeCache m_bucketLogicalSizeCache;
    private final JobProgressManager m_jobProgressManager;
    private final Ds3ConnectionFactory m_ds3ConnectionFactory;
    private final FeatureKeyValidator m_featureKeyValidator;
    private final Map< UUID, Long > m_customObjectCreationDates = new HashMap<>();

    private final ReadWriteLock m_objectLock;
    private final ReadWriteLock m_bucketLock;
    private final Object m_blobWriteCompletedLock = new Object();
    
    private final static Logger LOG = Logger.getLogger( DataPlannerResourceImpl.class );
}
