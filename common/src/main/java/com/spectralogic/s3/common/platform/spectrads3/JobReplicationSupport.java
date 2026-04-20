/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectrads3;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.platform.persistencetarget.BlobDestinationUtils;
import com.spectralogic.s3.common.rpc.dataplanner.domain.PersistenceTargetInfo;
import com.spectralogic.util.db.query.Require;
import lombok.NonNull;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobChunkToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

public final class JobReplicationSupport
{
	public JobReplicationSupport( 
            final BeansServiceManager serviceManager,
            final DetailedJobToReplicate jobToReplicate )
    {
		this( serviceManager, jobToReplicate, IomType.NONE);
    }
	
	
    /**
     * Use this constructor to get support for a job to replicate here that was created elsewhere.
     */
    public JobReplicationSupport( 
            final BeansServiceManager serviceManager,
            final DetailedJobToReplicate jobToReplicate,
    		final IomType isIom )
    {
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        Validations.verifyNotNull( "Job to replicate", jobToReplicate );
        Validations.verifyNotNull( "User id", jobToReplicate.getUserId() );
        Validations.verifyNotNull( "Bucket id", jobToReplicate.getBucketId() );
        
        m_job = BeanFactory.newBean( Job.class ).setUserId( jobToReplicate.getUserId() );
        m_user = serviceManager.getRetriever( User.class ).attain( jobToReplicate.getUserId() );
        m_bucket = m_serviceManager.getRetriever( Bucket.class ).attain( jobToReplicate.getBucketId() );
        m_job.setBucketId( m_bucket.getId() );
        m_job.setId( jobToReplicate.getJob().getId() );
        m_job.setChunkClientProcessingOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER );
        m_job.setRequestType( JobRequestType.PUT );
        m_job.setDeadJobCleanupAllowed( false );
        m_job.setReplicating( true );
        m_job.setName( jobToReplicate.getJob().getName() );
        m_job.setIomType( isIom );
        m_job.setVerifyAfterWrite( jobToReplicate.isVerifyAfterWrite() );
        m_restore = isIom;

        m_blobs = CollectionFactory.toSet( jobToReplicate.getJob().getBlobs() );
        m_objects = CollectionFactory.toSet( jobToReplicate.getJob().getObjects() );
        Set< UUID > existingBlobIds = new HashSet<>();
        if ( IomType.NONE == m_restore )
        {
        	existingBlobIds = stripOutExistingDaoTypes();
        }
        else
        {
            m_job.setCachedSizeInBytes( jobToReplicate.getCachedSizeInBytes().longValue() );
        }
        computeJobSize();
        
        m_jobChunks = new HashSet<>();
        for ( final JobChunkToReplicate chunk : jobToReplicate.getJob().getChunks() )
        {
            if (existingBlobIds.contains( chunk.getBlobId() ))
            {
                continue;
            }
            final JobEntry daoChunk = BeanFactory.newBean( JobEntry.class );
            daoChunk.setChunkNumber( chunk.getChunkNumber() );
            daoChunk.setId( chunk.getId() );
            daoChunk.setBlobId( chunk.getBlobId() );
            daoChunk.setJobId( m_job.getId() );
            if ( IomType.NONE != m_restore && null != chunk.getOriginalChunkId() )
            {
            	m_getChunks.put( daoChunk.getId(), chunk.getOriginalChunkId() );
            }
            
            m_jobChunks.add( daoChunk );
        }
        
        m_retval.setBucketId( m_job.getBucketId() );
        m_retval.setUserId( jobToReplicate.getUserId() );
        m_retval.setJob( jobToReplicate.getJob() );
        if ( null != jobToReplicate.getPriority() )
        {
            m_job.setPriority( jobToReplicate.getPriority() );
        }
        m_retval.setPriority( m_job.getPriority() );
    }
    
    
    private Set< UUID > stripOutExistingDaoTypes()
    {
        // Strip away objects that already exist with a precise match
        final Set< S3Object > existingObjects = m_serviceManager.getRetriever( S3Object.class ).retrieveAll(
                BeanUtils.< UUID >extractPropertyValues( m_objects, Identifiable.ID ) ).toSet();
        final Set< UUID > existingObjectIds = BeanUtils.< UUID >extractPropertyValues( 
                existingObjects,
                Identifiable.ID );
        UUID oldBucketId = null;
        final Map< UUID, Set< Blob > > blobs = new HashMap<>();
        for ( final S3Object o : new HashSet<>( m_objects ) )
        {
            if ( null == oldBucketId )
            {
                oldBucketId = o.getBucketId();
            }
            if ( !oldBucketId.equals( o.getBucketId() ) )
            {
                throw new FailureTypeObservableException( 
                        GenericFailure.BAD_REQUEST,
                        "Objects cannot span multiple buckets within a single job: " + oldBucketId + ", " 
                        + o.getBucketId() );
            }
            o.setBucketId( m_bucket.getId() );
            if ( existingObjectIds.contains( o.getId() ) )
            {
                m_objects.remove( o );
            }
        }
        
        // Strip away blobs that already exist with a precise match
        final Set< UUID > existingBlobIds = BeanUtils.< UUID >extractPropertyValues( 
                m_serviceManager.getRetriever( Blob.class ).retrieveAll(
                        BeanUtils.< UUID >extractPropertyValues( m_blobs, Identifiable.ID ) ).toSet(),
                Identifiable.ID );
        for ( final Blob blob : new HashSet<>( m_blobs ) )
        {
            blob.setChecksum( null );
            blob.setChecksumType( null );
            if ( existingBlobIds.contains( blob.getId() ) )
            {
                m_blobs.remove( blob );
                existingBlobIds.add( blob.getId() );
            }
            else
            {
                if ( !blobs.containsKey( blob.getObjectId() ) )
                {
                    blobs.put( blob.getObjectId(), new HashSet< Blob >() );
                }
                blobs.get( blob.getObjectId() ).add( blob );
            }
        }
        if ( !existingBlobIds.isEmpty() )
        {
            LOG.info( existingBlobIds.size() 
                    + " blobs do not need to be replicated since they already exist." );
        }
        
        // Validate blobs to objects
        for ( final S3Object o : m_objects )
        {
            long expectedOffset = 0;
            final Set< Blob > oBlobs = ( blobs.containsKey( o.getId() ) ) ?
                    BeanUtils.sort( blobs.remove( o.getId() ) )
                    : new HashSet< Blob >();
            for ( final Blob blob : oBlobs )
            {
                if ( expectedOffset != blob.getByteOffset() )
                {
                    throw new FailureTypeObservableException( 
                            GenericFailure.BAD_REQUEST, 
                            "Expected blob " + blob.getId() + " to have byte offset " 
                            + expectedOffset + "." );
                }
                expectedOffset += blob.getLength();
            }
            if ( oBlobs.isEmpty() )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "Object " + o.getId() + " does not define at least 1 blob." );
            }
            o.setCreationDate( null );
        }
        
        if ( !blobs.isEmpty() )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    blobs.size() + " blobs are orphaned from their objects." );
        }
        
        return existingBlobIds;
    }
    
    
    private void computeJobSize()
    {
        long jobSize = 0;
        for ( final Blob blob : m_blobs )
        {
            jobSize += blob.getLength();
        }
        m_job.setOriginalSizeInBytes( jobSize );
    }
    

    /**
     * Use this constructor to get support for generating a job to replicate payload from a job that already
     * exists here - to be used by sending to another appliance to replicate the job here elsewhere.
     */
    public JobReplicationSupport(
            @NonNull final BeansServiceManager serviceManager,
            final UUID jobId )
    {
    	m_restore =  IomType.NONE;
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        
        m_job = serviceManager.getRetriever( Job.class ).attain( jobId );
        if ( m_job.isAggregating() )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "Cannot replicate an aggregating job." );
        }
        
        m_bucket = serviceManager.getRetriever( Bucket.class ).attain( m_job.getBucketId() );
        m_user = serviceManager.getRetriever( User.class ).attain( m_job.getUserId() );
        m_jobChunks = serviceManager.getRetriever( JobEntry.class ).retrieveAll(
                JobEntry.JOB_ID, m_job.getId() ).toSet();
        
        m_retval.setBucketId( m_bucket.getId() );
        m_retval.setUserId( m_user.getId() );
        m_retval.setPriority( m_job.getPriority() );
        
        m_blobs = serviceManager.getRetriever( Blob.class ).retrieveAll( 
                BeanUtils.< UUID >extractPropertyValues( m_jobChunks, BlobObservable.BLOB_ID ) ).toSet();
        m_objects = serviceManager.getRetriever( S3Object.class ).retrieveAll( 
                BeanUtils.< UUID >extractPropertyValues( m_blobs, Blob.OBJECT_ID ) ).toSet();
        m_retval.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        populateJobChunksToReplicateFromExistingJob();
        m_retval.getJob().setBlobs( CollectionFactory.toArray( Blob.class, m_blobs ) );
        m_retval.getJob().setObjects( CollectionFactory.toArray( S3Object.class, m_objects ) );
        m_retval.getJob().setId( m_job.getId() );
        m_retval.getJob().setName( "Replicate " + m_job.getName() );
        
        if ( 0 < m_serviceManager.getRetriever( Job.class ).attain( m_job.getId() )
                .getCompletedSizeInBytes() )
        {
            LOG.warn( "Part of job " + m_job.getId() 
                      + " has already partially completed on source.  " 
                      + "Will permit job to be acceped here (a target) anyway." );
        }
    }
    
    
    private void populateJobChunksToReplicateFromExistingJob()
    {
        final Set< JobChunkToReplicate > retval = new HashSet<>();
        for ( final JobEntry chunk : m_jobChunks )
        {
            final JobChunkToReplicate jctr = BeanFactory.newBean( JobChunkToReplicate.class );
            jctr.setChunkNumber( chunk.getChunkNumber() );
            jctr.setId( chunk.getId() );
            jctr.setBlobId( chunk.getBlobId() );
            retval.add( jctr );
        }
        m_retval.getJob().setChunks( CollectionFactory.toArray( JobChunkToReplicate.class, retval ) );
    }
    
    
    public DetailedJobToReplicate getJobToReplicate()
    {
        return m_retval;
    }
    
    
    public Job getJob()
    {
        return m_job;
    }
    
    
    public Set< Blob > getBlobs()
    {
        return m_blobs;
    }
    
    
    public Set< S3Object > getObjects()
    {
        return m_objects;
    }
    
    
    public Set<JobEntry> getJobChunks()
    {
        return m_jobChunks;
    }


    public UUID commit(final CacheManager cacheManager, final PersistenceTargetInfo persistenceTargetInfo)
    {
        if (persistenceTargetInfo.getStorageDomainIds().length == 0
                && persistenceTargetInfo.getDs3TargetIds().length == 0
                && persistenceTargetInfo.getS3TargetIds().length == 0
                && persistenceTargetInfo.getAzureTargetIds().length == 0
                && m_job.getIomType() != IomType.STAGE) {
            throw new IllegalStateException("Job " + m_job.getId() + " needs to persist somewhere.");
        }
        if ( m_jobChunks.isEmpty() )
        {
            return null;
        }

        LOG.info( "Replicating PUT job " + m_job.getId() + " to this appliance..." );
        if ( IomType.NONE == m_restore )
        {
        	ensureCacheDoesNotContainBlobs( cacheManager );
        }
        
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( BucketService.class ).initializeLogicalSizeCache(
                    m_serviceManager.getService( BucketService.class ) );
            final JobEntryService chunkService = transaction.getService( JobEntryService.class );
            if ( IomType.NONE == m_restore )
            {
                transaction.getService( S3ObjectService.class ).create( m_objects );
            	transaction.getService( BlobService.class ).create( m_blobs );
            }
            transaction.getService( JobService.class ).create( m_job );
            chunkService.create( m_jobChunks );

            if (m_job.getRequestType() == JobRequestType.PUT) {
                createJobChunkPersistenceTargets(CollectionFactory.toSet(persistenceTargetInfo.getStorageDomainIds()), transaction);
                createJobChunkDs3Targets(CollectionFactory.toSet(persistenceTargetInfo.getDs3TargetIds()), transaction);
                createJobChunkS3Targets(CollectionFactory.toSet(persistenceTargetInfo.getS3TargetIds()), transaction);
                createJobChunkAzureTargets(CollectionFactory.toSet(persistenceTargetInfo.getAzureTargetIds()), transaction);
            }

            transaction.commitTransaction();
            if ( IomType.NONE != m_restore )
            {
                LOG.info( "Successfully created IOM PUT job " + m_job.getId() );
            }
            else
            {
                LOG.info( "Successfully replicated PUT job " + m_job.getId() + " to this appliance." );
            }
            
            return m_job.getId();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }

    private void createJobChunkPersistenceTargets(final Set<UUID> storageDomainIds, final BeansServiceManager transaction) {
        final Set<DataPersistenceRule> rules = transaction.getRetriever( DataPersistenceRule.class ).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals(DataPersistenceRule.DATA_POLICY_ID, m_bucket.getDataPolicyId()),
                        Require.beanPropertyEqualsOneOf(DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainIds)
                )
        ).toSet();
        BlobDestinationUtils.createLocalBlobDestinations(m_jobChunks, rules, m_bucket.getId(), transaction);
    }

    private void createJobChunkDs3Targets(final Set<UUID> ds3TargetIds, final BeansServiceManager transaction) {
        final Set<Ds3DataReplicationRule> rules = transaction.getRetriever( Ds3DataReplicationRule.class ).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals(Ds3DataReplicationRule.DATA_POLICY_ID, m_bucket.getDataPolicyId()),
                        Require.beanPropertyEqualsOneOf(Ds3DataReplicationRule.TARGET_ID, ds3TargetIds)
                )
        ).toSet();
        BlobDestinationUtils.createDs3BlobDestinations(m_jobChunks, rules, transaction);
    }

    private void createJobChunkAzureTargets(final Set<UUID> azureTargetIds, final BeansServiceManager transaction) {
        final Set<AzureDataReplicationRule> rules = transaction.getRetriever( AzureDataReplicationRule.class ).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals(AzureDataReplicationRule.DATA_POLICY_ID, m_bucket.getDataPolicyId()),
                        Require.beanPropertyEqualsOneOf(AzureDataReplicationRule.TARGET_ID, azureTargetIds)
                )
        ).toSet();
        BlobDestinationUtils.createAzureBlobDestinations(m_jobChunks, rules, transaction);
    }

    private void createJobChunkS3Targets(final Set<UUID> s3TargetIds, final BeansServiceManager transaction) {
        final Set<S3DataReplicationRule> rules = transaction.getRetriever( S3DataReplicationRule.class ).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals(S3DataReplicationRule.DATA_POLICY_ID, m_bucket.getDataPolicyId()),
                        Require.beanPropertyEqualsOneOf(S3DataReplicationRule.TARGET_ID, s3TargetIds)
                )
        ).toSet();
        BlobDestinationUtils.createS3BlobDestinations(m_jobChunks, rules, transaction);
    }

    /**
     * This method is essential to prevent bug BLKP-2947 - fix modified in EMPROD-1115
     */
    private void ensureCacheDoesNotContainBlobs( final CacheManager cacheManager )
    {
        final int blobsStuckInCache = getBlobsAllocatedInCache( cacheManager ).size();
        if ( 0 != blobsStuckInCache )
        {            
        	throw new FailureTypeObservableException(
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                    "Cannot re-replicate this job. " + blobsStuckInCache + " blobs from previous replication are"
                    + " still pending asynchronous reclaim from cache. Please reclaim cache or try again later." );
        }
    }
    
    
    private Set< UUID > getBlobsAllocatedInCache( final CacheManager cacheManager )
    {
        final Set< UUID > retval = new HashSet<>();
        for ( final Blob blob : m_blobs )
        {
            if ( cacheManager.isCacheSpaceAllocated( blob.getId() ) )
            {
                retval.add( blob.getId() );
            }
        }
        
        return retval;
    }
    
    
    private final Bucket m_bucket;
    private final User m_user;
    private final Job m_job;
    private final Set< Blob > m_blobs;
    private final Set< S3Object > m_objects;
    private final Set<JobEntry> m_jobChunks;
    private final IomType m_restore;
    private final BeansServiceManager m_serviceManager;
    private final DetailedJobToReplicate m_retval = BeanFactory.newBean( DetailedJobToReplicate.class );
    private final Map< UUID, UUID > m_getChunks = new HashMap<>();
    
    private final static Logger LOG = Logger.getLogger( JobReplicationSupport.class );
}
