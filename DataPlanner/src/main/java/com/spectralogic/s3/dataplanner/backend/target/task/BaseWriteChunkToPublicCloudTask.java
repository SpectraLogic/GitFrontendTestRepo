/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.target.PublicCloudBucketNameService;
import com.spectralogic.s3.common.dao.service.target.TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupport;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;

public abstract class BaseWriteChunkToPublicCloudTask 
    < T extends DatabasePersistable & PublicCloudReplicationTarget< T >,
      BT extends DatabasePersistable & BlobTarget< BT >,
      SBT extends DatabasePersistable & BlobTarget< BT >,
      R extends PublicCloudDataReplicationRule< R >,
      D extends DatabasePersistable & RemoteBlobDestination< D >,
      CF extends PublicCloudConnectionFactory< ?, T >>
    extends BasePublicCloudTask< T, CF >
{

    protected BaseWriteChunkToPublicCloudTask(
            final Class< T > targetType,
            final Class< ? extends PublicCloudBucketNameService< ? > > cloudBucketNameServiceType,
            final Class< ? extends TargetFailureService< ? > > failureServiceType,
            final Class< BT > blobTargetType,
            final Class< SBT > suspectBlobTargetType,
            final Class< R > ruleType,
            final Class<D> chunkTargetType,
            final TargetWriteDirective< T, D> writeDirective,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager,
            final CF connectionFactory )
    {
        super( cloudBucketNameServiceType,
                targetType,
                writeDirective.getTarget().getId(),
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory,
                writeDirective.getPriority() );
        
        m_failureServiceType = failureServiceType;
        m_blobTargetType = blobTargetType;
        m_suspectBlobTargetType = suspectBlobTargetType;
        m_ruleType = ruleType;
        m_chunkTargetType = chunkTargetType;
        m_writeDirective = writeDirective;
        Validations.verifyNotNull( "Failure service type", m_failureServiceType );
        Validations.verifyNotNull( "Blob target type", m_blobTargetType );
        Validations.verifyNotNull( "Rule type", m_ruleType );
    }

    
    @Override
    protected boolean prepareForExecution()
    {
        m_serviceManager.getService(JobEntryService.class).verifyEntriesExist(BeanUtils.extractPropertyValues(m_writeDirective.getEntries(), Identifiable.ID));
        return true;
    }

    
    @Override
    protected BlobStoreTaskState runInternal()
    {
        final Bucket bucket = m_writeDirective.getBucket();
        final R rule = getServiceManager().getRetriever( m_ruleType ).attain( Require.all( 
                Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, bucket.getDataPolicyId() ),
                Require.beanPropertyEquals( DataReplicationRule.TARGET_ID, getTargetId() ) ) );
        
        PublicCloudConnection connection = null;
        try
        {
            connection = getConnectionFactory().connect( getTarget() );
            PublicCloudBucketInformation cloudBucket;
            try
            {
                synchronized ( BUCKET_LOCK )
                {
                    cloudBucket = getCloudBucketSupport().verifyBucketForWrite(
                            connection, bucket.getId(), getTargetId() );
                    if ( null == cloudBucket )
                    {
                        final String cloudBucketName = getCloudBucketSupport().attainCloudBucketName(
                                bucket.getId(), getTargetId() );
                        cloudBucket = createCloudBucket( bucket, rule, cloudBucketName, connection );
                        
                        /*
                         * The original cloud-out design was for the cloud provider to be eventually 
                         * consistent. What that means is that if we fire
                         * up a bunch of write chunk tasks at the same time for the same cloud bucket, we'll
                         * have a race condition where a bunch of connections will try to determine if they
                         * should create the bucket and they'll be doing so in a non-transactional system
                         * that can't guarantee immediate consistency.
                         * 
                         * While data is still uploaded eventually instead of immediately, bucket creates
                         * are now synchronous for all create bucket requests to the BP. While explicit
                         * bucket create requests to the ds3 server will comprise the vast majority of bucket
                         * creation, this code will be left in place to facilitate bucket creation via foreign
                         * tape import into a data policy with cloud out replication as well as adding
                         * cloud out replication to an existing data policy in IOM workflows.
                         * 
                         * When we create a bucket this way, hold the lock for an extra 100ms, and if there's a
                         * failure getting the bucket, before retrying, re-create a connection.  Between these
                         * two things, we can minimize the probability of an unnecessary unhandled exception.
                         * 
                         * Finally, note that all of this does not guarantee we won't have an unhandled
                         * exception.  This case shall be properly handled.  We just want to minimize its
                         * likelihood to avoid cluttering up the logs unnecessarily.
                         */
                        Thread.sleep( 100 ); 
                    }
                }
            }
            catch ( final RuntimeException | InterruptedException ex )
            {
                LOG.info( "Failed to verify cloud bucket information.  " 
                          + "Will retry in case there is normal concurrent activity causing this.", ex );
                synchronized ( BUCKET_LOCK )
                {
                    connection.shutdown();
                    connection = getConnectionFactory().connect( getTarget() );
                    cloudBucket = getCloudBucketSupport().verifyBucket(
                            connection, bucket.getId(), getTargetId() );
                }
            }
            
            runReplication( bucket, rule, cloudBucket, connection );
            return BlobStoreTaskState.COMPLETED;
        }
        catch ( final RuntimeException ex )
        {
            requireTaskSuspension();
            handleFailure( ex );
            throw ExceptionUtil.toRuntimeException( ex );
        }
        finally
        {
            if ( null != connection )
            {
                connection.shutdown();
            }
        }
    }
    
    
    private PublicCloudBucketInformation createCloudBucket( 
            final Bucket bucket,
            final R rule,
            final String cloudBucketName,
            final PublicCloudConnection connection )
    {
        LOG.info( "Bucket '" + cloudBucketName
                  + "'does not yet exist or is not yet owned by us." );
        return connection.createOrTakeoverBucket(
                getInitialDataPlacement( bucket, rule ),
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                .setName( cloudBucketName )
                .setLocalBucketName( bucket.getName() )
                .setOwnerId( getServiceManager().getRetriever( DataPathBackend.class ).attain(
                        Require.nothing() ).getInstanceId() )
                .setVersion( PublicCloudBucketSupport.CLOUD_VERSION ) );
    }
    
    
    private void runReplication( 
            final Bucket bucket,
            final R rule,
            final PublicCloudBucketInformation cloudBucket,
            final PublicCloudConnection connection )
    {
        final Map< UUID, Blob > blobs = BeanUtils.toMap(
                getServiceManager().getRetriever( Blob.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( 
                                m_writeDirective.getEntries(), BlobObservable.BLOB_ID ) ).toSet() );
        final Map< UUID, S3Object > objects = BeanUtils.toMap( 
                getServiceManager().getRetriever( S3Object.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( blobs.values(), Blob.OBJECT_ID ) )
                        .toSet() );
                        
        final Map< UUID, Long > objectSizes = new HashMap<>();
        for ( final UUID objectId : objects.keySet() )
        {
        	objectSizes.put( objectId,
        			getServiceManager().getService( S3ObjectService.class ).getSizeInBytes( objectId ) );
        }
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final String dataDescription =
                m_writeDirective.getEntries().size() + " blobs (" + bytesRenderer.render( m_writeDirective.getSizeInBytes() ) + ")";
        LOG.info( "Will write " + dataDescription + "..." );

        final List< Future< ? > > writeThreads = new ArrayList<>();
        final Duration duration = new Duration();
        final Object initialDataPlacement = getInitialDataPlacement( bucket, rule );
        
        for ( final JobEntry jobEntry : m_writeDirective.getEntries() )
        {
            final Blob blob = blobs.get( jobEntry.getBlobId() );
            final S3Object object = objects.get( blob.getObjectId() );
            final Set< S3ObjectProperty > metadata = ( 0 < blob.getByteOffset() ) ?
                    new HashSet<>()
                    : getServiceManager().getRetriever( S3ObjectProperty.class ).retrieveAll(
                            S3ObjectProperty.OBJECT_ID, object.getId() ).toSet();
            final DiskFileInfo diskFileInfo = getDiskManager().getDiskFileFor( blob.getId() );
            final File file;
            try {
                file = new File( diskFileInfo.getFilePath() );
            } catch (final Exception e) {
                getServiceManager().getService(BlobPoolService.class).registerFailureToRead(diskFileInfo);
                throw e;
            }
            final List< Future< ? > > tmpThreads = connection.writeBlobToCloud(
                    cloudBucket, 
                    object,
                    objectSizes.get( object.getId() ),
                    blob,
                    getServiceManager().getRetriever( Blob.class ).getCount( Blob.OBJECT_ID, object.getId() ),
                    file,
                    object.getCreationDate(),
                    metadata,
                    rule.getMaxBlobPartSizeInBytes(),
                    initialDataPlacement );
            if ( null != tmpThreads )
            {
                writeThreads.addAll( tmpThreads );
            }
        }
        for ( final Future< ? > future : writeThreads )
        {
            try
            {
                future.get();
            } catch ( ExecutionException | InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        LOG.info( dataDescription + " written to target at " 
                  + bytesRenderer.render( m_writeDirective.getSizeInBytes(), duration ) + "." );

        try (final NestableTransaction transaction = m_serviceManager.startNestableTransaction()) {
            recordBlobTargets( m_writeDirective.getEntries(), transaction );
            final Set<UUID> destinationIds = BeanUtils.toMap(m_writeDirective.getBlobDestinations()).keySet();
            m_serviceManager.getUpdater( m_chunkTargetType ).update(
                    Require.beanPropertyEqualsOneOf( Identifiable.ID, destinationIds),
                    (destination) -> destination.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ),
                    RemoteBlobDestination.BLOB_STORE_STATE);
            transaction.commitTransaction();
        }
        doNotTreatReadyReturnValueAsFailure();
    }
    
    
    protected abstract Object getInitialDataPlacement( final Bucket bucket, final R replicationRule );
    
    
    private void recordBlobTargets( final Collection<JobEntry> jobEntries, final BeansServiceManager transaction)
    {
        final Map< UUID, UUID > alreadyRecordedBlobs = BeanUtils.toMap(
                transaction.getRetriever( m_blobTargetType ).retrieveAll( Require.all(
                Require.beanPropertyEquals( BlobTarget.TARGET_ID, getTargetId() ),
                Require.beanPropertyEqualsOneOf( 
                        BlobObservable.BLOB_ID, 
                        BeanUtils.< UUID >extractPropertyValues( 
                                jobEntries,
                                BlobObservable.BLOB_ID ) ) ) ).toSet(), BlobObservable.BLOB_ID );
        final Set< SBT > suspectBlobTargets = transaction.getRetriever( m_suspectBlobTargetType )
        		.retrieveAll( alreadyRecordedBlobs.keySet() ).toSet();
        final Set< UUID > suspectBlobTargetIds = BeanUtils.toMap( suspectBlobTargets ).keySet();
        final Set< BT > blobTargets = new HashSet<>();
        for ( final JobEntry jobEntry : jobEntries )
        {
            if ( !alreadyRecordedBlobs.values().contains( jobEntry.getBlobId() ) )
            {
                blobTargets.add( BeanFactory.newBean( m_blobTargetType )
                        .setBlobId( jobEntry.getBlobId() )
                        .setTargetId( getTargetId() ) );
            }
        }
        transaction.getCreator( m_blobTargetType ).create( blobTargets );
        transaction.getDeleter( m_suspectBlobTargetType ).delete( suspectBlobTargetIds );
    }
    
    
    protected void handleFailure( final Exception ex )
    {
        getServiceManager().getService( m_failureServiceType ).create(
                getTargetId(), TargetFailureType.WRITE_FAILED, ex, null );
    }

    
    public String getDescription()
    {
        return "Write " + m_writeDirective.getEntries().size() + " blobs to " + getTarget().getName();
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_writeDirective.getEntries(), JobEntry.JOB_ID).toArray(new UUID[0]);
    }


    private final TargetWriteDirective< T, D> m_writeDirective;
    private final Class< ? extends TargetFailureService< ? > > m_failureServiceType;
    private final Class< BT > m_blobTargetType;
    private final Class< SBT > m_suspectBlobTargetType;
    private final Class< R > m_ruleType;
    private final Class<D> m_chunkTargetType;
    
    private final static Object BUCKET_LOCK = new Object();
}
