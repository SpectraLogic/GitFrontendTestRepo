/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.target.ds3target;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.JobChunkClientProcessingOrderGuarantee;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.DeleteObjectsRequest;
import com.spectralogic.ds3client.commands.DeleteObjectsResponse;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.models.ChecksumType.Type;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Credentials;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.ds3client.networking.FailedRequestUsingMgmtPortException;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.s3.common.platform.spectrads3.BlobIdsSpecification;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailure;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailureReason;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.target.TargetLogger;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.find.FlagDetector;
import com.spectralogic.util.http.HttpUtil;
import com.spectralogic.util.io.SingleInputStreamProvider;
import com.spectralogic.util.io.ThreadedDataMover;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;
import com.spectralogic.util.thread.wp.SystemWorkPool;

final class Ds3ConnectionImpl extends BaseShutdownable implements Ds3Connection
{
    Ds3ConnectionImpl(
            final UUID sourceInstanceId,
            final Ds3Target target,
            final String authorizationId,
            final String secretKey )
    {
        this( sourceInstanceId,
              target, 
              authorizationId,
              ReplicationSourceIdentifierHeaderAddingDs3ClientFactory.build( 
                      sourceInstanceId,
                      Ds3ClientBuilder.create( 
                              buildEndpoint( target ), 
                              new Credentials( authorizationId, secretKey ) )
                              .withCertificateVerification( target.isDataPathVerifyCertificate() )
                              .withHttps( target.isDataPathHttps() )
                              .withProxy( target.getDataPathProxy() )
                              .withRedirectRetries( 0 )
                              .withConnectionTimeout( 10000 )
                              .build() ) );
    }
    
    
    private static String buildEndpoint( final Ds3Target target )
    {
        String retval = HttpUtil.formatForIpV6( target.getDataPathEndPoint() );
        if ( null != target.getDataPathPort() )
        {
            retval += ":" + target.getDataPathPort();
        }
        return retval;
    }
    
    
    Ds3ConnectionImpl(
            final UUID sourceInstanceId,
            final Ds3Target target,
            final String authorizationId,
            final Ds3Client client )
    {
        doNotLogWhenShutdown();
        final Duration duration = new Duration();
        m_sourceInstanceId = sourceInstanceId;
        m_client = client;
        m_endPoint = target.getDataPathEndPoint();
        m_authorizationId = authorizationId;

        final GetSystemInformationSpectraS3Response response;
        try
        {
            response = m_client.getSystemInformationSpectraS3( new GetSystemInformationSpectraS3Request() );
        }
        catch ( final Exception ex )
        {
            log( Level.INFO,
                 "Failed to connect to " + target.getDataPathEndPoint() + " after " + duration + ".", ex );
            shutdown();
            final Throwable t = getFailure( "connect to " + target.getDataPathEndPoint(), ex );
            if ( FailureTypeObservableException.class.isAssignableFrom( t.getClass() ) )
            {
                throw (FailureTypeObservableException)t;
            }
            
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT, 
                    "Failed to connect to " + target.getDataPathEndPoint() + ".",
                    t );
        }
        
        // (1) Ensure the software version on the target is identical to that of the source
        final String sourceVersion = 
                ConfigurationInformationProvider.getInstance().getBuildInformation().getVersion() + "";
        final String targetVersion =
                response.getSystemInformationResult().getBuildInformation().getVersion() + "";
        if ( !majorVersionsMatch( sourceVersion, targetVersion )
                && !FlagDetector.isFlagSet( DISABLE_TARGET_SOFTWARE_VERSION_CHECK ) )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "The target's software revision (" + targetVersion 
                    + ") differs from the source's software revision (" + sourceVersion + ")." );
        }
        
        // (2) Ensure clock skew is acceptable
        final long msOff = Math.abs( 
                response.getSystemInformationResult().getNow() - System.currentTimeMillis() );
        if ( msOff > MAX_CLOCK_SKEW_IN_MINS * 60L * 1000 )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "Clock skew is too great with target to connect.  Skew is " + msOff 
                    + "ms and the max skew is " + MAX_CLOCK_SKEW_IN_MINS + "mins." );
        }
        
        // (3) Ensure instance id is acceptable
        if ( null != target.getId() 
                && !target.getId().equals( response.getSystemInformationResult().getInstanceId() ) )
        {
            shutdown();
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "Expected target to have instance id " + target.getId() + ", but it had instance id "
                    + response.getSystemInformationResult().getInstanceId() + "." );
        }
        if ( null == target.getId() )
        {
            target.setId( response.getSystemInformationResult().getInstanceId() );
        }
        
        TargetLogger.LOG.info( "Connected to " + target.getDataPathEndPoint() + " in " + duration + "." );
        addShutdownListener( new MyShutdownListener() );
    }
    
    
    public static boolean majorVersionsMatch(final String v1, final String v2)
    {
        if ( v1.equals(v2) )
        {
            return true;
        }
        if ( -1 != v1.indexOf( '.' ) && -1 != v2.indexOf( '.' )
                && v1.substring( 0, v1.indexOf( '.' ) ).equals( v2.substring( 0, v2.indexOf( '.' ) ) ) )
        {
            return true;
        }
        return false;
    }
    
    
    private final class MyShutdownListener extends CriticalShutdownListener
    {
        @Override
        public void shutdownOccurred()
        {
            try
            {
                m_client.close();
                TargetLogger.LOG.info( "Closed connection to " + m_endPoint + "." );
            }
            catch ( final Exception ex )
            {
                log( Level.WARN, "Failed to close connection with " + m_endPoint + ".", ex );
            }
        }
    } // end inner class def
    
    
    public void verifyIsAdministrator()
    {
        new VoidOperationRunner( "verify admin access" )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final VerifyUserIsMemberOfGroupSpectraS3Response response =
                        m_client.verifyUserIsMemberOfGroupSpectraS3(
                                new VerifyUserIsMemberOfGroupSpectraS3Request(
                                        BuiltInGroup.ADMINISTRATORS.getName() ) );
                if (response.getGroupResult() == null)
                {
                    // User is not in Admin group, but verify returned 204
                    throw new RuntimeException("Credentials to remote DS3 are not administrator credentials.");
                }
            }
        }.run( Level.WARN );
    }
    
    
    public Set< DataPolicy > getDataPolicies()
    {
        return new OperationRunner< Set< DataPolicy > >( "get data policies" )
        {
            @Override
            protected Set< DataPolicy > performOperation() throws Exception
            {
                final GetDataPoliciesSpectraS3Response response =
                        m_client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
                final Set< DataPolicy > retval = new HashSet<>();
                for ( final com.spectralogic.ds3client.models.DataPolicy model
                        : response.getDataPolicyListResult().getDataPolicies() )
                {
                    final DataPolicy dataPolicy = BeanFactory.newBean( DataPolicy.class );
                    BeanCopier.copy( dataPolicy, model );
                    retval.add( dataPolicy );
                }
                return retval;
            }
        }.run( Level.WARN );
    }
    
    
    public Set< User > getUsers()
    {
        return new OperationRunner< Set< User > >( "get users" )
        {
            @Override
            protected Set< User > performOperation() throws Exception
            {
                final GetUsersSpectraS3Response response =
                        m_client.getUsersSpectraS3( new GetUsersSpectraS3Request() );
                final Set< User > retval = new HashSet<>();
                for ( final SpectraUser model : response.getSpectraUserListResult().getSpectraUsers() )
                {
                    final User user = BeanFactory.newBean( User.class );
                    BeanCopier.copy( user, model );
                    retval.add( user );
                }
                return retval;
            }
        }.run( Level.WARN );
    }   
    
    
    public void createUser( final User user, final String dataPolicy )
    {
        new VoidOperationRunner( "create user " + user.getName() )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                m_client.delegateCreateUserSpectraS3( new DelegateCreateUserSpectraS3Request( user.getName() ).withId(
                        user.getId()
                            .toString() )
                                                                                                              .withSecretKey(
                                                                                                                      user.getSecretKey() )
                                                                                                              .withMaxBuckets(
                                                                                                                      user.getMaxBuckets() ) );
                if ( null != dataPolicy )
                {
                    m_client.modifyUserSpectraS3( 
                            new ModifyUserSpectraS3Request( user.getName() ).withDefaultDataPolicyId(
                                    dataPolicy ) );
                }
            }
        }.run( Level.INFO );
    }
    
    
    public void updateUser( final User user )
    {
        new VoidOperationRunner( "update user " + user.getName() )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                m_client.modifyUserSpectraS3(
                        new ModifyUserSpectraS3Request( user.getName() ).withSecretKey( user.getSecretKey() )
                                                                        .withMaxBuckets( user.getMaxBuckets() ) );
            }
        }.run( Level.INFO );
    }
    
    
    public void deleteUser( final String userName )
    {
        new VoidOperationRunner( "delete user " + userName )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                m_client.delegateDeleteUserSpectraS3( 
                        new DelegateDeleteUserSpectraS3Request( userName ) );
            }
        }.run( Level.INFO );
    }
    
    
    public void createDs3Target( final Ds3Target target )
    {
        new VoidOperationRunner( "pair back target" )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final RegisterDs3TargetSpectraS3Request request = new RegisterDs3TargetSpectraS3Request( 
                        target.getAdminAuthId(), 
                        target.getAdminSecretKey(),
                        target.getDataPathEndPoint(), 
                        target.getName() )
                .withDataPathHttps( target.isDataPathHttps() )
                .withDataPathVerifyCertificate( target.isDataPathVerifyCertificate() )
                .withAccessControlReplication( Ds3TargetAccessControlReplication.valueOf( 
                        target.getAccessControlReplication().name() ) )
                .withDefaultReadPreference( TargetReadPreferenceType.valueOf( 
                        target.getDefaultReadPreference().name() ) )
                .withReplicatedUserDefaultDataPolicy(  
                        target.getReplicatedUserDefaultDataPolicy() );
                request.withDataPathPort( target.getDataPathPort() );
                request.withDataPathProxy( target.getDataPathProxy() );
                request.withPermitGoingOutOfSync( target.isPermitGoingOutOfSync() );
                m_client.registerDs3TargetSpectraS3( request );
            }
        }.run( Level.WARN );
    }    
    
    
    public DeleteObjectsResult deleteObjects(
            final PreviousVersions previousVersions, 
            final Bucket bucket,
            final Collection< S3Object > objects )
    {
        final Map< UUID, String > names = BeanUtils.toMap( objects, S3Object.NAME ); 
        
        return new OperationRunner<DeleteObjectsResult>( "delete " + objects.size() + " objects in bucket " + bucket.getId() 
                                 + " (" + bucket.getName() + ")" )
        {
            @Override
            protected DeleteObjectsResult performOperation() throws Exception
            {           	
                final DeleteObjectsRequest request;
                final List<Contents> toDelete = new ArrayList<>();
                if ( PreviousVersions.DELETE_ALL_VERSIONS == previousVersions )
                {
                    //send unversioned deletes
                    names.values().forEach( x -> {
                        final Contents deleteObject = new Contents();
                        deleteObject.setKey(x);
                        toDelete.add(deleteObject);
                    });
                    request = new DeleteObjectsRequest( bucket.getName(), toDelete );
                }
                else
                {
                    //specify version ID's
                    objects.forEach( object -> {
                        final Contents deleteObject = new Contents();
                        deleteObject.setKey(object.getName());
                        deleteObject.setVersionId(object.getId());
                        toDelete.add(deleteObject);
                    });
                    request = new DeleteObjectsRequest( bucket.getName(), toDelete );
                    if ( PreviousVersions.UNMARK_LATEST == previousVersions )
                    {
                        //this header allows us to specify which versions we are "unmarking latest"
                        request.getHeaders().put( S3HeaderType.STRICT_UNVERSIONED_DELETE.getHttpHeaderName(), "" );
                    }
                }
                final DeleteObjectsResponse result = m_client.deleteObjects( request );
                                
                final List< DeleteObjectFailure > failures = new ArrayList<DeleteObjectFailure>();
                for ( DeleteObjectError error : result.getDeleteResult().getErrors())
                {
                    final DeleteObjectFailure failure = BeanFactory.newBean( DeleteObjectFailure.class );;
                    if ( null != error.getVersionId() )
                    {
                        failure.setObjectId( error.getVersionId() );
                        failure.setReason(DeleteObjectFailureReason.valueOf( error.getCode() ) );
                    }
                    else
                    {
                        for (final Contents deleteObject: toDelete) {
                            if (deleteObject.getKey().equals(error.getKey())) {
                                failure.setObjectId(deleteObject.getVersionId());
                                break;
                            }
                        }
                        failure.setReason(DeleteObjectFailureReason.valueOf( error.getCode() ) );
                    }
                    failures.add( failure );
                }
                final DeleteObjectsResult retval = BeanFactory.newBean( DeleteObjectsResult.class );
                retval.setFailures( CollectionFactory.toArray( DeleteObjectFailure.class, failures ) );
                
                return retval;
            }
        }.run( Level.INFO );
    }
    
    
    public void undeleteObject( final S3Object object, final String bucketName )
    {
        new VoidOperationRunner( "undelete object " + object.getBucketId() + " " + object.getId() )
        {
            @Override protected void performVoidOperation() throws Exception
            {
                
                final UndeleteObjectSpectraS3Request request = new UndeleteObjectSpectraS3Request( bucketName,
                        object.getName() ).withVersionId( object.getId() );
                m_client.undeleteObjectSpectraS3( request );
            }
        }.run( Level.INFO );
    }
    
    
    public void deleteBucket( final String bucketName, final boolean deleteObjects )
    {
        final String suffix = ( deleteObjects ) ? " and all of its objects" : " if empty";
        new VoidOperationRunner( "delete bucket " + bucketName + suffix )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final DeleteBucketSpectraS3Request request = 
                        new DeleteBucketSpectraS3Request( bucketName );
                request.withForce( deleteObjects );
                m_client.deleteBucketSpectraS3( request );
            }
        }.run( Level.INFO );
    }
    
    
    public boolean isBucketExistant( final String bucketName )
    {
        return new ExistsOperationRunner( "bucket", bucketName )
        {
            @Override
            protected void performGet() throws Exception
            {
                final GetBucketSpectraS3Request request =
                        new GetBucketSpectraS3Request( bucketName );
                m_client.getBucketSpectraS3( request );
            }
        }.run( Level.INFO ).booleanValue();
    }
    
    
    public boolean isJobExistant( final UUID jobId )
    {
        return new ExistsOperationRunner( "job", jobId )
        {
            @Override
            protected void performGet() throws Exception
            {
                final GetActiveJobSpectraS3Request request = new GetActiveJobSpectraS3Request( jobId );
                m_client.getActiveJobSpectraS3( request );
            }
        }.run( Level.INFO ).booleanValue();
    }
    
    
    public boolean isChunkAllocated( final UUID jobId, final UUID chunkId )
    {
        return new OperationRunner< Boolean >( "check if chunk " + chunkId + " is allocated" )
        {
            @Override
            protected Boolean performOperation() throws Exception
            {
                final GetJobChunksReadyForClientProcessingSpectraS3Request request =
                        new GetJobChunksReadyForClientProcessingSpectraS3Request( jobId );
                request.withJobChunk( chunkId );
                request.withPreferredNumberOfChunks( PREFERRED_WORK_WINDOW_NUMBER_OF_CHUNKS );
                final GetJobChunksReadyForClientProcessingSpectraS3Response response =
                        m_client.getJobChunksReadyForClientProcessingSpectraS3( request );
                for ( final Objects objects : response.getMasterObjectListResult().getObjects() )
                {
                    if ( objects.getChunkId().equals( chunkId ) )
                    {
                        return Boolean.TRUE;
                    }
                }
                return Boolean.FALSE;
            }
        }.run( Level.INFO ).booleanValue();
    }


    public List<UUID> getBlobsReady( final UUID jobId )
    {
        return new OperationRunner<List< UUID >>( "check what blobs from " + jobId + " are ready" )
        {
            @Override
            protected List< UUID > performOperation() throws Exception
            {
                final GetJobChunksReadyForClientProcessingSpectraS3Request request =
                        new GetJobChunksReadyForClientProcessingSpectraS3Request( jobId );
                request.withPreferredNumberOfChunks( PREFERRED_WORK_WINDOW_NUMBER_OF_CHUNKS );
                final GetJobChunksReadyForClientProcessingSpectraS3Response response =
                        m_client.getJobChunksReadyForClientProcessingSpectraS3( request );
                return response.getMasterObjectListResult().getObjects().stream()
                        .flatMap(o -> o.getObjects().stream().map(BulkObject::getId)).collect(Collectors.toList());
            }
        }.run( Level.INFO );
    }


    public Boolean getChunkReadyToRead( final UUID chunkId )
    {
        final GetJobChunkDaoSpectraS3Response [] response = new GetJobChunkDaoSpectraS3Response[ 1 ];
        final ExistsOperationRunner runner = new ExistsOperationRunner( "chunk", chunkId )
        {
            @Override
            protected void performGet() throws Exception
            {
                final GetJobChunkDaoSpectraS3Request request = 
                    new GetJobChunkDaoSpectraS3Request( chunkId.toString() );
                response[ 0 ] = m_client.getJobChunkDaoSpectraS3( request );
            }
        };
        if ( !runner.run( Level.INFO ).booleanValue() )
        {
            return null;
        }
        
        return Boolean.valueOf( response[ 0 ].getJobChunkResult().getBlobStoreState().toString().equals(
                BlobStoreTaskState.COMPLETED.toString() ) );
    }
    
    
    public void createBucket( final UUID bucketId, final String bucketName, final String dataPolicy )
    {
        new VoidOperationRunner( "create bucket " + bucketName )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final PutBucketSpectraS3Request request = new PutBucketSpectraS3Request( bucketName );
                request.withDataPolicyId( dataPolicy );
                request.withId( bucketId );
                m_client.putBucketSpectraS3( request );
            }
        }.run( Level.WARN );
    }

    public UUID createGetJob(final Job job, final Collection< JobEntry > entries, final String bucketName ) {
        final List<Ds3Object> sdkObjects = new ArrayList<>();
        for (final JobEntry entry : entries) {
            sdkObjects.add(new Ds3Object(entry.getBlobId().toString()));
        }

        return new OperationRunner<UUID>("replicate GET job " + job.getId() + " (GET " + entries.size() + " blobs)") {
            @Override
            protected UUID performOperation() throws Exception {
                final GetBulkJobSpectraS3Request request = new GetBulkJobSpectraS3Request(
                        bucketName,
                        sdkObjects);
                request.withPriority(Priority.valueOf(job.getPriority().toString()));
                request.withChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.valueOf(
                                job.getChunkClientProcessingOrderGuarantee().toString()));
                request.withName(GET_JOB_PREFIX + job.getName());
                request.getHeaders().put(
                        S3HeaderType.SPECIFY_BY_ID.getHttpHeaderName(),
                        job.getId().toString());
                return m_client.getBulkJobSpectraS3(request).getMasterObjectList().getJobId();
            }
        }.run(Level.WARN);
    }


    public void verifySafeToCreatePutJob( final String bucketName )
    {
        new VoidOperationRunner( "verify safe to create PUT job for bucket " + bucketName )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final VerifySafeToCreatePutJobSpectraS3Request request =
                        new VerifySafeToCreatePutJobSpectraS3Request( bucketName );
                m_client.verifySafeToCreatePutJobSpectraS3( request );
            }
        }.run( Level.WARN );
    }


    public void replicatePutJob( final DetailedJobToReplicate job, final String bucketName )
    {
        new VoidOperationRunner( "replicate PUT job " + job.getJob().getId() )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final ReplicatePutJobSpectraS3Request request = new ReplicatePutJobSpectraS3Request( 
                        bucketName,
                        job.getJob().toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ) );
                request.withPriority( Priority.valueOf( job.getPriority().toString() ) );
                m_client.replicatePutJobSpectraS3( request );
            }
        }.run( Level.WARN );
    }
    
    
    public void cancelGetJob( final UUID sourceJobId )
    {
        new VoidOperationRunner( "cancel GET job " + sourceJobId )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final CancelJobSpectraS3Request request = new CancelJobSpectraS3Request( sourceJobId );
                m_client.cancelJobSpectraS3( request );
            }
        }.run( Level.INFO );
    }
    
    
    public BlobPersistenceContainer getBlobPersistence( final UUID jobId, final Set< UUID > blobIds )
    {
        final BlobIdsSpecification specBlobIds = BeanFactory.newBean( BlobIdsSpecification.class );
        specBlobIds.setJobId( jobId );
        specBlobIds.setBlobIds( CollectionFactory.toArray( UUID.class, blobIds ) );
        final String requestPayload = 
                specBlobIds.toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE );

        return new OperationRunner< BlobPersistenceContainer >( "get blob persistence" )
        {
            @Override
            protected BlobPersistenceContainer performOperation() throws Exception
            {
                final GetBlobPersistenceSpectraS3Request request =
                        new GetBlobPersistenceSpectraS3Request( requestPayload );
                final String response = m_client.getBlobPersistenceSpectraS3( request ).getStringResult();
                final BlobPersistenceContainer retval = 
                        JsonMarshaler.unmarshal( BlobPersistenceContainer.class, response );
                return retval;
            }
        }.run( Level.WARN );
    }
    
    
    public void getBlob( 
            final UUID jobId,
            final String bucketName,
            final String objectName, 
            final Blob blob,
            final File fileInCache )
    {
        new VoidOperationRunner( "read " + bucketName + "/" + objectName + " (blob " + blob.getId() + ")" )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                if ( !fileInCache.exists() )
                {
                    fileInCache.createNewFile();
                }
                FileOutputStream out = null;
                try
                {
                    out = new FileOutputStream( fileInCache );
                    final GetBlobParams params = new GetBlobParams( blob, out );
                    final GetObjectRequest request = new GetObjectRequest( 
                            bucketName,
                            objectName,
                            Channels.newChannel( params.m_pipedOut ),
                            jobId,
                            blob.getByteOffset() ).withVersionId(blob.getObjectId());
                    
                    SystemWorkPool.getInstance().submit( new GetBlobDataMoverRunner( params ) );
                    SystemWorkPool.getInstance().submit( new GetBlobRequestExecutor( params, request ) );
                    
                    params.m_latch.await();
                    if ( null != params.m_ex )
                    {
                        throw params.m_ex;
                    }
                    
                    final String checksum = Base64.encodeBase64String( params.m_dataMover.getChecksum() );
                    if ( !checksum.equals( blob.getChecksum() ) )
                    {
                        throw new RuntimeException( 
                                "Blob checksum mismatch: expected " + blob.getChecksum() + ", but got "
                                + checksum + "." );
                    }
                }
                catch ( final Exception ex )
                {
                    throw ExceptionUtil.toRuntimeException( ex );
                }
                finally
                {
                    if ( null != out )
                    {
                        out.close();
                    }
                }
            }
        }.run( Level.WARN );
    }
    
    
    private final static class GetBlobParams
    {
        private GetBlobParams( final Blob blob, final FileOutputStream out ) throws IOException
        {
            m_pipedIn = new PipedInputStream( m_pipedOut, HardwareInformationProvider.getTomcatBufferSize() );
            m_dataMover = new ThreadedDataMover( 
                    HardwareInformationProvider.getTomcatBufferSize(), 
                    HardwareInformationProvider.getZfsCacheFilesystemRecordSize(),
                    "Receive blob from DS3 target", 
                    blob.getLength(), 
                    blob.getChecksumType(), 
                    out, 
                    new SingleInputStreamProvider( m_pipedIn ), 
                    null );
        }
        
        void setException( final Exception ex )
        {
            m_ex = ex;
        }
        
        private volatile Exception m_ex = null;
        
        private final ThreadedDataMover m_dataMover;
        private final PipedOutputStream m_pipedOut = new PipedOutputStream();
        private final PipedInputStream m_pipedIn;
        private final String m_originalThreadName = Thread.currentThread().getName();
        private final CountDownLatch m_latch = new CountDownLatch( 1 );
    } // end inner class def
    
    
    private final static class GetBlobDataMoverRunner implements Runnable
    {
        private GetBlobDataMoverRunner( final GetBlobParams params )
        {
            m_params = params;
        }
        
        public void run()
        {
            Thread.currentThread().setName( m_params.m_originalThreadName + "-dataMover" );
            try
            {
                m_params.m_dataMover.run();
            }
            catch ( final Exception ex )
            {
                m_params.setException( ex );
            }
            finally
            {
                m_params.m_latch.countDown();
            }
        }
        
        private final GetBlobParams m_params;
    } // end inner class def
    
    
    private final class GetBlobRequestExecutor implements Runnable
    {
        private GetBlobRequestExecutor( final GetBlobParams params, final GetObjectRequest request )
        {
            m_params = params;
            m_request = request;
        }
        
        public void run()
        {
            Thread.currentThread().setName( m_params.m_originalThreadName + "-requestor" );
            try
            {
                m_client.getObject( m_request );
                m_params.m_pipedOut.flush();
                m_params.m_pipedOut.close();
            }
            catch ( final Exception ex )
            {
                m_params.setException( ex );
                m_params.m_latch.countDown();
            }
        }
        
        private final GetBlobParams m_params;
        private final GetObjectRequest m_request;
    } // end inner class def
    
    
    public void putBlob( 
            final UUID jobId,
            final String bucketName,
            final String objectName, 
            final Blob blob,
            final File fileInCache,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata )
    {
        new VoidOperationRunner( "write " + bucketName + "/" + objectName + " (blob " + blob.getId() + ")" )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final SeekableByteChannel channel = 
                        FileChannel.open( fileInCache.toPath(), StandardOpenOption.READ );
                try
                {
                    TargetLogger.LOG.info( 
                            "Will write blob " + blob.getId() + " (" + blob + ") with metadata keys: " 
                            + BeanUtils.extractPropertyValues( metadata, KeyValueObservable.KEY ) );
                    final PutObjectRequest request = new PutObjectRequest( 
                            bucketName,
                            objectName,
                            channel,
                            jobId,
                            blob.getByteOffset(),
                            blob.getLength() );
                    request.withChecksum(
                            ChecksumType.value( blob.getChecksum() ), 
                            Type.valueOf( blob.getChecksumType().toString() ) );
                    request.withJob( jobId );
                    request.getHeaders().put(
                            S3HeaderType.JOB_CHUNK_LOCK_HOLDER.getHttpHeaderName(), 
                            m_sourceInstanceId.toString() );
                    if ( null != objectCreationDate )
                    {
                        request.getHeaders().put(
                                S3HeaderType.OBJECT_CREATION_DATE.getHttpHeaderName(),
                                String.valueOf( objectCreationDate.getTime() ) );
                    }
                    for ( final S3ObjectProperty property : metadata )
                    {
                        request.getHeaders().put( property.getKey(), property.getValue() );
                    }
                    
                    m_client.putObject( request );
                }
                finally
                {
                    channel.close();
                }
            }
        }.run( Level.WARN );
    }
    
    
    public void keepJobAlive( final UUID jobId )
    {
        new VoidOperationRunner( "keep job " + jobId + " alive" )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                final ModifyJobSpectraS3Request request = new ModifyJobSpectraS3Request( jobId );
                m_client.modifyJobSpectraS3( request );
            }
        }.run( Level.INFO );
    }
    
    
    private abstract class ExistsOperationRunner extends OperationRunner< Boolean >
    {
        protected ExistsOperationRunner( 
                final String daoTypeDescription, 
                final Object id )
        {
            super( "check if " + daoTypeDescription + " " + id + " exists" );
        }
        
        
        @Override
        protected Boolean performOperation() throws Exception
        {
            try
            {
                performGet();
                return Boolean.TRUE;
            }
            catch ( final Exception ex )
            {
                Throwable t = ex;
                while ( null != t )
                {
                    if ( FailedRequestException.class.isAssignableFrom( t.getClass() ) )
                    {
                        final FailedRequestException ct = (FailedRequestException)t;
                        if ( 404 == ct.getStatusCode() )
                        {
                            return Boolean.FALSE;
                        }
                    }
                    t = t.getCause();
                }
                throw ex;
            }
        }
        
        
        protected abstract void performGet() throws Exception;
    } // end inner class def
    
    
    private abstract class VoidOperationRunner extends OperationRunner< Object >
    {
        protected VoidOperationRunner( final String operationDescription )
        {
            super( operationDescription );
        }
        
        
        @Override
        protected Object performOperation() throws Exception
        {
            performVoidOperation();
            return null;
        }
        

        protected abstract void performVoidOperation() throws Exception;
    } // end inner class def
    
    
    private abstract class OperationRunner< T >
    {
        protected OperationRunner( final String operationDescription )
        {
            m_operationDescription = operationDescription;
            Validations.verifyNotNull( "Operation description", m_operationDescription );
        }
        
        
        final public T run( final Level failureLogLevel )
        {
            final Duration duration = new Duration();
            TargetLogger.LOG.info( "On " + m_endPoint + ", attempting to " + m_operationDescription + "..." );
            try
            {
                final T retval = performOperation();
                final String msg = 
                        "On " + m_endPoint + ", succeeded to " + m_operationDescription + " " 
                        + getRequestId() + " in " + duration + ".";
                LOG.info( ( null == retval || 50 > retval.toString().length() ) ? 
                        msg + "  Result: " + retval 
                        : msg );
                TargetLogger.LOG.info( msg + "  Result:" + Platform.NEWLINE + ( ( null == retval ) ?
                        "null" 
                        : LogUtil.getShortVersion( retval.toString() ) ) );
                return retval;
            }
            catch ( final Exception ex )
            {
                log( failureLogLevel,
                     "On " + m_endPoint + ", failed to " + m_operationDescription + " " + getRequestId()
                     + " after " 
                     + duration + ".", ex );
                throw getFailure( m_operationDescription, ex );
            }
        }
        
        
        private String getRequestId()
        {
            return "<DS3-" + ReplicationSourceIdentifierHeaderAddingDs3ClientFactory.getLastRequestNumber() 
                   + ">";
        }
        
        
        protected abstract T performOperation() throws Exception;
        
        
        private final String m_operationDescription;
    } // end inner class def
    
    
    private FailureTypeObservableException getFailure( final String operationDescription, final Exception ex )
    {
        Throwable t = ex;
        while ( null != t )
        {
            if ( FailedRequestUsingMgmtPortException.class.isAssignableFrom( t.getClass() ) )
            {
                throw new FailureTypeObservableException( 
                        GenericFailure.BAD_REQUEST,
                        "You must connect to the data path and not the management path.  " + m_endPoint 
                        + " is the management path." );
            }
            if ( FailedRequestException.class.isAssignableFrom( t.getClass() ) )
            {
                break;
            }
            t = t.getCause();
        }
        if ( null == t )
        {
            t = ex;
        }
        
        final Ds3SdkFailure sdkFailure = ( FailedRequestException.class.isAssignableFrom( t.getClass() ) ) ?
                Ds3SdkFailure.valueOf( ( (FailedRequestException)t ).getStatusCode() )
                : null;
        final String suffix = ( null == sdkFailure || 403 != sdkFailure.getHttpResponseCode() ) ?
                ""
                : "either jobs are being blocked on the target or there was an authorization problem with id "
                  + m_authorizationId
                  + " (for user " + new String( Base64.decodeBase64( m_authorizationId ) ) + ") on "
                  + m_endPoint
                  + " either (i) does not exist; (ii) has a different secret key" 
                  + "; or (iii) does not have permission to perform the requested operation: ";
        return new FailureTypeObservableException(
                ( null != sdkFailure ) ? sdkFailure : GenericFailure.CONFLICT,
                "Failed to " + operationDescription + " on " + m_endPoint + " since " + suffix
                + ExceptionUtil.getReadableMessage( ex ) + ".", 
                ex );
    }
    
    
    private void log( final Level level, final String message, final Throwable t )
    {
        if ( Level.INFO == level )
        {
            LOG.log( level, message );
        }
        else
        {
            LOG.log( level, message, t );
        }
        TargetLogger.LOG.log( level, message, t );
    }
    
    
    private final UUID m_sourceInstanceId;
    private final Ds3Client m_client;
    private final String m_endPoint;
    private final String m_authorizationId;
    
    private final static int MAX_CLOCK_SKEW_IN_MINS = 15;
    private final static Logger LOG = Logger.getLogger( Ds3ConnectionImpl.class );
}
