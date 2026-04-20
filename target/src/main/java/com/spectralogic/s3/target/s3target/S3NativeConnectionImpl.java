package com.spectralogic.s3.target.s3target;

import java.nio.file.Files;
import java.nio.file.Path;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
import com.spectralogic.s3.target.frmwrk.*;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.BlobReadFailedException;
import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.core.exception.SdkException;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;




import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.RetryableException;


import java.io.*;
import java.lang.reflect.Array;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import javax.servlet.http.HttpServletResponse;


import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.*;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.target.Md5ComputingFileInputStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.S3Connection;
import com.spectralogic.s3.common.rpc.target.S3ObjectOnCloudInfo;
import com.spectralogic.s3.target.TargetLogger;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.HttpUtil;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.security.FastMD5;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

import ds3fatjar.org.apache.commons.codec.binary.Hex;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;



public class S3NativeConnectionImpl extends BaseShutdownable
	implements S3Connection, TestablePublicCloudConnection
{
	S3NativeConnectionImpl( final S3Target target, final UploadTracker uploadTracker, BeansServiceManager serviceManager )
    {
        doNotLogWhenShutdown();
        initWorkPool();
        final Duration duration = new Duration();
        final S3ClientBuilder builder = S3Client.builder();

        m_verifyConnectivity = !target.isRestrictedAccess();
        m_serviceManager = serviceManager;
        m_target = target;
        try
        {
            final AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(
                    target.getAccessKey(),
                    target.getSecretKey()
            );

            builder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));

            final String rawEndpoint = target.getDataPathEndPoint();
            final String formattedEndpoint = HttpUtil.formatForIpV6(rawEndpoint);
            final boolean isCustomEndpoint = formattedEndpoint != null && !formattedEndpoint.isEmpty();

            if (isCustomEndpoint) {
                // LOCALSTACK or S3-Compatible Storage
                builder.endpointOverride(handleEndpointConfiguration(target,formattedEndpoint));
                m_endPoint = formattedEndpoint;
            } else {
                // REAL AWS CLOUD (Automatic endpoint resolution via Region)
                m_endPoint = "AWS-S3 region " + target.getRegion();
            }
            S3Configuration.Builder s3ConfigBuilder = S3Configuration.builder()
                    .checksumValidationEnabled(false);

            s3ConfigBuilder.pathStyleAccessEnabled(isCustomEndpoint);
            
            if (target.getProxyHost() != null && target.getProxyPort() != null) {
                ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

                ProxyConfiguration.Builder proxyConfigBuilder =
                        ProxyConfiguration.builder()
                                .endpoint(URI.create("http://" + target.getProxyHost() + ":" + target.getProxyPort()));


                if (target.getProxyUsername() != null && target.getProxyPassword() != null) {
                    proxyConfigBuilder.username(target.getProxyUsername())
                            .password(target.getProxyPassword());
                }

                httpClientBuilder.proxyConfiguration(proxyConfigBuilder.build());
                builder.httpClient(httpClientBuilder.build());
            }
            Region region = resolveRegion(target);
            builder.region(region);

            final String version = ConfigurationInformationProvider.getInstance()
                    .getBuildInformation()
                    .getVersion();
            if (version != null) {
                builder.overrideConfiguration(config -> config.putHeader("User-Agent", USER_AGENT_PREFIX + version));
            } else {
                TargetLogger.LOG.warn("Unable to retrieve version info. Will not set user agent header data.");
            }
            builder.serviceConfiguration(s3ConfigBuilder.build());
            m_client = builder.build();

            S3AsyncClientBuilder asyncBuilder = S3AsyncClient.builder()
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials));

            asyncBuilder.serviceConfiguration(s3ConfigBuilder.build());

            if (isCustomEndpoint) {
                asyncBuilder.endpointOverride(handleEndpointConfiguration(target,formattedEndpoint));
            }



            S3AsyncClient asyncClient = asyncBuilder.build();
            m_txManager = S3TransferManager.builder()
                    .s3Client(asyncClient)
                    .build();

            m_uploadTracker = uploadTracker;

        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to initialize SDK connection.", ex );
        }

        try
        {
            if ( m_verifyConnectivity ) {
                verifyConnectivity();
            }
        }
        catch ( final RuntimeException ex )
        {
            shutdown();
            throw ex;
        }

        addShutdownListener( new S3NativeConnectionImpl.MyShutdownListener() );
        TargetLogger.LOG.info( "Connected to " + m_endPoint + " in " + duration + "." );
    }


    private Region resolveRegion(S3Target target) {
        Region region;
        if (target.getRegion() != null) {
            try {
                region = Region.of(target.getRegion().toString().toLowerCase().replace('_', '-'));
            } catch (IllegalArgumentException e) {
                TargetLogger.LOG.warn("Invalid region: " + target.getRegion() + ". Defaulting to US_EAST_1");
                region = Region.US_EAST_1;
            }
        } else {
            region = Region.US_EAST_1;
        }
        return region;

    }
    private final class MyShutdownListener extends CriticalShutdownListener
    {
        @Override
        public void shutdownOccurred()
        {
            m_txManager.close();
            TargetLogger.LOG.info( "Closed connection to " + m_endPoint + "." );
        }
    } // end inner class def

    
    @Override
	public void verifyConnectivity()
    {
        new VoidOperationRunner( "verify connectivity" )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                ListBucketsResponse response = m_client.listBuckets(ListBucketsRequest.builder().build());
                response.owner();
            }
        }.run( Level.WARN );
    }
    

    @Override
	public PublicCloudBucketInformation getExistingBucketInformation( final String bucketName )
    {
        return new OperationRunner< PublicCloudBucketInformation >( "discover bucket " + bucketName )
        {
            @Override
            protected PublicCloudBucketInformation performOperation() throws Exception
            {
                if (!bucketExists(bucketName)) {
                    return null;
                }

                try {
                    m_client.headObject(HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(SPECTRA_BUCKET_META_KEY)
                            .build());

                    final HeadObjectResponse objectMeta = m_client.headObject(HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(SPECTRA_BUCKET_META_KEY)
                            .build());

                    return MarshalUtil.newBean(
                            PublicCloudBucketInformation.class,
                            objectMeta.metadata()
                    );

                } catch (S3Exception e) {
                    if (e.statusCode() != 404) {
                        throw e;
                    }
                }

                return BeanFactory.newBean( PublicCloudBucketInformation.class ).setName( bucketName );
            }
        }.run( Level.WARN );
    }

    private boolean bucketExists(String bucketName) {
        try {
            m_client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            return true; // Bucket exists
        } catch (S3Exception e) {
            // If the exception's status code is 404, the bucket does not exist
            if (e.statusCode() == 404) {
                return false; // Bucket does not exist
            }
            // Re-throw for other exceptions
            throw e;
        }
    }


    @Override
	public PublicCloudBucketInformation createOrTakeoverBucket(
            final Object initialDataPlacement,
            final PublicCloudBucketInformation cloudBucket )
    {
        return new OperationRunner< PublicCloudBucketInformation >( "create bucket " + cloudBucket.getName() )
        {
            @Override
            protected PublicCloudBucketInformation performOperation() throws Exception
            {
                if ( !bucketExists(cloudBucket.getName()))
                {
                    m_client.createBucket(CreateBucketRequest.builder()
                            .bucket(cloudBucket.getName())
                            .build());

                } else {
                    LOG.info( "Bucket '" + cloudBucket.getName()
                            + "' already exists. Will attempt to take ownership." );
                }
                final PublicCloudBucketInformation bucketInfo =
                        getExistingBucketInformation( cloudBucket.getName() );
                if ( null == bucketInfo.getOwnerId() )
                {
                    takeOwnershipInternal( cloudBucket );
                }
                else if ( bucketInfo.getOwnerId().equals( cloudBucket.getOwnerId() ) )
                {
                    throw new RuntimeException( "Bucket " + cloudBucket.getName() + " was already created on cloud "
                            + " and is already owned by you." );
                }
                else
                {
                    throw new RuntimeException( "Bucket " + cloudBucket.getName() + " already exists on cloud "
                            + " and is owned by another Black Pearl (" + bucketInfo.getOwnerId() + ").");
                }
                return getExistingBucketInformation( cloudBucket.getName() );
            }
        }.run( Level.WARN );
    }
    
    
    @Override
	public PublicCloudBucketInformation takeOwnership( 
            final PublicCloudBucketInformation cloudBucket,
            final UUID newOwnerId )
    {
        return new OperationRunner< PublicCloudBucketInformation >( 
                "take ownership over bucket " + cloudBucket.getName() )
        {
            @Override
            protected PublicCloudBucketInformation performOperation() throws Exception
            {
                cloudBucket.setOwnerId( newOwnerId );
                takeOwnershipInternal( cloudBucket );
                return getExistingBucketInformation( cloudBucket.getName() );
            }
        }.run( Level.WARN );
    }
    
    
    private void takeOwnershipInternal( final PublicCloudBucketInformation cloudBucket ) throws Exception
    {
        Map<String, String> metadata = new HashMap<>();
        for (final Map.Entry<String, String> entry : toBeanPropsMap(cloudBucket).entrySet()) {
            metadata.put(entry.getKey(), entry.getValue());
        }
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(cloudBucket.getName())
                .key(SPECTRA_BUCKET_META_KEY)
                .metadata(metadata)
                .build();
        m_client.putObject(request, RequestBody.empty());
    }


    protected boolean isObjectReadyToBeReadFromCloud(
            final String cloudBucketName,
            final String objectName) throws Exception
    {
        HeadObjectResponse metadata = m_client.headObject(HeadObjectRequest.builder()
                    .bucket(cloudBucketName)
                    .key(objectName)
                    .build());

        final String storageClass = metadata.storageClassAsString();
        final boolean offlineStorageClass =
                "GLACIER".equalsIgnoreCase(storageClass) ||
                        "DEEP_ARCHIVE".equalsIgnoreCase(storageClass);
        final String restoreStatus = metadata.restore();
        final boolean restoreCompleted =
                restoreStatus != null && restoreStatus.contains("ongoing-request=\"false\"");
        return (!offlineStorageClass || restoreCompleted);
    }


    protected void beginStagingObjectToRead( 
            final String cloudBucketName,
            final String objectName,
            final int stagedDataExpirationInDays )
    {
        final RestoreRequest restoreRequestParams = RestoreRequest.builder()
                .days(stagedDataExpirationInDays)
                .glacierJobParameters(p -> p.tier("Standard"))
                .build();

        final RestoreObjectRequest request = RestoreObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(objectName)
                .restoreRequest(restoreRequestParams)
                .build();
        try
        {
            m_client.restoreObject(request);
        }
        catch ( final S3Exception ex )
        {
            final String errorCode = ex.awsErrorDetails().errorCode();

            if ("RestoreAlreadyInProgress".equalsIgnoreCase(errorCode)) {
                LOG.info("Restore already in progress for " + objectName + ".");
            }
            else if ("InvalidObjectState".equalsIgnoreCase(errorCode)) {
                LOG.info("Restore already completed for " + objectName + ".", ex);
            }
            else {
                throw ex;
            }
        }
    }
    
    
    private void verifyNoCorruption( 
            final FastMD5 calculatedDigest,
            final long transferSize, 
            final String digestOnCloud,
            final long sizeOnCloud,
            final Blob blob )
    {
    	final String calculatedMd5 = Hex.encodeHexString( calculatedDigest.digestAndReset() );
    	final String blobMD5; 
    	if ( ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED.equals( blob.getChecksum() ) )
    	{
    		blobMD5 = ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED;
    	}
    	else
    	{
    		blobMD5 = Hex.encodeHexString( Base64.decodeBase64( blob.getChecksum() ) );
    	}
    	final String cleanDigestOnCloud = digestOnCloud != null ? digestOnCloud.replace("\"", "") : digestOnCloud;
    	final boolean singleUploadDigest = cleanDigestOnCloud != null && cleanDigestOnCloud.length() == 32;

    	if ( transferSize == sizeOnCloud )
    	{
	    	if ( singleUploadDigest )
	    	{
		        if ( !calculatedMd5.equals( cleanDigestOnCloud ) )
		        {
		            throw new RuntimeException(
		                    "Corruption occurred during transmission of blob " + blob.getId() +
		                    ". Calculated an MD5 checksum of " + calculatedMd5 +
		                    ", but checksum on cloud is " + cleanDigestOnCloud + "." );
		        }
	    	}
	    	else
	    	{
	    		LOG.debug( "Cannot verify calculated checksum " + calculatedMd5 + " against multipart checksum "
	    				+ cleanDigestOnCloud + ".");
	    	}
    	}
    	else
    	{
    		LOG.debug( "Cannot verify calculated checksum for " + transferSize + "b of transferred data against "
    				+ " cloud checksum for " + sizeOnCloud + " bytes of data." );	
    	}

        if ( sizeOnCloud == blob.getLength() )
        {
        	if ( singleUploadDigest )
        	{
	        	if ( ChecksumType.MD5.equals( blob.getChecksumType() ) )
	        	{
	        		if ( !ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED.equals( blobMD5 )
	        				&& !blobMD5.equals( cleanDigestOnCloud ) )
		            {
                        markBlobAsSuspect( blob.getId());
		                throw new RuntimeException(
		                        "Possible data corruption on blob " + blob.getId() +
		                        " detected. Cloud reports checksum as " + cleanDigestOnCloud
		                        + ", but database reports a checksum of " + blobMD5 + "." );
		            }
	        	}
	        	else
	        	{
	        		LOG.debug( "Unable to verify local checksum against cloud since local checksum type is not MD5." );
	        	}
        	}
        	else
        	{
        		LOG.debug( "Cannot verify blob checksum " + blobMD5 + " against multipart checksum "
        				+ cleanDigestOnCloud + ".");	
        	}
        }
        else
        {
        	LOG.debug( "Unable to verify local checksum against cloud since part or object does not represent a "
        			+ "single complete blob." );
        }
        
        
        if ( blob.getLength() == transferSize )
        {
        	if ( ChecksumType.MD5.equals( blob.getChecksumType() ) )
        	{
        		if ( !ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED.equals( blobMD5 )
        				&& !blobMD5.equals( calculatedMd5 ) )
	            {
                    markBlobAsSuspect( blob.getId());
	                throw new RuntimeException(
	                        "Possible data corruption of blob " + blob.getId() +
	                        " detected. Calculated a checksum of " + calculatedMd5
	                        + ", but database reports a checksum of " + blobMD5 + "." );
	            }
        	}
        	else
        	{
        		LOG.debug( "Unable to verify local checksum against calculated since local checksum type is not MD5." );
        	}
        }
        else
        {
        	LOG.debug( "Unable to verify local checksum against calculated since transfer was not an entire blob." );
        }
    }
    
    
    protected String writePartToCloud( 
            final String cloudBucketName,
            final S3Object object,
            final String uploadId,
            final long objectSize,
            final Blob blob,
            final long offset,
            final InputStream inputStream, 
            final long length ) throws CloudTransferFailedException
    {
        Map<String, String> userMetadata = toBeanPropsMap(blob);

        final int partNumber = (int) (1 + (offset * MAX_PART_NUMBERS) / (objectSize));

        UploadPartRequest.Builder requestBuilder = UploadPartRequest.builder()
                .bucket(cloudBucketName)
                .key(object.getName())    // Keep using getName() as it's the correct method
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength(length);

        if (length == blob.getLength() && ChecksumType.MD5 == blob.getChecksumType()) {
            requestBuilder.contentMD5(blob.getChecksum());
        }



        S3Exception lastTimeoutException = null;
        int retry = 30;
        final int RETRY_DELAY_MS = 500;


        while (retry > 0) {
            try {
                RequestBody requestBody = RequestBody.fromInputStream(inputStream, length);
                UploadPartResponse response = m_client.uploadPart(requestBuilder.build(), requestBody);
                return response.eTag();
            } catch (S3Exception e) {
                if (e.statusCode() == HttpServletResponse.SC_SERVICE_UNAVAILABLE) {
                    lastTimeoutException = e;
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (final InterruptedException ie) {
                        LOG.warn("Interrupted while sleeping waiting to retry request");
                        Thread.currentThread().interrupt();
                    }
                } else if (e.statusCode() == 404) {
                    throw new RuntimeException("Bucket or upload ID not found", e);
                } else {
                    throw new CloudTransferFailedException(e);
                }
            } catch (Exception e) {
                throw new CloudTransferFailedException("Unexpected error during upload", e);
            }
            retry--;
        }
        throw new CloudTransferFailedException("Max retries exceeded after " + 30 + " attempts", lastTimeoutException);



    }
    
    
    public void syncUploads( final String cloudBucketName )
    {
    	m_uploadTracker.syncUploads( cloudBucketName, m_client );
    }


    protected void deleteKeys( final String cloudBucketName, final Set< String > cloudKeys )
    {
        final Set< String > segment = new HashSet<>();
        for ( final String key : cloudKeys )
        {
            if ( 1000 == segment.size() )
            {
                deleteKeys( cloudBucketName, segment );
                segment.clear();
            }

            segment.add( key );
        }

        if (!segment.isEmpty()) {
            deleteKeysV2(cloudBucketName, segment);
        }
    }

    private void deleteKeysV2(final String cloudBucketName, final Set<String> segment) {

        final Delete deleteObjectList = Delete.builder()
                .objects(segment.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .collect(Collectors.toList()))
                .build();

        try {
            final DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(cloudBucketName)
                    .delete(deleteObjectList)
                    .build();

            final DeleteObjectsResponse response = m_client.deleteObjects(request);

            final int numDeletes = response.deleted().size();
            TargetLogger.LOG.info("Deleted " + numDeletes + " keys.");

        }
        catch (final S3Exception ex) {
            TargetLogger.LOG.info("Failed to delete " + segment.size() + " keys at once.", ex);

            for (final String key : segment) {
                try {
                    m_client.deleteObject(r -> r.bucket(cloudBucketName).key(key));
                    TargetLogger.LOG.info("Deleted key: " + key);
                }
                catch (final Exception ex2) {
                    TargetLogger.LOG.info("Failed to delete key: " + key, ex2);
                }
            }
        }
    }
    @Override
	public void deleteBucket( final String cloudBucketName )
    {
        new VoidOperationRunner( "delete bucket " + cloudBucketName )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                emptyBucketContents(cloudBucketName);
                m_client.deleteBucket(r -> r.bucket(cloudBucketName));
            }
        }.run( Level.WARN );
    }

    private void emptyBucketContents(final String cloudBucketName) {
        final ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(cloudBucketName)
                .maxKeys(1000) // Ensure max list size matches max delete size
                .build();

        final ListObjectsV2Iterable listObjects = m_client.listObjectsV2Paginator(listRequest);

        listObjects.stream().forEach(response -> {

            final List<ObjectIdentifier> keysToDelete = response.contents().stream()
                    .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                    .collect(Collectors.toList());

            if (!keysToDelete.isEmpty()) {
                TargetLogger.LOG.info(
                        "Deleting " + keysToDelete.size() + " objects in bucket " + cloudBucketName + "...");

                final DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                        .bucket(cloudBucketName)
                        .delete(Delete.builder().objects(keysToDelete).build())
                        .build();

                try {
                    m_client.deleteObjects(deleteRequest);
                } catch (final S3Exception ex) {
                    TargetLogger.LOG.warn("Failed to delete batch of keys in " + cloudBucketName, ex);
                }
            }
        });
    }

    protected FailureTypeObservableException getFailure(
            final String operationDescription,
            final Exception ex )
    {
        Throwable t = ex;
        while (null != t) {
            if (software.amazon.awssdk.services.s3.model.S3Exception.class.isAssignableFrom(t.getClass())) {
                break;
            }
            t = t.getCause();
        }
        if (null == t) {
            t = ex;
        }

        final S3SdkFailure sdkFailure = (software.amazon.awssdk.services.s3.model.S3Exception.class.isAssignableFrom(t.getClass())) ?
                S3SdkFailure.valueOf(
                        ((software.amazon.awssdk.services.s3.model.S3Exception) t).awsErrorDetails().errorCode(),
                        ((software.amazon.awssdk.services.s3.model.S3Exception) t).statusCode()
                ) : null;

        return new FailureTypeObservableException(
                (null != sdkFailure) ? sdkFailure : GenericFailure.CONFLICT,
                "Failed to " + operationDescription + " on " + m_endPoint + " since "
                        + ExceptionUtil.getReadableMessage(ex) + ".",
                ex);

    }


    @Override
	public void createGenericBucket( final String name )
    {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(name)
                .build();

        m_client.createBucket(request);

    }
    
    
    @Override
    public void deleteOldBuckets( final long maxAgeInMillis, final String bucketNamePrefix )
    {
        final ListBucketsResponse response = m_client.listBuckets();
        final List<Bucket> cloudBuckets = response.buckets();

        final Instant currentTime = Instant.now();

        for (Bucket b : cloudBuckets) {
            final Instant creationInstant = b.creationDate();

            if (creationInstant != null) {
                final long cloudBucketAgeMillis = ChronoUnit.MILLIS.between(creationInstant, currentTime);

                if (b.name().startsWith(bucketNamePrefix) && cloudBucketAgeMillis >= maxAgeInMillis) {
                    deleteBucket(b.name());
                }
            }
        }
    }

    
    
    final protected void setEndPoint( final String endPoint )
    {
        Validations.verifyNotNull( "End point", endPoint );
        m_endPoint = endPoint;
    }

    final public boolean isBlobAvailableOnCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob )
    {
        return new OperationRunner< Boolean >( "determine if blob " + blob.getId() + " is online" )
        {
            @Override
            public Boolean performOperation()
            {
                try
                {
                    m_client.headObject(HeadObjectRequest.builder()
                            .bucket(cloudBucket.getName())
                            .key(object.getName())
                            .build());
                    return Boolean.TRUE;
                }
                catch ( final Exception ex )
                {
                    SdkServiceException s3ServiceException = null;
                    if (ex instanceof SdkServiceException) {
                        s3ServiceException = (SdkServiceException) ex;
                    }
                    if (s3ServiceException != null && s3ServiceException.statusCode() == 404) {
                        // Now check if it's specifically a NoSuchKeyException
                        LOG.warn("Blob part not found (404 NoSuchKey) in S3 for blob " );
                        markBlobAsSuspect( blob.getId());
                        return Boolean.FALSE; // The blob is not ready if a part is missing
                    }

                    throw new RuntimeException( ex );
                }
            }
        }.run( Level.WARN ).booleanValue();
    }
    @Override
	final public boolean isBlobReadyToBeReadFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob )
    {
        return new OperationRunner< Boolean >( "determine if blob " + blob.getId() + " is online" )
        {
            @Override
            public Boolean performOperation()
            {
                try
                {
                    return isObjectReadyToBeReadFromCloud( cloudBucket.getName(), object.getName() );
                }
                catch ( final Exception ex )
                {
                    software.amazon.awssdk.core.exception.SdkServiceException s3ServiceException = null;
                    if (ex instanceof software.amazon.awssdk.core.exception.SdkServiceException) {
                        s3ServiceException = (software.amazon.awssdk.core.exception.SdkServiceException) ex;
                    }

                    if (s3ServiceException != null && s3ServiceException.statusCode() == 404) {
                        // Now check if it's specifically a NoSuchKeyException
                        LOG.warn("Blob part not found (404 NoSuchKey) in S3 for blob " );
                        return Boolean.FALSE; // The blob is not ready if a part is missing
                    }
                    
                    throw new RuntimeException( ex );
                }
            }
        }.run( Level.WARN ).booleanValue();
    }



    @Override
	public void beginStagingToRead( 
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object, 
            final Blob blob,
            final int stagedDataExpirationInDays )
    {
        new VoidOperationRunner( 
                "begin staging blob " + blob.getId() 
                + " (" + new BytesRenderer().render( blob.getLength() ) + ")" )
        {
            @Override
            public void performVoidOperation() throws Exception
            {
                beginStagingObjectToRead( 
                        cloudBucket.getName(), object.getName(), stagedDataExpirationInDays );
            }
        }.run( Level.WARN );
    }



    @Override
    final public List<Future<?>> readBlobFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final File fileInCache) {

        // 1. Define the GetObjectRequest (V2 builder pattern)
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(cloudBucket.getName())
                .key(object.getName())
                .range(String.format("bytes=%d-%d",
                        blob.getByteOffset(),
                        blob.getByteOffset() + blob.getLength() - 1))
                .build();

        // 2. Define the DownloadFileRequest
        final DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .getObjectRequest(getObjectRequest)
                // Specify the destination path
                .destination(fileInCache.toPath())
                .build();

        // 3. Execute the asynchronous download using the Transfer Manager
        // The Transfer Manager runs this on its internal thread pool, replacing the need for s_wp.submit()
        final FileDownload download = m_txManager.downloadFile(downloadFileRequest);

        try{
            final CompletableFuture<Void> verificationFuture = download.completionFuture().thenAccept(completedDownload -> {

                final GetObjectResponse downloadResponse = completedDownload.response();
                String cloudEtag = downloadResponse.eTag();
                if (cloudEtag != null) {
                    cloudEtag = cloudEtag.replace("\"", "");
                }

                // For range requests, contentLength() returns the range size, not the total object size.
                // Parse the total object size from the Content-Range header to detect appended/truncated data.
                long sizeOnCloud = downloadResponse.contentLength();
                final String contentRange = downloadResponse.contentRange();
                if (contentRange != null && contentRange.contains("/")) {
                    final String totalSizeStr = contentRange.substring(contentRange.indexOf('/') + 1).trim();
                    if (!"*".equals(totalSizeStr)) {
                        final long totalObjectSize = Long.parseLong(totalSizeStr);
                        if (totalObjectSize != blob.getLength()) {
                            markBlobAsSuspect(blob.getId());
                            throw new RuntimeException(
                                    "Possible data corruption on blob " + blob.getId() +
                                    " detected. Expected cloud object size of " + blob.getLength() +
                                    " bytes, but actual size is " + totalObjectSize + " bytes.");
                        }
                        sizeOnCloud = totalObjectSize;
                    }
                }

                // 6. Perform local MD5 verification
                try (final Md5ComputingFileInputStream fis = constructFis(fileInCache)) {
                    // Read the entire file to calculate the digest
                    byte[] buffer = new byte[1024 * 1024];
                    while (-1 != fis.read(buffer));

                    verifyNoCorruption(
                            fis.getDigest(),
                            blob.getLength(),
                            cloudEtag,
                            sizeOnCloud,
                            blob);
                } catch (final IOException e) {
                    // Catch IO issues during local file reading/verification
                    throw new RuntimeException("Error during local MD5 verification for " + object.getName(), e);
                } catch (final SdkException e) {
                    // Catch SDK issues if fetching the response metadata failed for some reason
                    throw new RuntimeException("Error fetching S3 metadata after download for " + object.getName(), e);
                }


            });
            return Collections.singletonList(verificationFuture);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



	private Future< ? > writeBlobToCloudInSingleRequest(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final File fileInCache,
            final Set< S3ObjectProperty > metadata,
            final Object initialDataPlacement )
    {
        return s_wp.submit( new Runnable() {
			@Override
			public void run()
			{
				int retries = MAX_TRANSFER_RETRIES;
				boolean complete = false;
                final Map<String, String> userMetadata = new HashMap<>();
				while (!complete)
				{
					try ( final Md5ComputingFileInputStream fis = constructFis( fileInCache ) )
					{
                        long metadataSize = 0;
                        for (final S3ObjectProperty m : metadata) {
                            userMetadata.put(m.getKey(), m.getValue());
                            metadataSize += m.getKey().getBytes().length;
                            metadataSize += m.getValue().getBytes().length;
                        }

                        final String blobInfo = CloudUtils.getObjectInfoAsString(object.getId(), CollectionFactory.toSet(blob));
                        metadataSize += S3ObjectOnCloudInfo.OBJECT_INFO_META_KEY.getBytes().length;
                        metadataSize += blobInfo.getBytes().length;

                        if (metadataSize >= 2000) {
                            LOG.warn("Metadata size including blob info would be " + metadataSize + "B, " +
                                    "exceeding 2KB max. Saving blob info to separate object");

                            final String objectInfoOnly = CloudUtils.getObjectInfoAsString(object.getId(), new HashSet<>());
                            userMetadata.put(S3ObjectOnCloudInfo.OBJECT_INFO_META_KEY, objectInfoOnly);

                            // V2: Fallback putObject for metadata
                            // Requires RequestBody and PutObjectRequest
                            m_client.putObject(PutObjectRequest.builder()
                                            .bucket(cloudBucket.getName())
                                            .key(S3ObjectOnCloudInfo.getBlobInfoKey(cloudBucket.getOwnerId(), object.getId()))
                                            .build(),
                                    RequestBody.fromBytes(blobInfo.getBytes()));

                        } else {
                            userMetadata.put(S3ObjectOnCloudInfo.OBJECT_INFO_META_KEY, blobInfo);
                        }

                        final InputStream contentStream = new BufferedInputStream(new RangedInputStream(fis, 0, blob.getLength()));

                        final RequestBody requestBody = RequestBody.fromInputStream(contentStream, blob.getLength());


                        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                                .bucket(cloudBucket.getName())
                                .key(object.getName())
                                .contentLength(blob.getLength()) // V1: s3Metadata.setContentLength(blob.getLength())
                                .metadata(userMetadata);

                        if (null != initialDataPlacement) {
                            final String storageClassV2 = ReflectUtil.convertEnum(
                                    S3InitialDataPlacementPolicy.class,
                                    StorageClass.class,
                                    (S3InitialDataPlacementPolicy) initialDataPlacement).toString();

                            requestBuilder.storageClass(StorageClass.fromValue(storageClassV2));
                        }
                        PutObjectResponse result = m_client.putObject(requestBuilder.build(), requestBody);

                        // V1: result.getETag() is V2: result.eTag()
                        String cloudEtag = result.eTag();

                        // Verification logic remains the same
                        verifyNoCorruption(fis.getDigest(), blob.getLength(), cloudEtag, blob.getLength(), blob);
                        complete = true;

                    } catch (final SdkException | IOException e) {
                        LOG.warn("Received AWS " + e.getClass().getSimpleName() + " exception", e);
                        if (--retries > 0) {
                            // short delay then try again
                            try {
                                Thread.sleep(500);
                            } catch (final InterruptedException ie) {
                                LOG.warn("Interrupted while sleeping waiting try retry request");
                            }
                        } else {
                            // retries exceeded
                            if (e instanceof RetryableException) {
                                throw new RuntimeException("Error writing blob, will reschedule blob.");
                            } else {
                                // Re-throw the exception with the original cause
                                throw new RuntimeException("Error writing blob, will reschedule blob.", e);
                            }
                        }
                    }




				}			
			}
		});
    }
    
    
	private List< Future< ? > > writeBlobToCloudUsingMultipartUpload(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final long objectSize,
            final Blob blob,
            final String uploadId,
            final File fileInCache,
            final long maxPartLength )
    {
        long remaining = blob.getLength();
        int partNumber = -1;
        final List< Future< ? > > writeThreads = new ArrayList<>();

        while ( 0 < remaining || -1 == partNumber )
        {
            try
            {
                ++partNumber;
                final long offset = maxPartLength * partNumber;
                final long length = Math.min( remaining, maxPartLength );
                remaining -= length;

                writeThreads.add( s_wp.submit( new Runnable() {
                    @Override
                    public void run()
                    {
                    	int retries = 0;
                        boolean complete = false;
                        while ( !complete )
                        {
                            try ( final Md5ComputingFileInputStream fis = constructFis( fileInCache ) )
                            {
                                final InputStream rangedInputStream = new BufferedInputStream(new RangedInputStream( fis, offset, length ));
                            	final String cloudDigest = writePartToCloud(
                                        cloudBucket.getName(),
                                        object,
                                        uploadId,
                                        objectSize,
                                        blob,
                                        blob.getByteOffset() + offset, 
                                        rangedInputStream,
                                        length );
                            	verifyNoCorruption( fis.getDigest(), length, cloudDigest, length, blob );
                            	m_uploadTracker.completeUploadIfReady(
                            			cloudBucket,
                            			object,
                            			objectSize,
                            			uploadId,
                            			m_client );
                                complete = true;
                            } catch ( final CloudTransferFailedException e )
                            {
                            	if ( retries < MAX_TRANSFER_RETRIES )
                            	{
                            		try
									{
                            			LOG.warn( "Transfer Failure", e );
										Thread.sleep( MILLISECONDS_BETWEEN_RETRIES * retries );
									}
									catch ( InterruptedException e1 )
									{
										throw new RuntimeException( e1 ); 
									}
                            		retries++;
                            	}
                            	else
                            	{
                            		throw new RuntimeException( "Max retries exceeded to write blob part "
                            				+ blob.getId(), e );
                            	}
                            } catch ( final IOException fileStreamException )
							{
								throw new RuntimeException( fileStreamException );
							}
                        }
                    }
                } ) );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException(
                        "Failed to write blob " + blob + " from object " + object.getName() + " to cloud.", ex );
            }
        }
        return writeThreads;
    }
    

    @Override
	public final List< Future< ? > > writeBlobToCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final long objectSize,
            final Blob blob,
            final int numBlobsForObject,
            final File fileInCache,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata,
            final long maxPartLength,
            final Object initialDataPlacement )
    {
    	final List< Future< ? > > writeThreads = new ArrayList<>(); 
    	final boolean performStandardUpload = blob.getLength() <= maxPartLength && blob.getLength() == objectSize;
    	if ( performStandardUpload )
    	{
    		new VoidOperationRunner(
                    "start thread to write blob " + blob.getId() + " (" +
                     new BytesRenderer().render( blob.getLength() ) + ") in one request." )
            {
                @Override
				public void performVoidOperation()
                {
                	writeThreads.add( writeBlobToCloudInSingleRequest(
            				cloudBucket, object, blob, fileInCache, metadata, initialDataPlacement) );
                }
            }.run( Level.WARN );
    	}
    	else
    	{
    		final String uploadId = m_uploadTracker.initBlobWrite(
    				cloudBucket,
    				object,
    				metadata,
    				initialDataPlacement,
    				m_client );
    		new VoidOperationRunner(
                    "start threads to write blob " + blob.getId() + " (" +
                     new BytesRenderer().render( blob.getLength() ) + ") in parts." )
            {
                @Override
                public void performVoidOperation()
                {
            		writeThreads.addAll( writeBlobToCloudUsingMultipartUpload(
            				cloudBucket, object, objectSize, blob, uploadId, fileInCache, maxPartLength) );
                }
            }.run( Level.WARN );
    	}
        return writeThreads;
    }
        

    private Md5ComputingFileInputStream constructFis( final File file )
    {
        try
        {
            return new Md5ComputingFileInputStream( file );
        }
        catch ( IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    final protected Map< String, String > toBeanPropsMap(
            final SimpleBeanSafeToProxy bean )
    {
        final Map< String, String > retval = new HashMap<>();
        for ( final String prop : BeanUtils.getPropertyNames( bean.getClass() ) )
        {
            try
            {
                final Object rawValue = BeanUtils.getReader( bean.getClass(), prop ).invoke( bean );
                if ( null == rawValue )
                {
                    throw new RuntimeException( "Property " + prop + " cannot have a null value." );
                }
                retval.put(
                        prop,
                        MarshalUtil.getStringFromTypedValue( rawValue ) );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        return retval;
    }
    
    
    @Override
	final public BucketOnPublicCloud discoverContents(
            final PublicCloudBucketInformation cloudBucket, 
            final String marker )
    {
        return new OperationRunner<BucketOnPublicCloud>(
                "read contents for bucket " + cloudBucket.getName() + " starting at marker " + marker) {
            @Override
            protected BucketOnPublicCloud performOperation() throws Exception {
                final BucketOnPublicCloud retval = BeanFactory.newBean(BucketOnPublicCloud.class);

                final PublicCloudBucketInformation bucketInformation =
                        getExistingBucketInformation(cloudBucket.getName());
                retval.setBucketName(bucketInformation.getLocalBucketName());

                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(cloudBucket.getName())
                        .continuationToken(marker)
                        .build();

                ListObjectsV2Response results = m_client.listObjectsV2(listRequest);
                final List<S3ObjectOnMedia> objects = new ArrayList<>();

                for (software.amazon.awssdk.services.s3.model.S3Object o : results.contents()) {
                    if (cloudBucket.getOwnerId() != null && o.key().startsWith(cloudBucket.getOwnerId().toString())) {
                        continue;
                    }

                    final S3ObjectOnMedia oom = BeanFactory.newBean(S3ObjectOnMedia.class);
                    oom.setObjectName(o.key());

                    HeadObjectRequest headRequest = HeadObjectRequest.builder()
                            .bucket(cloudBucket.getName())
                            .key(o.key())
                            .build();

                    HeadObjectResponse metaOnCloud = m_client.headObject(headRequest);
                    final List<S3ObjectMetadataKeyValue> userMeta = new ArrayList<>();

                    for (Map.Entry<String, String> m : metaOnCloud.metadata().entrySet()) {
                        if ((KeyValueObservable.AMZ_META_PREFIX + m.getKey())
                                .equals(S3ObjectOnCloudInfo.OBJECT_INFO_META_KEY)) {
                            S3ObjectOnCloudInfo objectInfo = CloudUtils.getObjectInfoFromString(m.getValue());
                            oom.setId(objectInfo.getId());
                            BlobOnMedia[] blobInfo = objectInfo.getBlobs();

                            if (blobInfo.length == 0) {
                                final String blobInfoObjectKey = S3ObjectOnCloudInfo.getBlobInfoKey(
                                        cloudBucket.getOwnerId(), oom.getId());
                                LOG.info("Blob info empty in metadata. Attempting to retrieve from separate blob info " +
                                        "object at: " + cloudBucket.getName() + "/" + blobInfoObjectKey + ".");

                                GetObjectRequest getRequest = GetObjectRequest.builder()
                                        .bucket(cloudBucket.getName())
                                        .key(blobInfoObjectKey)
                                        .build();

                                ResponseInputStream<GetObjectResponse> blobInfoObjectStream =
                                        m_client.getObject(getRequest);
                                blobInfo = CloudUtils.getObjectInfoFromString(
                                        IOUtils.toString(blobInfoObjectStream)).getBlobs();
                            }
                            oom.setBlobs(blobInfo);
                        } else {
                            final S3ObjectMetadataKeyValue kvp = BeanFactory.newBean(S3ObjectMetadataKeyValue.class);
                            kvp.setKey(m.getKey());
                            kvp.setValue(m.getValue());
                            userMeta.add(kvp);
                        }
                    }

                    oom.setMetadata(userMeta.toArray(new S3ObjectMetadataKeyValue[0]));
                    if (null != oom.getBlobs()) {
                        objects.add(oom);
                    } else {
                        TargetLogger.LOG.warn("No blob info discovered for object " + oom.getObjectName()
                                + " in bucket " + cloudBucket.getName() + ".");
                    }
                }

                retval.setNextMarker(results.nextContinuationToken());
                retval.setObjects(objects.toArray(new S3ObjectOnMedia[0]));
                retval.setFailures(new HashMap<>());

                return retval;
            }
        }.run(Level.WARN);

    }
    
    
    @Override
	final public void delete( final PublicCloudBucketInformation cloudBucket, final Set< S3ObjectOnMedia > objects )
    {
        new VoidOperationRunner( 
                "delete objects: " + LogUtil.getShortVersion( objects, 10 ) )
        {
            @Override
            public void performVoidOperation() throws Exception
            {
                final Set< String > keysToDelete = BeanUtils.extractPropertyValues( objects, S3ObjectOnMedia.OBJECT_NAME );
                final Set< String > metadataFilesToDelete = BeanUtils.toMap( objects ).keySet()
                        .stream().map( id -> S3ObjectOnCloudInfo.getBlobInfoKey( cloudBucket.getOwnerId(), id ) ).collect( Collectors.toSet() );
                deleteKeys( cloudBucket.getName(), keysToDelete );
                deleteKeys( cloudBucket.getName(), metadataFilesToDelete );
            }
        }.run( Level.WARN );
    }
    
    
    protected abstract class OperationRunner< T >
    {
        protected OperationRunner( final String operationDescription )
        {
            m_operationDescription = operationDescription;
            Validations.verifyNotNull( "Operation description", m_operationDescription );
        }
        
        
        final public T run( final Level failureLogLevel )
        {
            Validations.verifyNotNull( "End point", m_endPoint );
            
            final Duration duration = new Duration();
            TargetLogger.LOG.info( "On " + m_endPoint + ", attempting to " + m_operationDescription + "..." );
            try
            {
                final T retval = performOperation();
                final String msg = 
                        "On " + m_endPoint + ", succeeded to " + m_operationDescription 
                        + " in " + duration + ".";
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
                     "On " + m_endPoint + ", failed to " + m_operationDescription
                     + " after " + duration + ".", ex );
                throw getFailure( m_operationDescription, ex );
            }
        }
        
        
        protected abstract T performOperation() throws Exception;
        
        
        private final String m_operationDescription;
    } // end inner class def
    
    
    protected abstract class VoidOperationRunner extends OperationRunner< Object >
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
    
    
    final protected void log( final Level level, final String message, final Throwable t )
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
    
    
    private static void initWorkPool()
    {
        synchronized ( WP_LOCK )
        {
            if ( null == s_wp || s_wp.isShutdown() )
            {
                s_wp = WorkPoolFactory.createWorkPool(
                        MAX_CONNECTIONS,
                        "PublicCloudConnectionWorker" );
            }
        }
    }

    private void markBlobAsSuspect(UUID blobId) {
        final JobEntryService entryService = m_serviceManager.getService( JobEntryService.class );
        Set<UUID> failedBlobIds = new HashSet<>();

        Set<BlobS3Target> blobTargets = m_serviceManager.getRetriever(BlobS3Target.class).retrieveAll(Require.all(
                Require.beanPropertyEqualsOneOf(
                        BlobTarget.TARGET_ID, m_target.getId()),
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobId))).toSet();
        for ( final BlobS3Target blobTarget : blobTargets ) {
            Set<SuspectBlobS3Target> suspectBlobs = m_serviceManager.getRetriever(SuspectBlobS3Target.class).retrieveAll(Require.all(
                    Require.beanPropertyEqualsOneOf(
                            BlobTarget.TARGET_ID, m_target.getId()),
                    Require.beanPropertyEqualsOneOf(
                            BlobObservable.BLOB_ID, blobId))).toSet();
            if (suspectBlobs.isEmpty()) {
                final SuspectBlobS3Target bean = BeanFactory.newBean( SuspectBlobS3Target.class );
                BeanCopier.copy( bean, blobTarget );
                m_serviceManager.getService( SuspectBlobS3TargetService.class ).create( bean );
                failedBlobIds.add(blobId);
            }

        }

    }

    private URI handleEndpointConfiguration(S3Target target, String endpoint) {
        // Ensure the endpoint has a scheme. If not default to http://
        URI endpointUri = null;
        try {
            if (target.isHttps() && !endpoint.startsWith("https://")) {
                endpointUri =  URI.create("https://" + endpoint);
            } else {
                endpointUri =  URI.create("http://" + endpoint);
            }
        } catch (IllegalArgumentException e) {
            TargetLogger.LOG.warn("Invalid endpoint: " + endpoint, e);
        }
        return endpointUri;
    }


    private final boolean m_verifyConnectivity;
    private final S3Client m_client;
    private final UploadTracker m_uploadTracker;
    private final S3TransferManager m_txManager;
    private static final String USER_AGENT_PREFIX = "APN/1.0 SpectraLogic/1.0 BlackPearl/";
    //RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE is 128KB in AWS Java SDK
    public final static int PUT_READ_LIMIT = 10 * 1024 * 1024; //10MB
    public final static int MAX_PART_NUMBERS = 10000;
    protected volatile String m_endPoint;
    private final static String SPECTRA_BUCKET_META_KEY = "spectra.blackpearl.bucket";
    private final static ByteArrayInputStream ZERO_LENGTH_BYTE_ARRAY_IS =
            new ByteArrayInputStream( (byte[])Array.newInstance( byte.class, 0 ) );
    private BeansServiceManager m_serviceManager;
    private S3Target m_target;
    //private final static ConcurrentHashMap< UUID, String > s_uploadIds = new ConcurrentHashMap<>();
    
    private static WorkPool s_wp;
    private final static Object WP_LOCK = new Object();
    
    private final static Logger LOG = Logger.getLogger( S3NativeConnectionImpl.class );

    private final static int MAX_TRANSFER_RETRIES = 10;
    private final static int MILLISECONDS_BETWEEN_RETRIES = 5000;
}
