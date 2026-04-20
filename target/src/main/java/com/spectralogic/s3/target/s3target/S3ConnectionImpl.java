/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.s3target;

import java.io.ByteArrayInputStream;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.*;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.core.exception.SdkServiceException;


import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Level;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3InitialDataPlacementPolicy;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.S3Connection;
import com.spectralogic.s3.target.TargetLogger;
import com.spectralogic.s3.target.frmwrk.BaseDataOfflineablePublicCloudConnection;
import com.spectralogic.s3.target.frmwrk.CloudTransferFailedException;
import com.spectralogic.s3.target.frmwrk.ContentsSegment;
import com.spectralogic.s3.target.frmwrk.TestablePublicCloudConnection;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.HttpUtil;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.shutdown.CriticalShutdownListener;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import software.amazon.awssdk.core.exception.SdkClientException;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import static com.spectralogic.s3.common.dao.domain.target.BlobTarget.TARGET_ID;

final class S3ConnectionImpl 
    extends BaseDataOfflineablePublicCloudConnection 
    implements S3Connection, TestablePublicCloudConnection
{
    S3ConnectionImpl(final S3Target target, BeansServiceManager serviceManager) {
        doNotLogWhenShutdown();
        m_target = target;
        m_serviceManager = serviceManager;

        final Duration duration = new Duration();
        final S3ClientBuilder builder = S3Client.builder();

        m_verifyConnectivity = !target.isRestrictedAccess();
        try {
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
            // PathStyle logic:
            // LocalStack generally requires true.
            // Real AWS Cloud requires false (Virtual Hosted-Style).
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

        } catch (final Exception ex) {
            throw new RuntimeException("Failed to initialize SDK connection.", ex);
        }

        try {
            if (m_verifyConnectivity) {
                verifyConnectivity();
            }
        } catch (final RuntimeException ex) {
            shutdown();
            throw ex;
        }

        addShutdownListener(new MyShutdownListener());
        TargetLogger.LOG.info("Connected to " + m_endPoint + " in " + duration + ".");
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


    private final class MyShutdownListener extends CriticalShutdownListener
    {
        @Override
        public void shutdownOccurred()
        {
            TargetLogger.LOG.info( "Closed connection to " + m_endPoint + "." );
        }
    } // end inner class def

    
    public void verifyConnectivity()
    {
        new VoidOperationRunner( "verify connectivity" )
        {
            @Override
            protected void performVoidOperation() throws Exception
            {
                // Perform an operation to verify connectivity using list buckets and the response has owner information.
                ListBucketsResponse response = m_client.listBuckets(ListBucketsRequest.builder().build());
                response.owner();

            }
        }.run( Level.WARN );
    }
    

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


    @Override
    protected Map< String, Long > getBlobPartsOnCloud(
            final String cloudBucketName,
            final String cloudKeyBlobRoot ) {
        final Map<String, Long> retval = new HashMap<>();
        String continuationToken = null;

        do {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(cloudBucketName)
                    .prefix(cloudKeyBlobRoot)
                    .continuationToken(continuationToken)
                    .build();

            ListObjectsV2Response response = m_client.listObjectsV2(request);

            for (S3Object objectSummary : response.contents()) {
                retval.put(objectSummary.key(), objectSummary.size());
            }

            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);
        return retval;
    }


    @Override
    protected boolean isBlobPartReadyToBeReadFromCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart ) throws Exception
    {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyBlobPart)
                .build();

        HeadObjectResponse response = m_client.headObject(request);

        boolean offlineStorageClass = StorageClass.GLACIER.equals(response.storageClass())
                || StorageClass.DEEP_ARCHIVE.equals(response.storageClass());

        return (!offlineStorageClass || response.restore() != null);

    }

    @Override
    protected void markBlobAsSuspect(UUID blobId)  {
        Set<BlobS3Target> blobTargets = m_serviceManager.getRetriever(BlobS3Target.class).retrieveAll(Require.all(
                Require.beanPropertyEqualsOneOf(
                        TARGET_ID, m_target.getId()),
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobId))).toSet();


        for ( final BlobS3Target blobTarget : blobTargets )
        {
            Set<SuspectBlobS3Target> suspectBlobs = m_serviceManager.getRetriever(SuspectBlobS3Target.class).retrieveAll(Require.all(
                    Require.beanPropertyEqualsOneOf(
                            TARGET_ID, m_target.getId()),
                    Require.beanPropertyEqualsOneOf(
                            BlobObservable.BLOB_ID, blobId))).toSet();
            if (suspectBlobs.isEmpty()) {
                final SuspectBlobS3Target bean = BeanFactory.newBean( SuspectBlobS3Target.class );
                BeanCopier.copy( bean, blobTarget );
                m_serviceManager.getService( SuspectBlobS3TargetService.class ).create( bean );
            }
        }
    }


    @Override
    protected void beginStagingBlobPartToRead( 
            final String cloudBucketName,
            final String cloudKeyBlobPart,
            final int stagedDataExpirationInDays )
    {
        try {
            RestoreRequest restoreRequest = RestoreRequest.builder()
                    .days(stagedDataExpirationInDays)
                    .build();

            RestoreObjectRequest request = RestoreObjectRequest.builder()
                    .bucket(cloudBucketName)
                    .key(cloudKeyBlobPart)
                    .restoreRequest(restoreRequest)
                    .build();

            m_client.restoreObject(request);

        } catch (S3Exception ex) {
            if ("RestoreAlreadyInProgress".equalsIgnoreCase(ex.awsErrorDetails().errorCode())) {
                LOG.info("Restore already in progress for " + cloudKeyBlobPart + ".");
            } else if ("InvalidObjectState".equalsIgnoreCase(ex.awsErrorDetails().errorCode())) {
                LOG.info("Restore already completed for " + cloudKeyBlobPart + ".", ex);
            } else {
                throw ex;
            }
        }
    }


    @Override
    protected InputStream readBlobPartFromCloud( 
            final String cloudBucketName,
            final String cloudKeyBlobPart )
    {
        final GetObjectRequest request = GetObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyBlobPart)
                .build();
        return m_client.getObject(request);
    }
    
    
    @Override
    protected boolean isObjectMetadataWritten(
            final String cloudBucketName,
            final String cloudKeyObjectMetadata )
    {
        final HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyObjectMetadata)
                .build();
        try {
            m_client.headObject(headObjectRequest);
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }
    
    
    @Override
    protected void writeObjectMetadata( 
            final String cloudBucketName,
            final String cloudKeyObjectMetadata,
            final ByteArrayInputStream metadata,
            final Object initialDataPlacement )
    {
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyObjectMetadata);
        final int contentLength = metadata.available();
        if (null != initialDataPlacement) {
            StorageClass storageClass = ReflectUtil.convertEnum(
                    S3InitialDataPlacementPolicy.class,
                    StorageClass.class,
                    (S3InitialDataPlacementPolicy) initialDataPlacement);

            requestBuilder.storageClass(storageClass);
        }
        final PutObjectRequest request = requestBuilder.build();
        final RequestBody requestBody = RequestBody.fromInputStream(metadata, contentLength);
        m_client.putObject(request, requestBody);

    }
    
    
    @Override
    protected void writeObjectIndex( 
            final String cloudBucketName, 
            final String cloudKeyObjectIndex,
            final Map< String, String > metadata,
            final Object initialDataPlacement)
    {
        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyObjectIndex);
        if (null != initialDataPlacement) {
            StorageClass storageClass = ReflectUtil.convertEnum(
                    S3InitialDataPlacementPolicy.class,
                    StorageClass.class,
                    (S3InitialDataPlacementPolicy) initialDataPlacement);

            requestBuilder.storageClass(storageClass);
        }
        final PutObjectRequest request = requestBuilder.build();
        final RequestBody requestBody = RequestBody.empty();

        m_client.putObject(request, requestBody);
    }


    @Override
    protected void writeBlobPartToCloud( 
            final String cloudBucketName,
            final String cloudKeyBlobPart,
            final Blob blob,
            final InputStream inputStream, 
            final long length,
            Object initialDataPlacement ) throws CloudTransferFailedException {

        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyBlobPart)
                .metadata(toBeanPropsMap(blob));
        if (length == blob.getLength() && ChecksumType.MD5 == blob.getChecksumType()
                && !ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED.equals(blob.getChecksum())) {
            requestBuilder.contentMD5(blob.getChecksum());
        }
        if (null != initialDataPlacement) {
            StorageClass storageClass = ReflectUtil.convertEnum(
                    S3InitialDataPlacementPolicy.class,
                    StorageClass.class,
                    (S3InitialDataPlacementPolicy) initialDataPlacement);
            requestBuilder.storageClass(storageClass);
        }
        final PutObjectRequest request = requestBuilder.build();
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, length);
        try {
            m_client.putObject(request, requestBody);
        } catch (S3Exception e) {
            if (e.statusCode() == HttpServletResponse.SC_NOT_FOUND) {
                throw new RuntimeException(e);
            }

            throw new CloudTransferFailedException(e);

        } catch (SdkClientException e) {
            LOG.warn("Received AWS SdkClientException during putObject", e);
            throw new CloudTransferFailedException("Error writing blob part, will reschedule blob");

        }

    }


    // Helper method to check if a bucket exists
    private boolean bucketExists(String bucketName) {
        try {
            m_client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            return true;
        } catch (S3Exception e) {
            // If the exception's status code is 404, the bucket does not exist
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }


    @Override
    protected String getBlobPartWrittenToCloudMd5Base64(
            final String cloudBucketName,
            final String cloudKeyBlobPart )
    {
        try {
            final HeadObjectResponse response = m_client.headObject(HeadObjectRequest.builder()
                    .bucket(cloudBucketName)
                    .key(cloudKeyBlobPart)
                    .build());

            final String etag = response.eTag();

            if (etag == null || (etag.length() != 32 && etag.contains("-"))) {
                return null;
            }

            String hexEtag = etag.replace("\"", "");


            return Base64.encodeBase64String(Hex.decodeHex(hexEtag.toCharArray()));

        } catch (final SdkClientException | DecoderException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    protected ContentsSegment discoverContentsSegment(
            final String cloudBucketName,
            final String marker )
    {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(cloudBucketName)
                .prefix(DATA)
                .continuationToken(marker)
                .build();
        final ListObjectsV2Response results = m_client.listObjectsV2(listObjectsRequest);

        final Set<String> keys = new HashSet<>();
        for (final S3Object o : results.contents()) {
            keys.add(o.key());
        }

        return new ContentsSegment(keys, results.nextContinuationToken());

    }


    @Override
    protected Map< String, String > discoverBlobOnCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart )
    {
        final HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyBlobPart)
                .build();

        try {
            final HeadObjectResponse response = m_client.headObject(request);

            return response.metadata();

        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return Collections.emptyMap();
            }
            throw e;
        }
    }


    @Override
    protected InputStream discoverObjectOnCloud(
            final String cloudBucketName,
            final String cloudKeyObjectMetadata )
    {
        final GetObjectRequest request = GetObjectRequest.builder()
                .bucket(cloudBucketName)
                .key(cloudKeyObjectMetadata)
                .build();

        return m_client.getObject(request);
    }


    @Override
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
    
    
    @Override
    protected FailureTypeObservableException getFailure(
            final String operationDescription,
            final Exception ex )
    {
        Throwable t = ex;
        SdkServiceException sdkServiceException = null;
        while (null != t) {
            if (t instanceof SdkServiceException) {
                sdkServiceException = (SdkServiceException) t;
                break;
            }
            t = t.getCause();
        }
        if (null == sdkServiceException) {
            t = ex;
        }
        final S3SdkFailure sdkFailure;


        if (sdkServiceException != null) {
            S3Exception s3Exception = (S3Exception) sdkServiceException;

            String errorCode = s3Exception.awsErrorDetails().errorCode();
            int statusCode = sdkServiceException.statusCode();

            // V1: S3SdkFailure.valueOf(errorCode, statusCode)
            sdkFailure = S3SdkFailure.valueOf(errorCode, statusCode);
        } else {
            sdkFailure = null;
        }


        return new FailureTypeObservableException(
                (null != sdkFailure) ? sdkFailure : GenericFailure.CONFLICT,
                "Failed to " + operationDescription + " on " + m_endPoint + " since "
                        + ExceptionUtil.getReadableMessage(ex) + ".",
                ex);

    }


    public void createGenericBucket( final String name )
    {
        final CreateBucketRequest request = CreateBucketRequest.builder()
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



    private final boolean m_verifyConnectivity;
    private final S3Client m_client;
    private static final String USER_AGENT_PREFIX = "APN/1.0 SpectraLogic/1.0 BlackPearl/";
    //RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE is 128KB in AWS Java SDK
    public final static int PUT_READ_LIMIT = 10 * 1024 * 1024; //10MB
    private BeansServiceManager m_serviceManager;
    private S3Target m_target;
}
