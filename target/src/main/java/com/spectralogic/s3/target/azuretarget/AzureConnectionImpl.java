/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.azuretarget;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.target.SuspectBlobAzureTargetService;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.BlobReadFailedException;
import org.apache.log4j.Level;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultContinuationType;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageErrorCode;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.StorageEventMultiCaster;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.s3.common.rpc.target.AzureConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.target.TargetLogger;
import com.spectralogic.s3.target.frmwrk.BaseDataAlwaysOnlinePublicCloudConnection;
import com.spectralogic.s3.target.frmwrk.CloudTransferFailedException;
import com.spectralogic.s3.target.frmwrk.ContentsSegment;
import com.spectralogic.s3.target.frmwrk.TestablePublicCloudConnection;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.shutdown.CriticalShutdownListener;

import static com.microsoft.azure.storage.CloudStorageAccount.getDevelopmentStorageAccount;
import static com.spectralogic.s3.common.dao.domain.target.BlobTarget.TARGET_ID;

final class AzureConnectionImpl 
    extends BaseDataAlwaysOnlinePublicCloudConnection 
    implements AzureConnection, TestablePublicCloudConnection
{

    AzureConnectionImpl( final CloudStorageAccount cloudStorageAccount, final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        doNotLogWhenShutdown();
        final Duration duration = new Duration();
        initializeClient(cloudStorageAccount);
        addShutdownListener( new MyShutdownListener() );
        TargetLogger.LOG.info( "Connected to " + m_endPoint + " in " + duration + "." );
    }

    AzureConnectionImpl( final AzureTarget target, final BeansServiceManager serviceManager )
    {
        m_target = target;
        m_serviceManager = serviceManager;
        doNotLogWhenShutdown();
        final Duration duration = new Duration();
        final String version =
                ConfigurationInformationProvider.getInstance().getBuildInformation().getVersion();
        CloudStorageAccount cloudStorageAccount;
        StorageCredentialsAccountAndKey storageCredentials;
        try
        {
            storageCredentials =
                    new StorageCredentialsAccountAndKey( target.getAccountName(), target.getAccountKey() );

            if (version.equals("TEST_VERSION")) {
                cloudStorageAccount = getDevelopmentStorageAccount();
            } else if (version.equals("DOCKER_VERSION")) {
                 String storageConnectionString =
                         "DefaultEndpointsProtocol=http;" +
                                 "AccountName=devstoreaccount1;" +
                                 "AccountKey=Ss0sk4dZsuH0Cji92F1Ye2kuoEhv+mmYCLfLzGrdw0A1zQagbiBBbnHJNiALudX5nXXZkc4lxT0nFREbg8lpAQ==;" +
                                 "BlobEndpoint=http://azurite:10000/devstoreaccount1;";

                 cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
                 cloudStorageAccount.createCloudBlobClient();
            } else {
                cloudStorageAccount = new CloudStorageAccount( storageCredentials, target.isHttps() );
            }
            initializeClient(cloudStorageAccount);
        }
        /*
         * This is intended to catch and rethrow the exception thrown by the innermost try catch block. This is so that
         * it isn't caught and thrown as a RuntimeException in the final catch below.
         */
         catch ( final FailureTypeObservableException ex )
         {
             throw ex;
         }
        /*
         * Catch runtime exceptions here because the AzureSDK is supposed to throw a URISyntaxException when
         * invalid credentials are given, but sometimes it throws runtime exceptions instead.
         */
         catch ( final RuntimeException rtex )
         {
                final AzureSdkFailure sdkFailure = AzureSdkFailure.valueOf( StorageErrorCode.ACCESS_DENIED.toString(),
                        HttpURLConnection.HTTP_FORBIDDEN );
                throw new FailureTypeObservableException( sdkFailure, "Failed to authenticate, most likely caused by"
                        + " invalid credentials." );
         }

        /*
         * When the CloudStorageAccount variable is created, it will throw this URISyntaxException if the given account
         * name is invalid, so this catches that error and returns a 403 error with the invalid credentials message
         * instead of the 500 error it would have done with the following catch block.
         */
        catch ( final URISyntaxException ex )
        {
            final AzureSdkFailure sdkFailure = AzureSdkFailure.valueOf( StorageErrorCode.ACCESS_DENIED.toString(),
                    HttpURLConnection.HTTP_FORBIDDEN );
            throw new FailureTypeObservableException( sdkFailure, "Invalid credentials." );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to initialize SDK connection.", ex );
        }

        addShutdownListener( new MyShutdownListener() );
        TargetLogger.LOG.info( "Connected to " + m_endPoint + " in " + duration + "." );
    }

    private void initializeClient(CloudStorageAccount cloudStorageAccount) {
        m_client = cloudStorageAccount.createCloudBlobClient();
        m_endPoint = m_client.getEndpoint().toString();
        final String version =
                ConfigurationInformationProvider.getInstance().getBuildInformation().getVersion();
        if (null == version) {
            TargetLogger.LOG.warn(
                    "Unable to retrieve version info. Will not set user agent header data.");
        } else {
            final String userAgent = USER_AGENT_PREFIX + version;
            TargetLogger.LOG.info("Configuring Azure with user agent: " + userAgent);
            final HashMap<String, String> map = new HashMap<>();
            map.put("User-Agent", userAgent);
            final StorageEvent<SendingRequestEvent> listener = new StorageEvent<SendingRequestEvent>() {
                @Override
                public void eventOccurred(final SendingRequestEvent arg0) {
                    HashMap<String, String> headers = arg0.getOpContext().getUserHeaders();
                    if (null == headers) {
                        headers = map;
                    } else {
                        headers.putAll(map);
                    }
                    arg0.getOpContext().setUserHeaders(headers);
                }
            };
            final StorageEventMultiCaster<SendingRequestEvent, StorageEvent<SendingRequestEvent>>
                    globalRetryingEventHandler = new StorageEventMultiCaster<>();
            globalRetryingEventHandler.addListener(listener);
            OperationContext.setGlobalSendingRequestEventHandler(globalRetryingEventHandler);
        }
        try
        {
            verifyConnectivity();
        }
        catch ( final RuntimeException ex )
        {
            shutdown();
            throw ex;
        }
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
                final CloudBlobContainer container =
                        m_client.getContainerReference( UUID.randomUUID().toString() );
                container.exists();
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
                final CloudBlobContainer container;
                try
                {
                     container = m_client.getContainerReference( bucketName );
                     if ( !container.exists() )
                     {
                         return null;
                     }
                }
                catch (StorageException e)
                {
                    if ( 400 == e.getHttpStatusCode() )
                    {
                        LOG.warn( "Received 400 trying to discover bucket \"" + bucketName + "\" on target."
                                + " Will assume bucket does not exist on target.", e);
                        return null;
                    }
                    throw e;
                }
                
                final CloudBlockBlob cb = container.getBlockBlobReference( SPECTRA_BUCKET_META_KEY );
                if ( cb.exists() )
                {
                    return MarshalUtil.newBean(
                            PublicCloudBucketInformation.class,
                            cb.getMetadata() );
                }
                
                return BeanFactory.newBean( PublicCloudBucketInformation.class ).setName( bucketName );
            }
        }.run( Level.WARN );
    }


    public PublicCloudBucketInformation createOrTakeoverBucket(
            final Object initialDataPlacement,
            final PublicCloudBucketInformation cloudBucket )
    {
        return new OperationRunner< PublicCloudBucketInformation >( "create bucket " + cloudBucket.getName() )
        {
            @Override
            protected PublicCloudBucketInformation performOperation() throws Exception
            {
                final CloudBlobContainer container = m_client.getContainerReference( cloudBucket.getName() );
                container.create();
                
                takeOwnershipInternal( cloudBucket );
                
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
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucket.getName() );
        final CloudBlockBlob metadata = container.getBlockBlobReference( SPECTRA_BUCKET_META_KEY );
        metadata.setMetadata( new HashMap<>( toBeanPropsMap( cloudBucket ) ) );
        metadata.upload( ZERO_LENGTH_BYTE_ARRAY_IS, 0 );
    }


    @Override
    protected void markBlobAsSuspect(UUID blobId)  {
        Set<BlobAzureTarget> blobTargets = m_serviceManager.getRetriever(BlobAzureTarget.class).retrieveAll(Require.all(
                Require.beanPropertyEqualsOneOf(
                        TARGET_ID, m_target.getId()),
                Require.beanPropertyEqualsOneOf(
                        BlobObservable.BLOB_ID, blobId))).toSet();

        for ( final BlobAzureTarget blobTarget : blobTargets )
        {
            Set<SuspectBlobAzureTarget> suspectBlobs = m_serviceManager.getRetriever(SuspectBlobAzureTarget.class).retrieveAll(Require.all(
                    Require.beanPropertyEqualsOneOf(
                            BlobTarget.TARGET_ID, m_target.getId()),
                    Require.beanPropertyEqualsOneOf(
                            BlobObservable.BLOB_ID, blobId))).toSet();
            if (suspectBlobs.isEmpty()) {
                final SuspectBlobAzureTarget bean = BeanFactory.newBean( SuspectBlobAzureTarget.class );
                BeanCopier.copy( bean, blobTarget );
                m_serviceManager.getService( SuspectBlobAzureTargetService.class ).create( bean );
            }
        }
    }

    @Override
    protected Map< String, Long > getBlobPartsOnCloud(
            final String cloudBucketName,
            final String cloudKeyBlobRoot ) throws Exception
    {
        final Map< String, Long > retval = new HashMap<>();
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        for ( final ListBlobItem e : container.listBlobs( cloudKeyBlobRoot, true ) )
        {
            if ( !CloudBlockBlob.class.isAssignableFrom( e.getClass() ) )
            {
                continue;
            }
            final CloudBlockBlob s3Object = (CloudBlockBlob)e;
            retval.put( s3Object.getName(), Long.valueOf( s3Object.getProperties().getLength() ) );
        }
        return retval;
    }


    @Override
    protected InputStream readBlobPartFromCloud( 
            final String cloudBucketName,
            final String cloudKeyBlobPart )
    {
    	try
    	{
	        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
	        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyBlobPart );
	        return s3Object.openInputStream();
    	}
    	catch( final Exception e )
    	{
    		throw new RuntimeException( e );
    	}
    }
    
    
    @Override
    protected boolean isObjectMetadataWritten(
            final String cloudBucketName,
            final String cloudKeyObjectMetadata ) throws Exception
    {
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyObjectMetadata );
        return s3Object.exists();
    }
    
    
    @Override
    protected void writeObjectMetadata( 
            final String cloudBucketName,
            final String cloudKeyObjectMetadata,
            final ByteArrayInputStream metadata,
            final Object initialDataPlacement ) throws Exception
    {
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyObjectMetadata );
        s3Object.upload( metadata, metadata.available() );
    }
    
    
    @Override
    protected void writeObjectIndex( 
            final String cloudBucketName, 
            final String cloudKeyObjectIndex,
            final Map< String, String > metadata,
            final Object initialDataPlacement) throws Exception
    {
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyObjectIndex );
        s3Object.setMetadata( new HashMap<>( metadata ) );
        s3Object.upload( ZERO_LENGTH_BYTE_ARRAY_IS, 0 );
    }


    @Override
    protected void writeBlobPartToCloud( 
            final String cloudBucketName,
            final String cloudKeyBlobPart,
            final Blob blob,
            final InputStream inputStream, 
            final long length,
            final Object initialDataPlacement ) throws CloudTransferFailedException
    {
    	final CloudBlobContainer container;
    	try
    	{
    		container = m_client.getContainerReference( cloudBucketName );
    	}
    	catch ( final StorageException | URISyntaxException e )
    	{
    		throw new RuntimeException( e );
    	}
    	try
    	{
	        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyBlobPart );
	        if ( length == blob.getLength() && ChecksumType.MD5 == blob.getChecksumType()
	        		&& !ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED.equals( blob.getChecksum() ) )
    		{
	            s3Object.getProperties().setContentMD5( blob.getChecksum() );
	        }
	        s3Object.setMetadata( new HashMap<>( toBeanPropsMap( blob ) ) );
	        s3Object.upload( inputStream, length );
    	}
    	catch ( final Exception e )
    	{
    		throw new CloudTransferFailedException( e );
    	}
    }
    
    
    @Override
    protected String getBlobPartWrittenToCloudMd5Base64(
            final String cloudBucketName,
            final String cloudKeyBlobPart )
    {
        CloudBlobContainer container;
		try
		{
			container = m_client.getContainerReference( cloudBucketName );
	        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyBlobPart );
	        s3Object.downloadAttributes();
	        return s3Object.getProperties().getContentMD5();
		} catch ( final URISyntaxException | StorageException e )
		{
			throw new RuntimeException( e );
		}
    }


    @Override
    protected ContentsSegment discoverContentsSegment(
            final String cloudBucketName,
            final String marker ) throws Exception
    {
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        final ResultContinuation continuation = new ResultContinuation();
        continuation.setContinuationType( ResultContinuationType.BLOB );
        continuation.setNextMarker( marker );
        final ResultSegment< ListBlobItem > results = container.listBlobsSegmented(
                DATA,
                true,
                EnumSet.noneOf( BlobListingDetails.class ), 
                Integer.valueOf( 1024 ), 
                continuation,
                null,
                null );
        
        final Set< String > keys = new HashSet<>();
        for ( final ListBlobItem e : results.getResults() )
        {
            keys.add( ( (CloudBlockBlob)e ).getName() );
        }
        
        return new ContentsSegment(
                keys,
                ( null == results.getContinuationToken() ) ?
                        null 
                        : results.getContinuationToken().getNextMarker() );
    }


    @Override
    protected Map< String, String > discoverBlobOnCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart ) throws Exception
    {
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyBlobPart );
        s3Object.downloadAttributes();
        return s3Object.getMetadata();
    }


    @Override
    protected InputStream discoverObjectOnCloud(
            final String cloudBucketName,
            final String cloudKeyObjectMetadata ) throws Exception
    {
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        final CloudBlockBlob s3Object = container.getBlockBlobReference( cloudKeyObjectMetadata );
        return s3Object.openInputStream();
    }


    @Override
    protected void deleteKeys( final String cloudBucketName, final Set< String > cloudKeys ) throws Exception
    {
        final CloudBlobContainer container = m_client.getContainerReference( cloudBucketName );
        for ( final String key : cloudKeys )
        {
            try
            {
                container.getBlockBlobReference( key ).delete();
                TargetLogger.LOG.info( "Deleted key: " + key );
            }
            catch ( final StorageException ex )
            {
                TargetLogger.LOG.info( "Failed to delete key: " + key, ex );
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
                m_client.getContainerReference( cloudBucketName ).deleteIfExists();
            }
        }.run( Level.WARN );
    }
    
    
    @Override
    protected FailureTypeObservableException getFailure(
            final String operationDescription, 
            final Exception ex )
    {
        Throwable t = ex;
        while ( null != t )
        {
            if ( StorageException.class.isAssignableFrom( t.getClass() ) )
            {
                break;
            }
            t = t.getCause();
        }
        if ( null == t )
        {
            t = ex;
        }

        final AzureSdkFailure sdkFailure = ( StorageException.class.isAssignableFrom( t.getClass() ) ) ?
                AzureSdkFailure.valueOf( 
                        ( (StorageException)t ).getErrorCode(), ( (StorageException)t ).getHttpStatusCode() )
                : null;
        if (ex instanceof BlobReadFailedException) {
            return (FailureTypeObservableException) ex;
        }
        return new FailureTypeObservableException(
                ( null != sdkFailure ) ? sdkFailure : GenericFailure.CONFLICT,
                "Failed to " + operationDescription + " on " + m_endPoint + " since "
                + ExceptionUtil.getReadableMessage( ex ) + ".", 
                ex );
    }


    public void createGenericBucket( final String name ) throws Exception
    {
        m_client.getContainerReference( name ).create();
    }
    
    
    @Override
    public void deleteOldBuckets( final long maxAgeInMillis, final String bucketNamePrefix )
    {
        final Iterable< CloudBlobContainer > cloudBuckets = m_client.listContainers();
        for ( CloudBlobContainer b : cloudBuckets )
        {
            final long cloudBucketAge =
                    System.currentTimeMillis() - b.getProperties().getLastModified().getTime();
            if ( b.getName().startsWith( bucketNamePrefix ) && cloudBucketAge >= maxAgeInMillis )
            {
                deleteBucket( b.getName() );
            }
        }
    }

    private  CloudBlobClient m_client;
    private final BeansServiceManager m_serviceManager;
    private static final String USER_AGENT_PREFIX = "APN/1.0 SpectraLogic/1.0 BlackPearl/";
    private  AzureTarget m_target;
}
