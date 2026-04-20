/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.s3.common.platform.cache.CacheListener;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheInformation;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.server.mock.*;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.apache.commons.io.FileUtils;

import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.dispatch.RequestDispatcherImpl;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.MockRpcFailureResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetObjectRequestHandler_Test 
{
    @Test
    public void testGetMultipleVersions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        MockDiskManager cacheManager = new MockDiskManager(dbSupport.getServiceManager());
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        DataPolicy policy = mockDaoDriver.createDataPolicy("policy1");
        policy.setVersioning(VersioningLevel.KEEP_MULTIPLE_VERSIONS);
        mockDaoDriver.updateBean(
                policy.setVersioning(VersioningLevel.KEEP_MULTIPLE_VERSIONS),
                DataPolicy.VERSIONING );
        Bucket bucket = mockDaoDriver.createBucket( null, policy.getId(), "b1");
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1");
        final Blob b1 = mockDaoDriver.getBlobFor( o.getId() );


        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o1");
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o1");
        final Blob b3 = mockDaoDriver.getBlobFor( o2.getId() );

        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        cacheManager.blobLoadedToCache(b2.getId());
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                bucket.getName() + "/" + o.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 307 );
    }



    @Test
    public void testGetPrimedObjectThatSpansMultipleBlobsWithoutOffsetOrByteRangesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetObjectThatSpansMultipleBlobsWithoutOffsetOrByteRangesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetPrimedObjectThatSpansMultipleBlobsWithoutOffsetByteRangesSpanBlobsNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10,110-120" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetObjectThatSpansMultipleBlobsWithoutOffsetByteRangesSpanBlobsNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10,110-120" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetPrimedObjectNotRelyingOnImplicitJobIdResolutionAlwaysWorks()
                throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1 = dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10" ).addParameter(
                                RequestParameterType.JOB.toString(), job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );
        
        mockDaoDriver.attainOneAndOnly( Job.class );
        mockDaoDriver.updateBean( 
                job.setImplicitJobIdResolution( true ),
                Job.IMPLICIT_JOB_ID_RESOLUTION );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10" ).addParameter(
                                RequestParameterType.JOB.toString(), job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );
        
        mockDaoDriver.attainOneAndOnly( Job.class );
    }
    
    
    @Test
    public void testGetPrimedObjectRelyingOnImplicitJobIdResolutionWorksWhenImplicitResolutionEnabled()
                throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        final UUID id1 = dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();

        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setImplicitJobIdResolution( true ),
                Job.IMPLICIT_JOB_ID_RESOLUTION );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "11" );
        requiredHeaders.put( 
                S3HeaderType.ACCEPT_BYTE_RANGES, 
                "bytes" );
        requiredHeaders.put( 
                S3HeaderType.CONTENT_BYTE_RANGES, 
                "bytes 0-10/200" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(
                job.isNaked(),
                "Should notta been a naked job since job was created explicitly."
                 );
    }
    
    @Test
    public void testGetPrimedObjectRelyingOnImplicitJobIdCreatesSecondJob()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setImplicitJobIdResolution( false ),
                Job.IMPLICIT_JOB_ID_RESOLUTION );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1 = dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "11" );
        requiredHeaders.put( 
                S3HeaderType.ACCEPT_BYTE_RANGES, 
                "bytes" );
        requiredHeaders.put( 
                S3HeaderType.CONTENT_BYTE_RANGES, 
                "bytes 0-10/200" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );

        assertEquals(2,  support.getDatabaseSupport().getServiceManager().getRetriever(Job.class).getCount(), "Shoulda created a second job since existing one doesn't permit implicit job id resolution.");
    }
    
    
    @Test
    public void testGetPrimedObjectWithSingleByteRangeWithinSingleBlobAllowed()
                throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "11" );
        requiredHeaders.put( 
                S3HeaderType.ACCEPT_BYTE_RANGES, 
                "bytes" );
        requiredHeaders.put( 
                S3HeaderType.CONTENT_BYTE_RANGES, 
                "bytes 0-10/200" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(
                job.isNaked(),
                "Should notta been a naked job since job was created explicitly."
                 );
    }
    
    
    @Test
    public void testGetObjectWithSingleByteRangeWithinSingleBlobAllowed()
                throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "11" );
        requiredHeaders.put( 
                S3HeaderType.ACCEPT_BYTE_RANGES, 
                "bytes" );
        requiredHeaders.put( 
                S3HeaderType.CONTENT_BYTE_RANGES, 
                "bytes 0-10/200" );
    }
    
    
    @Test
    public void testGetPrimedObjectWithMultipleByteRangesWithinSingleBlobAllowed()
                throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10, 15-19" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "16" );
        requiredHeaders.put( 
                S3HeaderType.ACCEPT_BYTE_RANGES, 
                "bytes" );
        requiredHeaders.put( 
                S3HeaderType.CONTENT_BYTE_RANGES, 
                "bytes 0-10,15-19/200" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }
    
    
    @Test
    public void testGetPrimedObjectWithMultipleByteRangesWithinBlobWithOffsetAllowed()
                throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                        .addParameter( RequestParameterType.OFFSET.toString(), "100" )
                        .addHeader( 
                                S3HeaderType.BYTE_RANGES,
                                "bytes=100-110, 115-119" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "16" );
        requiredHeaders.put( 
                S3HeaderType.ACCEPT_BYTE_RANGES, 
                "bytes" );
        requiredHeaders.put( 
                S3HeaderType.CONTENT_BYTE_RANGES, 
                "bytes 100-110,115-119/200" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }
    
    
    @Test
    public void testGetObjectWithMultipleByteRangesWithinSingleBlobAllowed()
                throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1 = dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() ).addHeader( 
                        S3HeaderType.BYTE_RANGES, "bytes=0-10, 15-19" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "16" );
        requiredHeaders.put( 
                S3HeaderType.ACCEPT_BYTE_RANGES, 
                "bytes" );
        requiredHeaders.put( 
                S3HeaderType.CONTENT_BYTE_RANGES, 
                "bytes 0-10,15-19/200" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }
    
    
    @Test
    public void testGetObjectRequestReturnsMetadata() throws NoSuchMethodException, SecurityException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );



        final String bucketName = "testBucket";
        final String objectName = "testObject";

        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "x-amz-meta-test", "this value tests custom metadata" );
        propertiesMapping.put( "x-amz-meta-another", "more than one metadata field can be set" );


        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final UUID objectId = mockDaoDriver.createObject( bucket.getId(), objectName,28 ).getId();
        mockDaoDriver.createObjectProperties( objectId, propertiesMapping );
        support.getDatabaseSupport().getServiceManager().getService( BlobService.class ).update( 
                mockDaoDriver.getBlobFor( objectId )
                    .setChecksumType( ChecksumType.CRC_32 ).setChecksum( "D3c2Rg==" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        propertiesMapping.put( "Content-CRC32", "D3c2Rg==" );
        propertiesMapping.put( "Content-Length", "28" );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1 = dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 28 );
        file1.deleteOnExit();
        final BlobCache bc1 = BeanFactory.newBean(BlobCache.class).setSizeInBytes(file1.length());
        bc1.setId(UUID.randomUUID());
        bc1.setPath(file1.getPath());
        bc1.setLastAccessed(new Date(System.currentTimeMillis()));
        bc1.setState(CacheEntryState.IN_CACHE);
        bc1.setBlobId(id1);
        dbSupport.getDataManager().createBean( bc1 );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                bucketName + "/" + objectName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientHasHeaders( propertiesMapping );
        
        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH,
                "28" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertTrue(job.isNaked(), "Shoulda been a naked job since job was created implicitly.");
    }
    
    
    @Test
    public void testGetObjectWhenPutJobForSameStillCreatesNakedGetJob()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();



        final String bucketName = "testBucket";
        final String objectName = "testObject";

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final UUID objectId = mockDaoDriver.createObject( bucket.getId(), objectName ).getId();
        final Blob blob = mockDaoDriver.getBlobFor( objectId );
        support.getDatabaseSupport().getServiceManager().getService( BlobService.class ).update( 
                blob.setChecksumType( ChecksumType.CRC_32 ).setChecksum( "D3c2Rg==" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        
        mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 10 );
        file1.deleteOnExit();

        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                bucketName + "/" + objectName );;
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(2,  support.getDatabaseSupport().getServiceManager().getRetriever(Job.class).getCount(), "Shoulda created GET job even though there was a PUT job for the same blob.");

        final Job job = support.getDatabaseSupport().getServiceManager().getRetriever( Job.class ).attain( 
                JobObservable.REQUEST_TYPE, JobRequestType.GET );
        assertTrue(job.isNaked(), "Job created shoulda been naked and permitted implicit job id resolution.");
        assertTrue(job.isImplicitJobIdResolution(), "Job created shoulda been naked and permitted implicit job id resolution.");
    }
    
    
    @Test
    public void testGetObjectWhenCreatedButNotInCacheReturns307() 
            throws NoSuchMethodException, SecurityException
    {
        RequestDispatcherImpl.disableSleepsFor307s();
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        final StartObjectReadInvocationHandler sorih = new StartObjectReadInvocationHandler();
        sorih.m_fails = true;
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                sorih,
                ih ) );

        final String bucketName = "testBucket";
        final String objectName = "testObject";

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final UUID objectId = mockDaoDriver.createObject( bucket.getId(), objectName ).getId();
        mockDaoDriver.simulateObjectUploadCompletion( objectId );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                bucketName + "/" + objectName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 307 );
    }
    
    
    @Test
    public void testGetObjectWhenNotCreatedAndNotInCacheNotAllowed() 
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        final StartObjectReadInvocationHandler sorih = new StartObjectReadInvocationHandler();
        sorih.m_fails = true;
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                sorih,
                ih ) );

        final String bucketName = "testBucket";
        final String objectName = "testObject";

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        mockDaoDriver.createObjectStub( bucket.getId(), objectName, 10 ).getId();
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                bucketName + "/" + objectName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testGetFolderWithoutCreationDateNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObjectStub( null, "folder/", 0 );
        mockDaoDriver.createObjectProperties( o.getId(), CollectionFactory.toMap( "somekey", "somevalue" ) );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testGetFolderWorksWithoutGoingToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObjectStub( null, "folder/", 0 );
        mockDaoDriver.createObjectProperties( o.getId(), CollectionFactory.toMap( "somekey", "somevalue" ) );
        mockDaoDriver.simulateObjectUploadCompletion( o.getId() );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Map< String, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( 
                S3HeaderType.CONTENT_LENGTH.toString(),
                "0" );
        requiredHeaders.put( 
                "somekey",
                "somevalue" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }

    @Test
    public void testGetObjectNoCacheOption()
            throws NoSuchMethodException, SecurityException
    {
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );

        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();

        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addParameter( RequestParameterType.OFFSET.toString(), "100" )
                .addHeader(
                        S3HeaderType.BYTE_RANGES,
                        "bytes=100-110, 115-119" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );

        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put(
                S3HeaderType.CONTENT_LENGTH,
                "16" );
        requiredHeaders.put(
                S3HeaderType.ACCEPT_BYTE_RANGES,
                "bytes" );
        requiredHeaders.put(
                S3HeaderType.CONTENT_BYTE_RANGES,
                "bytes 100-110,115-119/200" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }

    @Test
    public void testGetObjectWhenObjectIsInCache()
            throws NoSuchMethodException, SecurityException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final String bucketName = "testBucket";
        final String objectName = "testObject";


        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), objectName, 100 );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final BlobCacheService cache = dbSupport.getServiceManager().getService( BlobCacheService.class );

        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final BlobCache bc1 = BeanFactory.newBean(BlobCache.class).setSizeInBytes(file1.length());
        bc1.setId(UUID.randomUUID());
        bc1.setPath(file1.getPath());
        bc1.setLastAccessed(new Date(System.currentTimeMillis()));
        bc1.setState(CacheEntryState.IN_CACHE);
        bc1.setBlobId(id1);
        dbSupport.getDataManager().createBean( bc1 );



        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheResultInvocationHandler( id1),
                null );
        final StartObjectReadInvocationHandler sorih = new StartObjectReadInvocationHandler(file1);

        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                sorih,
                ih ) );



        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                bucketName + "/" + objectName )
                .addParameter(String.valueOf(RequestParameterType.CACHED_ONLY), "true")
                ;
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertFalse(driver.getResponseToClientAsString().isEmpty());;

    }

    @Test
    public void testGetObjectWhenObjectIsInCacheRange()
            throws NoSuchMethodException, SecurityException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final String bucketName = "testBucket";
        final String objectName = "testObject";


        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), objectName, 20000 );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final UUID id1 = dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();
        final BlobCacheService cache = dbSupport.getServiceManager().getService( BlobCacheService.class );

        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 20000 );
        final BlobCache bc1 = BeanFactory.newBean(BlobCache.class).setSizeInBytes(file1.length());
        bc1.setId(UUID.randomUUID());
        bc1.setPath(file1.getPath());
        bc1.setLastAccessed(new Date(System.currentTimeMillis()));
        bc1.setState(CacheEntryState.IN_CACHE);
        bc1.setBlobId(id1);
        dbSupport.getDataManager().createBean( bc1 );

        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheResultInvocationHandler( id1),
                null );
        final StartObjectReadInvocationHandler sorih = new StartObjectReadInvocationHandler(file1);

        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                sorih,
                ih ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                bucketName + "/" + objectName )
                .addParameter(String.valueOf(RequestParameterType.CACHED_ONLY), "true")
                .addHeader(
                        S3HeaderType.BYTE_RANGES, "bytes=0-10,70-80" );

        assertEquals(0,  mockDaoDriver.attain(Blob.class, id1).getByteOffset(), "Shoulda attempted to get offset 0.");
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );
        assertFalse(driver.getResponseToClientAsString().isEmpty());
        assertEquals(1,dbSupport.getServiceManager().getRetriever(Job.class).retrieveAll().toSet().size(), " jobs should be created");
    }

    @Test
    public void testGetObjectWhenObjectMultiBlob()
            throws NoSuchMethodException, SecurityException
    {
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), 2, 100 );

        mockDaoDriver.createJobWithEntries( JobRequestType.GET, blobs );
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();

        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        RetrieveBeansResult<Blob> dbBlobList = dbSupport.getServiceManager().getService(BlobService.class).retrieveAll();
        final UUID id1 = dbBlobList.toList().get(1).getId();

        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final BlobCache bc1 = BeanFactory.newBean(BlobCache.class).setSizeInBytes(file1.length());
        bc1.setId(UUID.randomUUID());
        bc1.setPath(file1.getPath());
        bc1.setLastAccessed(new Date(System.currentTimeMillis()));
        bc1.setState(CacheEntryState.IN_CACHE);
        bc1.setBlobId(id1);
        dbSupport.getDataManager().createBean( bc1 );

        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                null );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                new StartObjectReadInvocationHandler(file1),
                ih ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addParameter( RequestParameterType.OFFSET.toString(), "100" )
                .addHeader(
                        S3HeaderType.BYTE_RANGES,
                        "bytes=100-110, 115-119" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 206 );
        
        final Map< S3HeaderType, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put(
                S3HeaderType.CONTENT_LENGTH,
                "16" );
        requiredHeaders.put(
                S3HeaderType.ACCEPT_BYTE_RANGES,
                "bytes" );
        requiredHeaders.put(
                S3HeaderType.CONTENT_BYTE_RANGES,
                "bytes 100-110,115-119/200" );
        String resp = driver.getResponseToClientAsString();

        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }
    @Test
    public void testGetObjectWhenObjectInCacheMultiBlob()
            throws NoSuchMethodException, SecurityException
    {

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final String bucketName = "testBucket";
        final String objectName = "testObject";


        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), objectName, -1 );
        final List<Blob> blobs = mockDaoDriver.createBlobs( o.getId(), 2, 10 );
        final Blob blob1 = blobs.get(0);
        final Blob blob2 = blobs.get(1);
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        RetrieveBeansResult<Blob> dbBlobList = dbSupport.getServiceManager().getService(BlobService.class).retrieveAll();
        final UUID id1 = dbBlobList.toList().get(1).getId();
        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 10 );
        mockDaoDriver.markBlobInCache(blob2.getId());



        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheResultInvocationHandler(blob2.getId() ),
                null );
        final StartObjectReadInvocationHandler sorih = new StartObjectReadInvocationHandler(file1);

        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                sorih,
                ih ) );



         MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                bucketName + "/" + objectName )
                .addParameter(String.valueOf(RequestParameterType.CACHED_ONLY), "true")
                .addParameter(String.valueOf(RequestParameterType.OFFSET), blob1.getByteOffset() + "");
        driver.run();
        driver.assertHttpResponseCodeEquals( 503 );




        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                bucketName + "/" + objectName )
                .addParameter(String.valueOf(RequestParameterType.CACHED_ONLY), "true")
                .addParameter(String.valueOf(RequestParameterType.OFFSET), blob2.getByteOffset() + "");
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(1,dbSupport.getServiceManager().getRetriever(Job.class).retrieveAll().toSet().size(), " jobs should be created");
    }

    @Test
    public void testGetObjectWhenObjectIsNotInCacheCachedOption()
            throws NoSuchMethodException, SecurityException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final String bucketName = "testBucket";
        final String objectName = "testObject";


        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), objectName, 100 );

        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final UUID id1= dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();


        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheResultInvocationHandler( id1),
                null );
        final StartObjectReadInvocationHandler sorih = new StartObjectReadInvocationHandler();

        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                sorih,
                ih ) );



        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                bucketName + "/" + objectName )
                .addParameter(String.valueOf(RequestParameterType.CACHED_ONLY), "true");
        driver.run();
        driver.assertHttpResponseCodeEquals( 503);
        assertEquals(0,dbSupport.getServiceManager().getRetriever(Job.class).retrieveAll().toSet().size(), "No jobs should be created");;
    }

    @Test
    public void testGetObjectWhenObjectIsNotInCache()
            throws NoSuchMethodException, SecurityException
    {
        final MockCacheFilesystemDriver cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final String bucketName = "testBucket";
        final String objectName = "testObject";


        final User user = mockDaoDriver.createUser( "testUser" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), bucketName );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), objectName, 100 );

        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final UUID id1 = dbSupport.getServiceManager().getService( BlobService.class ).retrieveAll().getFirst().getId();

        final File file1 = cacheFilesystemDriver.writeCacheFile( id1, 100 );
        file1.deleteOnExit();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheResultInvocationHandler( id1),
                null );
        final StartObjectReadInvocationHandler sorih = new StartObjectReadInvocationHandler(file1);

        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobRead", UUID.class, UUID.class ),
                sorih,
                ih ) );



        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                bucketName + "/" + objectName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }



    private static final class StartObjectReadInvocationHandler implements InvocationHandler
    {
        File file;
        public StartObjectReadInvocationHandler(File file) {
            this.file = file;
        }
        public StartObjectReadInvocationHandler() {
            this.file = null;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            if ( m_fails )
            {
                return new MockRpcFailureResponse<>( 
                        new RpcProxyException( "allocate", BeanFactory.newBean( Failure.class ) ) );
            }
            
            final String file = Paths.get(
                    System.getProperty( "java.io.tmpdir" ),
                    toStringOrEmpty( args[0] ),
                    toStringOrEmpty( args[1] ) ).toString();
            if (this.file != null) {
                final byte FILL_VALUE = (byte) 'A';
                int arraySize = (int) this.file.length();
                byte[] dataToWrite = new byte[arraySize];
                Arrays.fill(dataToWrite, FILL_VALUE);
                Files.write(
                       this.file.toPath(),
                        dataToWrite,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING // Overwrite if file exists
                );
                Long size =  this.file.length();
                System.out.println("Wrote " + arraySize + " bytes to " + this.file.length() + " bytes");
                return new RpcResponse<>( BeanFactory.newBean( DiskFileInfo.class ).setFilePath(this.file.getPath()) );
            } else {
                FileUtils.writeByteArrayToFile( new File(file), getTestRequestPayload() );
                return new RpcResponse<>( BeanFactory.newBean( DiskFileInfo.class ).setFilePath( file ) );
            }


        }
        
        private volatile boolean m_fails;
    }// end class def
    
    
    private static String toStringOrEmpty(Object value)
    {
        return (value == null) ? "" : value.toString();
    }
    
    
    private static byte[] getTestRequestPayload() throws UnsupportedEncodingException
    {
        return "This is the request payload.".getBytes("UTF-8");
    }

    private static DatabaseSupport dbSupport;
    private static MockCacheFilesystemDriver cacheFilesystemDriver;

    @BeforeAll
    public static void setDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        cacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport, 1024 * 1024 );
    }

    @BeforeEach
    public void setUp() {
        dbSupport.reset();
    }
}
