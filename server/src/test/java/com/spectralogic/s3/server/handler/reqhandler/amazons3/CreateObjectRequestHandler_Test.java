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
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.dispatch.RequestDispatcherImpl;
import com.spectralogic.s3.server.mock.GetBlobsInCacheEmptyResultInvocationHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.security.ChecksumType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class CreateObjectRequestHandler_Test
{
    @Test
    public void testPutObjectRequestWithoutContentLengthHttpHeaderNotAllowed()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );

        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .setRequestPayload( requestPayload );
        driver.addHeader( S3HeaderType.CONTENT_LENGTH, null );
        driver.run();
        driver.assertHttpResponseCodeEquals( 411 );
    }


    @Test
    public void testPutObjectRequestWithoutChecksumReturns200OK()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.createCacheFilesystem();

        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertTrue(job.isNaked(), "Shoulda been a naked job since job was created implicitly.");
        assertTrue(job.isImplicitJobIdResolution(), "Job created shoulda been naked and permitted implicit job id resolution.");
    }


    @Test
    public void testPutObjectThatGetsDeletedDuringDataTransmissionReturns410Gone()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.createCacheFilesystem();

        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final InvocationHandler originalDpIh = support.getPlannerInterfaceIh();
        final InvocationHandler dpIh = new InvocationHandler()
        {
            public Object invoke( final Object proxy, final Method method, final Object[] args )
                    throws Throwable
            {
                final Object retval = originalDpIh.invoke( proxy, method, args );
                if ( "startBlobWrite".equals( method.getName() ) )
                {
                    mockDaoDriver.deleteAll( Blob.class );
                }
                return retval;
            }
        };
        support.setPlannerInterfaceIh( dpIh );

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 410 );
    }


    @Test
    public void testPutObjectRequestWithByteRangeHeaderReturns400BadRequest()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .addHeader(
                        S3HeaderType.BYTE_RANGES,
                        "0-0" )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testPutObjectRequestWithCorrectCrcChecksumReturns200OK()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        support.getDatabaseSupport().getServiceManager().getService( DataPolicyService.class ).update(
                dataPolicy.setChecksumType( ChecksumType.CRC_32 ),
                DataPolicy.CHECKSUM_TYPE );

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        "Content-CRC32",
                        "LIdgzQ==" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testPutObjectRequestWithCorrectMd5ChecksumReturns200OK()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        "Content-MD5",
                        "8Dn/GQtZ78RR+slAJGUjuA==" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testPutObjectRequestWithCorrectSha256ChecksumReturns200OK()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        support.getDatabaseSupport().getServiceManager().getService( DataPolicyService.class ).update(
                dataPolicy.setChecksumType( ChecksumType.SHA_256 ),
                DataPolicy.CHECKSUM_TYPE );

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        "Content-SHA256",
                        "xGkbhZdJ7OoJvOgQzYdyXrSCTfXu3DQDopiHitUwpw4=" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testPutObjectRequestWithoutChecksumNotAllowedWhenEndToEndCrcRequired()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();
        support.getDatabaseSupport().getServiceManager().getService( DataPolicyService.class ).update(
                dataPolicy.setEndToEndCrcRequired( true ),
                DataPolicy.END_TO_END_CRC_REQUIRED );

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testPutObjectRequestChecksumNotSameAsCrcConfiguredOnBucketNotAllowed()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();
        support.getDatabaseSupport().getServiceManager().getService( DataPolicyService.class ).update(
                dataPolicy.setChecksumType( ChecksumType.MD5 ),
                DataPolicy.CHECKSUM_TYPE );

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        "Content-CRC32",
                        "AAAAACyHYM0=" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testPutObjectRequestWithIncorrectChecksumReturns400BadDigest()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        RequestDispatcherImpl.disableSleepsFor307s();
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createCacheFilesystem();
        setUpBucket( support );

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_MD5,
                        "U6DBrxQ9BE2sRdnrg8zScA==" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( "BadDigest" );
    }


    @Test
    public void testPutFolderAsPartOfWrongJobNotAllowed() throws SecurityException, NoSuchMethodException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "folder_name/", -1 );
        final Blob blob = mockDaoDriver.createBlobs( o.getId(), 1, 0 ).get( 0 );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "folder_name2/", -1 );
        final Blob blob2 = mockDaoDriver.createBlobs( o2.getId(), 1, 0 ).get( 0 );
        final UUID jobId1 = mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId();
        final UUID jobId2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId();
        mockDaoDriver.createJobEntries(jobId1,
                CollectionFactory.toSet( blob ) );
        mockDaoDriver.createJobEntries( jobId2,
                CollectionFactory.toSet( blob2 ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/folder_name/" )
                .addParameter( RequestParameterType.JOB.toString(), jobId2.toString() )
                .addHeader( S3HeaderType.CONTENT_LENGTH, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }


    @Test
    public void testPutFolderAsPartOfJobWorks() throws SecurityException, NoSuchMethodException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "folder_name/", -1 );
        final Blob blob = mockDaoDriver.createBlobs( o.getId(), 1, 0 ).get( 0 );
        final UUID jobId = mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId();
        mockDaoDriver.createJobEntries( jobId,
                CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/folder_name/" )
                .addParameter( RequestParameterType.JOB.toString(), jobId.toString() )
                .addHeader( S3HeaderType.CONTENT_LENGTH, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(
                job.isNaked(),
                "Should notta been a naked job since job was created explicitly."
        );
    }


    @Test
    public void testPutFolderWithMetadataAllowed() throws SecurityException, NoSuchMethodException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "x-amz-meta-test", "this value tests custom metadata" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/folder_name/" ).addHeader( S3HeaderType.CONTENT_LENGTH, "0" );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();

        final UUID objectId = serviceManager.getService( S3ObjectService.class )
                .attainId( "bucket_name", "folder_name/", null, true );
        final Set< S3ObjectProperty > objectProperties = serviceManager
                .getRetriever( S3ObjectProperty.class )
                .retrieveAll( S3ObjectProperty.OBJECT_ID, objectId ).toSet();

        propertiesMapping.remove( "x-invalid" );
        final Method methodBlobWriteCompleted =
                ReflectUtil.getMethod( DataPlannerResource.class, "blobWriteCompleted" );
        assertEquals(0,  objectProperties.size(), "Shoulda delegated to data planner to create object properties.");
        assertEquals(propertiesMapping.size(),  ((S3ObjectProperty[]) support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted).get(0).getArgs().get(5)).length, "Shoulda delegated to data planner to create object properties.");
        assertNotNull(( (S3ObjectProperty[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted ).get( 0 ).getArgs().get( 5 ) )[ 0 ].getId(), "Shoulda delegated to data planner to create object properties.");
        assertNotNull(( (S3ObjectProperty[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted ).get( 0 ).getArgs().get( 5 ) )[ 0 ].getKey(), "Shoulda delegated to data planner to create object properties.");
        assertNotNull(( (S3ObjectProperty[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted ).get( 0 ).getArgs().get( 5 ) )[ 0 ].getObjectId(), "Shoulda delegated to data planner to create object properties.");
        assertNotNull(( (S3ObjectProperty[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted ).get( 0 ).getArgs().get( 5 ) )[ 0 ].getValue(), "Shoulda delegated to data planner to create object properties.");
    }


    @Test
    public void testPutFolderTwiceNotAllowed() throws SecurityException, NoSuchMethodException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/folder_name/" ).addHeader( S3HeaderType.CONTENT_LENGTH, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        support.getDatabaseSupport().getDataManager().updateBeans(
                CollectionFactory.toSet( ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE ),
                BeanFactory.newBean( Blob.class ).setChecksum( "aa" ).setChecksumType( ChecksumType.CRC_32 ),
                Require.nothing() );

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/folder_name/" ).addHeader( S3HeaderType.CONTENT_LENGTH, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( GenericFailure.CONFLICT.getHttpResponseCode() );
    }


    @Test
    public void testPutObjectTwiceWhenObjectVersioningNoneNotAllowed()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final byte[] requestPayload = getTestRequestPayload();
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        support.getDatabaseSupport().getDataManager().updateBeans(
                CollectionFactory.toSet( ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE ),
                BeanFactory.newBean( Blob.class ).setChecksum( "aa" ).setChecksumType( ChecksumType.CRC_32 ),
                Require.nothing() );

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( GenericFailure.CONFLICT.getHttpResponseCode() );
    }


    @Test
    public void testPutObjectTwiceWhenObjectVersioningKeepLatestIsAllowed()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPolicy.class ).setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final byte[] requestPayload = getTestRequestPayload();
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                    .addHeader(
                            S3HeaderType.CONTENT_LENGTH,
                            Integer.toString( requestPayload.length ) )
                    .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        support.getDatabaseSupport().getDataManager().updateBeans(
                CollectionFactory.toSet( ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE ),
                BeanFactory.newBean( Blob.class ).setChecksum( "aa" ).setChecksumType( ChecksumType.CRC_32 ),
                Require.nothing() );

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                    .addHeader(
                            S3HeaderType.CONTENT_LENGTH,
                            Integer.toString( requestPayload.length ) )
                    .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testPutObjectAfterObjectAlreadyPersistedNotAllowed()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createObject( bucket.getId(), "object_name" );
        support.getDatabaseSupport().getDataManager().updateBeans(
                CollectionFactory.toSet( ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE ),
                BeanFactory.newBean( Blob.class ).setChecksum( "aa" ).setChecksumType( ChecksumType.CRC_32 ),
                Require.nothing() );

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( GenericFailure.CONFLICT.getHttpResponseCode() );
    }


    @Test
    public void testPutObjectWhenNotAllowingNewJobRequestsFails()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.updateBean( support.getDatabaseSupport()
                .getServiceManager()
                .getRetriever( DataPathBackend.class )
                .attain( Require.nothing() )
                .setAllowNewJobRequests( false ), DataPathBackend.ALLOW_NEW_JOB_REQUESTS );
        final User user = mockDaoDriver.createUser( "jason" );
        support.getDatabaseSupport()
                .getServiceManager()
                .getRetriever( DataPathBackend.class )
                .attain( Require.nothing() )
                .setAllowNewJobRequests( false );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockUserAuthorizationStrategy( "jason" ), RequestType.PUT,
                        "bucket_name/object_name" ).addHeader( S3HeaderType.CONTENT_LENGTH,
                                Integer.toString( requestPayload.length ) )
                        .setRequestPayload( requestPayload );
        support.getDatabaseSupport()
                .getServiceManager()
                .getRetriever( DataPathBackend.class )
                .attain( Require.nothing() )
                .setAllowNewJobRequests( false );
        driver.run();
        driver.assertHttpResponseCodeEquals( GenericFailure.FORBIDDEN.getHttpResponseCode() );
    }


    @Test
    public void testPutObjectRequestWithIncorrectContentLengthReturns400BadRequest()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createCacheFilesystem();
        setUpBucket( support );

        final byte[] requestPayload = getTestRequestPayload();
        final int incorrectContentLength = requestPayload.length + 123;
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name/object_name" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( incorrectContentLength ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testPutObjectRequestSavesMetadata()
            throws UnsupportedEncodingException, NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final String bucketName = "testBucket";
        final String objectName = "testObject";

        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "x-amz-meta-test", "this value tests custom metadata" );
        propertiesMapping.put( "x-amz-meta-another", "more than one metadata field can be set" );
        propertiesMapping.put( "x-invalid", "should be ignored due to bad prefix" );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.createBucket( user.getId(), bucketName );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                bucketName + "/" + objectName )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();

        final UUID objectId = serviceManager.getService( S3ObjectService.class )
                .attainId( bucketName, objectName, null, true );
        final Set< S3ObjectProperty > objectProperties = serviceManager
                .getRetriever( S3ObjectProperty.class )
                .retrieveAll( S3ObjectProperty.OBJECT_ID, objectId ).toSet();

        propertiesMapping.remove( "x-invalid" );
        final Method methodBlobWriteCompleted =
                ReflectUtil.getMethod( DataPlannerResource.class, "blobWriteCompleted" );
        assertEquals(0,  objectProperties.size(), "Shoulda delegated to data planner to create object properties.");
        assertEquals(propertiesMapping.size(),  ((S3ObjectProperty[]) support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted).get(0).getArgs().get(5)).length, "Shoulda delegated to data planner to create object properties.");
        for ( final S3ObjectProperty objectProperty
                : ( (S3ObjectProperty[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted ).get( 0 ).getArgs().get( 5 ) ) )
        {
            final String metadataEntryValue = propertiesMapping.get( objectProperty.getKey() );
            final String message1 = "Shoulda not had extraneous property '" + objectProperty.getKey() + "'.";
            assertNotNull(metadataEntryValue, message1);
            final String message = "Shoulda had the same value for property '" + objectProperty.getKey() + "'.";
            assertEquals(metadataEntryValue, objectProperty.getValue(), message);
        }
    }


    @Test
    public void testPutObjectPartOfReplicatingJobAppliesAdditionalValidationsToRequest()
            throws UnsupportedEncodingException, NoSuchMethodException, SecurityException
    {
        final Boolean [] blobWriteCompletedResult = new Boolean [] { null };
        final MockHttpRequestSupport support = buildHttpRequestSupport( blobWriteCompletedResult );
        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "x-amz-meta-test", "this value tests custom metadata" );
        propertiesMapping.put( "x-amz-meta-another", "more than one metadata field can be set" );
        propertiesMapping.put( "x-invalid", "should be ignored due to bad prefix" );
        propertiesMapping.put( "etag", "should not be ignored" );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.createCacheFilesystem();

        final byte[] requestPayload = getTestRequestPayload();
        final S3Object o = mockDaoDriver.createObjectStub( null, "o1", requestPayload.length );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Job job = mockDaoDriver.createJob( o.getBucketId(), user.getId(), JobRequestType.PUT );
        mockDaoDriver.updateBean( job.setReplicating( true ), Job.REPLICATING );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPolicy.class ).setChecksumType( ChecksumType.CRC_32 ),
                DataPolicy.CHECKSUM_TYPE );
        mockDaoDriver.createJobEntries( job.getId(),
                CollectionFactory.toSet( blob ) );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final long creationDate = new Date().getTime();

        // No end-to-end CRC, with null result
        blobWriteCompletedResult[ 0 ] = null;
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload )
                .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
                .addHeader( S3HeaderType.OBJECT_CREATION_DATE, String.valueOf( creationDate ) );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        // No object creation date, but null result to simulate no error since not completed yet
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload )
                .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
                .addHeader( "Content-CRC32", "LIdgzQ==" );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        // No object creation date, but FALSE result to simulate no object creation date upon completion
        blobWriteCompletedResult[ 0 ] = Boolean.FALSE;
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload )
                .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
                .addHeader( "Content-CRC32", "LIdgzQ==" );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        // No end-to-end CRC
        blobWriteCompletedResult[ 0 ] = Boolean.TRUE;
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload )
                .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
                .addHeader( S3HeaderType.OBJECT_CREATION_DATE, String.valueOf( creationDate ) );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        // No object creation date, but TRUE result to simulate there was an object creation date
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload )
                .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
                .addHeader( "Content-CRC32", "LIdgzQ==" );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        // No object metadata
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload )
                .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
                .addHeader( S3HeaderType.OBJECT_CREATION_DATE, String.valueOf( creationDate ) )
                .addHeader( "Content-CRC32", "LIdgzQ==" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        // Correct invocation
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + o.getName() )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload )
                .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
                .addHeader( S3HeaderType.OBJECT_CREATION_DATE, String.valueOf( creationDate ) )
                .addHeader( "Content-CRC32", "LIdgzQ==" );
        for ( final Map.Entry< String, String > propertyEntry : propertiesMapping.entrySet() )
        {
            driver.addHeader( propertyEntry.getKey(), propertyEntry.getValue() );
        }
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();

        final Set< S3ObjectProperty > objectProperties = serviceManager
                .getRetriever( S3ObjectProperty.class )
                .retrieveAll( S3ObjectProperty.OBJECT_ID, o.getId() ).toSet();

        propertiesMapping.remove( "x-invalid" );
        final Method methodBlobWriteCompleted =
                ReflectUtil.getMethod( DataPlannerResource.class, "blobWriteCompleted" );
        assertEquals(0,  objectProperties.size(), "Shoulda delegated to data planner to create object properties.");
        assertEquals(propertiesMapping.size(),  ((S3ObjectProperty[]) support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted).get(0).getArgs().get(5)).length, "Shoulda delegated to data planner to create object properties.");
        for ( final S3ObjectProperty objectProperty
                : ( (S3ObjectProperty[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                methodBlobWriteCompleted ).get( 0 ).getArgs().get( 5 ) ) )
        {
            final String metadataEntryValue =
                    ( S3HeaderType.ETAG.getHttpHeaderName().equals( objectProperty.getKey() ) ) ?
                            propertiesMapping.get( "etag" )
                            : propertiesMapping.get( objectProperty.getKey() );
            final String message1 = "Shoulda not had extraneous property '" + objectProperty.getKey() + "'.";
            assertNotNull(metadataEntryValue, message1);
            final String message = "Shoulda had the same value for property '" + objectProperty.getKey() + "'.";
            assertEquals(metadataEntryValue, objectProperty.getValue(), message);
        }
    }


    @Test
    public void testPutObjectRequestCausesDataPolicyToBeIncompatibleWithFullLtfsCompWhenObjectNameIsTooLong()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final DataPolicyService dataPolicyService =
                support.getDatabaseSupport().getServiceManager().getService( DataPolicyService.class );
        assertTrue(dataPolicyService.areStorageDomainsWithObjectNamingAllowed(
                mockDaoDriver.attainOneAndOnly( DataPolicy.class ) ), "Shoulda initialized data policy as allowing FULL ltfs compatibility.");
        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                "bucket_name" + StringUtils.repeat( "/123456789", 103 ) )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertFalse(
                dataPolicyService.areStorageDomainsWithObjectNamingAllowed(
                        mockDaoDriver.attainOneAndOnly( DataPolicy.class ) ),
                "Shoulda marked data policy as not allowing FULL ltfs compatibility."
        );
    }


    @Test
    public void testPutObjectRequestReturns200OKWhenProvidedInternationalObjectName()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final String bucketName = "bucket_name";
        final String objectName =
                "%E9%A4%8A%E6%89%B1%E9%87%8F%E5%A4%A7/%E8%B2%A0%E6%96%87%E5%AE%87%E9%96%89";

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), bucketName );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();

        final byte[] requestPayload = getTestRequestPayload();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT,
                bucketName + "/" + objectName )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testPutObjectRequestReturns200WhenMetadataSuppliedForBothBlobsInTwoBlobObject()
            throws NoSuchMethodException, SecurityException, UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = buildHttpRequestSupport();
        final byte[] requestPayload = getTestRequestPayload();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.createCacheFilesystem();
        final UUID jobId = mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId();
        mockDaoDriver.createJobEntry( jobId );
        mockDaoDriver.createJobEntries(
                mockDaoDriver.createBlobs( null, 2, requestPayload.length ) );

        sendJobObjectPartWithMetadata( support, requestPayload, jobId, 200, 0L );
        sendJobObjectPartWithMetadata( support, requestPayload, jobId, 200, requestPayload.length );
        assertEquals(0,  support.getDatabaseSupport().getServiceManager().getRetriever(
                S3ObjectProperty.class).getCount(), "Should notta created any object metadata.");
    }


    private static void sendJobObjectPartWithMetadata(
            final MockHttpRequestSupport support,
            final byte[] requestPayload,
            final UUID jobId,
            final int expectedResponseCode,
            final long offset )
    {
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/" + MockDaoDriver.DEFAULT_OBJECT_NAME )
                .addParameter( "job", jobId.toString() )
                .addParameter( "offset", Long.toString( offset ) )
                .addHeader( "x-amz-meta-foo", "bar" )
                .addHeader(
                        S3HeaderType.CONTENT_LENGTH,
                        Integer.toString( requestPayload.length ) )
                .setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( expectedResponseCode );
    }


    private MockHttpRequestSupport buildHttpRequestSupport()
            throws NoSuchMethodException, SecurityException
    {
        return buildHttpRequestSupport( new Boolean [] { null } );
    }


    private MockHttpRequestSupport buildHttpRequestSupport( final Boolean [] blobWriteCompletedResult )
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "startBlobWrite", UUID.class, UUID.class ),
                new StartBlobWriteInvocationHandler(),
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( DataPlannerResource.class, "blobWriteCompleted" ),
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object arg0,
                                    final Method arg1,
                                    final Object[] arg2 ) throws Throwable
                            {
                                return new RpcResponse<>( blobWriteCompletedResult[ 0 ] );
                            }
                        },
                        null ) );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                DataPlannerResource.class.getMethod( "getBlobsInCache", UUID[].class ),
                new GetBlobsInCacheEmptyResultInvocationHandler(),
                ih ) );

        return support;
    }


    private void setUpBucket( final MockHttpRequestSupport support )
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.createBucket( user.getId(), "bucket_name" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
    }


    private static final class StartBlobWriteInvocationHandler implements InvocationHandler
    {
        public Object invoke( final Object proxy, final Method method, final Object[] args )
                throws Throwable
        {
            final String tmpDirPath = Paths.get(
                    System.getProperty( "java.io.tmpdir" ),
                    toStringOrEmpty( args[0] ) ).toString();
            String filePath = Paths.get(
                    System.getProperty( "java.io.tmpdir" ),
                    toStringOrEmpty( args[0] ),
                    toStringOrEmpty( args[1] ) ).toString();

            if ( filePath.endsWith( Platform.FILE_SEPARATOR ) )
            {
                filePath = filePath.substring( 0, filePath.length() - 1 );
            }
            final File tmpDir = new File( tmpDirPath );
            final File file = new File( filePath );
            FileUtils.writeByteArrayToFile( file, getTestRequestPayload() );
            tmpDir.deleteOnExit();
            file.deleteOnExit();
            return new RpcResponse<>( filePath );
        }
    }// end class def


    private static String toStringOrEmpty(Object value)
    {
        return (value == null) ? "" : value.toString();
    }


    private static byte[] getTestRequestPayload() throws UnsupportedEncodingException
    {
        return "This is the request payload that we are verifying the checksum for.".getBytes("UTF-8");
    }
}
