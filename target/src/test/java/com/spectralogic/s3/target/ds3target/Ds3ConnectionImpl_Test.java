/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import com.spectralogic.ds3client.commands.DeleteObjectsRequest;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.Group;
import com.spectralogic.ds3client.models.JobChunkBlobStoreState;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.models.ChecksumType.Type;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.delete.DeleteObject;
import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetAccessControlReplication;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public final class Ds3ConnectionImpl_Test
{
    @Test
    public void testConstructorFailsWhenCannotConnect()
    {
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "authid", "key" );
            }
        } );
    }

    @Test
    public void testConstructorFailsWhenCannotConnectAndTargetPortSpecified()
    {
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" ).setDataPathPort( Integer.valueOf( 888 ) );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "authid", "key" );
            }
        } );
    }

    @Test
    public void testConstructorNullTargetNotAllowed()
    {
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), null, "authid", "key" );
            }
        } );
    }

    @Test
    public void testConstructorNullAuthIdNotAllowed()
    {
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, null, "key" );
            }
        } );
    }

    @Test
    public void testConstructorNullAuthKeyNotAllowed()
    {
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "authid", (String)null );
            }
        } );
    }

    @Test
    public void testSuccessfulConnectionWhenInstanceIdWasNull() {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(instanceId, target.getId(), "Shoulda returned correct instance id.");
    }

    @Test
    public void testFailedConnectionWhenVersionMismatch() {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        systemInformation.getBuildInformation().setVersion( "invalid" );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
            }
        } );
    }

    @Test
    public void testSuccessfulConnectionWhenRevisionMismatch() {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        systemInformation.getBuildInformation().setRevision( "invalid" );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
    }
    
    @Test
    public void testFailedConnectionWhenConnectingToManagementPath() {
        final MockDs3Client client = new MockDs3Client();
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( "Spectra-Data-Path-Request-Made-On-Management-Path", "something" );
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        systemInformation.getBuildInformation().setRevision( "invalid" );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                404,
                headers );
        
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Throwable t = TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
            }
        } );
        assertTrue(t.getMessage().contains( "management path" ), "Shoulda said something about connecting to the management path by mistake.");
    }

    @Test
    public void testFailedConnectionWhen403() {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        systemInformation.getBuildInformation().setRevision( "invalid" );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                403,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 403 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
            }
        } );
    }

    @Test
    public void testSuccessfulConnectionButClockSkewTooGreatNotAllowed() {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        systemInformation.setNow( System.currentTimeMillis() - 3600 * 1000 );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
            }
        } );

        systemInformation.setNow( System.currentTimeMillis() - 60 * 1000 );
        new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );

        systemInformation.setNow( System.currentTimeMillis() + 60 * 1000 );
        new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
    }

    @Test
    public void testSuccessfulConnectionWhenInstanceIdWasNonNull() {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        target.setId( instanceId );
        new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(instanceId, target.getId(), "Shoulda returned correct instance id.");
    }

    @Test
    public void testSuccessfulConnectionWhenInstanceIdInConflictNotAllowed() {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        target.setId( UUID.randomUUID() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
            }
        } );
    }

    @Test
    public void testSuccessfulConnectionWithNon200ResponseNotAllowed() {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                503,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 503 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
            }
        } );
    }

    @Test
    public void testVerifyIsAdministratorWith200ResponseAllowed() {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final Group group = new Group();
        client.setResponse(
                VerifyUserIsMemberOfGroupSpectraS3Request.class,
                group,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(instanceId, target.getId(), "Shoulda returned correct instance id.");

        connection.verifyIsAdministrator();
    }

    @Test
    public void testVerifyIsAdministratorWithNon200ResponseNotAllowed()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                VerifyUserIsMemberOfGroupSpectraS3Request.class,
                null,
                204,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(instanceId, target.getId(), "Shoulda returned correct instance id.");

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.verifyIsAdministrator();
            }
        } );
    }

    @Test
    public void testVerifyIsAdministratorWithNon200RequestFailedResponseNotAllowed()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                VerifyUserIsMemberOfGroupSpectraS3Request.class,
                null,
                404,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(instanceId, target.getId(), "Shoulda returned correct instance id.");

        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 404 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.verifyIsAdministrator();
            }
        } );
    }


    //NOTE: this function should be private instead of public and not individually tested
    //However, we currently have no means of mocking our local version info, so we do this
    //instead of failing to test properly.
    @Test
    public void testMatchMajorVersionsWorksAsIntended()
    {
        assertTrue( Ds3ConnectionImpl.majorVersionsMatch( "3.5", "3.2" ) );
        assertTrue( Ds3ConnectionImpl.majorVersionsMatch( "3.2", "3.5" ) );
        assertTrue( Ds3ConnectionImpl.majorVersionsMatch( "3.2", "3.5.2" ) );
        assertTrue( Ds3ConnectionImpl.majorVersionsMatch( "fake", "fake" ) );
        assertFalse( Ds3ConnectionImpl.majorVersionsMatch( "4.2", "3.2" ) );
        assertFalse( Ds3ConnectionImpl.majorVersionsMatch( "x", "y" ) );
    }

    @Test
    public void testGetDataPoliciesDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final com.spectralogic.ds3client.models.DataPolicy dp1 =
                new com.spectralogic.ds3client.models.DataPolicy();
        dp1.setId( UUID.randomUUID() );
        dp1.setName( "name1" );
        dp1.setAlwaysForcePutJobCreation( true );
        dp1.setChecksumType( Type.MD5 );
        dp1.setMaxVersionsToKeep( 42 );

        final com.spectralogic.ds3client.models.DataPolicy dp2 =
                new com.spectralogic.ds3client.models.DataPolicy();
        dp2.setId( UUID.randomUUID() );
        dp2.setName( "name2" );
        dp2.setAlwaysForcePutJobCreation( false );
        dp2.setChecksumType( Type.SHA_512 );
        dp2.setMaxVersionsToKeep( 5 );

        final DataPolicyList dataPolicies = new DataPolicyList();
        dataPolicies.setDataPolicies( CollectionFactory.toList( dp1, dp2 ) );
        client.setResponse(
                GetDataPoliciesSpectraS3Request.class,
                dataPolicies,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(instanceId, target.getId(), "Shoulda returned correct instance id.");

        final List< DataPolicy > daoDataPolicies = new ArrayList<>( connection.getDataPolicies() );
        assertEquals(2, daoDataPolicies.size(), "Shoulda unmarshaled data policies correctly.");
        final DataPolicy daoDataPolicy1;
        final DataPolicy daoDataPolicy2;
        if ( "name1".equals( daoDataPolicies.get( 0 ).getName() ) )
        {
            daoDataPolicy1 = daoDataPolicies.get( 0 );
            daoDataPolicy2 = daoDataPolicies.get( 1 );
        }
        else
        {
            daoDataPolicy2 = daoDataPolicies.get( 0 );
            daoDataPolicy1 = daoDataPolicies.get( 1 );
        }

        assertEquals(dp1.getName(), daoDataPolicy1.getName(), "Shoulda populated data policies correctly.");
        assertEquals(dp1.getId(), daoDataPolicy1.getId(), "Shoulda populated data policies correctly.");
        assertEquals(dp1.getAlwaysForcePutJobCreation(), daoDataPolicy1.isAlwaysForcePutJobCreation(), "Shoulda populated data policies correctly.");
        assertEquals(dp1.getChecksumType().toString(), daoDataPolicy1.getChecksumType().toString(), "Shoulda populated data policies correctly.");

        assertEquals(dp2.getName(), daoDataPolicy2.getName(), "Shoulda populated data policies correctly.");
        assertEquals(dp2.getId(), daoDataPolicy2.getId(), "Shoulda populated data policies correctly.");
        assertEquals(dp2.getAlwaysForcePutJobCreation(), daoDataPolicy2.isAlwaysForcePutJobCreation(), "Shoulda populated data policies correctly.");
        assertEquals(dp2.getChecksumType().toString(), daoDataPolicy2.getChecksumType().toString(), "Shoulda populated data policies correctly.");
    }

    @Test
    public void testGetUsersDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final SpectraUser user1 = new SpectraUser();
        user1.setAuthId( "authId1" );
        user1.setDefaultDataPolicyId( UUID.randomUUID() );
        user1.setId( UUID.randomUUID() );
        user1.setName( "name1" );
        user1.setSecretKey( "secret1" );

        final SpectraUser user2 = new SpectraUser();
        user2.setAuthId( "authId2" );
        user2.setDefaultDataPolicyId( null );
        user2.setId( UUID.randomUUID() );
        user2.setName( "name2" );
        user2.setSecretKey( "secret2" );

        final SpectraUserList users = new SpectraUserList();
        users.setSpectraUsers( CollectionFactory.toList( user1, user2 ) );
        client.setResponse(
                GetUsersSpectraS3Request.class,
                users,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(instanceId, target.getId(), "Shoulda returned correct instance id.");

        final List< User > daoUsers = new ArrayList<>( connection.getUsers() );
        assertEquals(2, daoUsers.size(), "Shoulda unmarshaled users correctly.");
        final User daoUser1;
        final User daoUser2;
        if ( "name1".equals( daoUsers.get( 0 ).getName() ) )
        {
            daoUser1 = daoUsers.get( 0 );
            daoUser2 = daoUsers.get( 1 );
        }
        else
        {
            daoUser2 = daoUsers.get( 0 );
            daoUser1 = daoUsers.get( 1 );
        }

        assertEquals(user1.getName(), daoUser1.getName(), "Shoulda populated users correctly.");
        assertEquals(user1.getAuthId(), daoUser1.getAuthId(), "Shoulda populated users correctly.");
        assertEquals(user1.getSecretKey(), daoUser1.getSecretKey(), "Shoulda populated users correctly.");
        assertEquals(user1.getId(), daoUser1.getId(), "Shoulda populated users correctly.");
        assertEquals(user1.getDefaultDataPolicyId(), daoUser1.getDefaultDataPolicyId(), "Shoulda populated users correctly.");

        assertEquals(user2.getName(), daoUser2.getName(), "Shoulda populated users correctly.");
        assertEquals(user2.getAuthId(), daoUser2.getAuthId(), "Shoulda populated users correctly.");
        assertEquals(user2.getSecretKey(), daoUser2.getSecretKey(), "Shoulda populated users correctly.");
        assertEquals(user2.getId(), daoUser2.getId(), "Shoulda populated users correctly.");
        assertEquals(user2.getDefaultDataPolicyId(), daoUser2.getDefaultDataPolicyId(), "Shoulda populated users correctly.");
    }

    @Test
    public void testCreateUserWithoutDataPolicyDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final User userArg = BeanFactory.newBean( User.class );
        userArg.setAuthId( "aid" );
        userArg.setId( UUID.randomUUID() );
        userArg.setName( "name" );
        userArg.setSecretKey( "secretkey" );

        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new com.spectralogic.ds3client.models.SpectraUser(),
                201,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.createUser( userArg, null );

        final DelegateCreateUserSpectraS3Request request =
                client.getRequest( DelegateCreateUserSpectraS3Request.class );
        client.verifyRequestMatchesDao( User.class, userArg, request );
    }

    @Test
    public void testCreateUserWithDataPolicyDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final User userArg = BeanFactory.newBean( User.class );
        userArg.setAuthId( "aid" );
        userArg.setId( UUID.randomUUID() );
        userArg.setName( "name" );
        userArg.setSecretKey( "secretkey" );

        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new com.spectralogic.ds3client.models.SpectraUser(),
                201,
                null );

        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new com.spectralogic.ds3client.models.SpectraUser(),
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.createUser( userArg, "dp" );

        final DelegateCreateUserSpectraS3Request request =
                client.getRequest( DelegateCreateUserSpectraS3Request.class );
        client.verifyRequestMatchesDao( User.class, userArg, request );

        final ModifyUserSpectraS3Request modifyRequest =
                client.getRequest( ModifyUserSpectraS3Request.class );
        assertEquals("dp", modifyRequest.getDefaultDataPolicyId(), "Shoulda modified user dp to match request.");
    }

    @Test
    public void testUpdateUserUpdatesSecretKey()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final SpectraUser user1 = new SpectraUser();
        user1.setAuthId( "authId1" );
        user1.setDefaultDataPolicyId( UUID.randomUUID() );
        user1.setId( UUID.randomUUID() );
        user1.setName( "name1" );
        user1.setSecretKey( "secret1" );
        user1.setMaxBuckets( 666 );

        client.setResponse(
                ModifyUserSpectraS3Request.class,
                user1,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.updateUser( (User)BeanFactory.newBean( User.class )
                .setName( user1.getName() ).setSecretKey( user1.getSecretKey() ).setMaxBuckets( user1.getMaxBuckets() ).setId(
                        user1.getId() ) );

        final ModifyUserSpectraS3Request request =
                client.getRequest( ModifyUserSpectraS3Request.class );
        assertEquals(user1.getName(), request.getUserId(), "Shoulda modified user.");
        assertTrue(request.getPath().contains( user1.getName() ), "Shoulda modified user.");
        assertEquals(2, request.getQueryParams().size(), "Should of modified secret key and max buckets.");
        assertTrue(request.getQueryParams().keySet().contains( "secret_key" ), "Should of modified secret key.");
        assertTrue(request.getQueryParams().values().contains( user1.getSecretKey() ), "Should of modified secret key.");
        assertTrue(request.getQueryParams().keySet().contains( "max_buckets" ), "Should of modified max buckets.");
        assertTrue(request.getQueryParams().values().contains( user1.getMaxBuckets() + "" ), "Should of modified max buckets.");
    }

    @Test
    public void testDeleteUserDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                DelegateDeleteUserSpectraS3Request.class,
                null,
                204,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.deleteUser( "bob" );

        final DelegateDeleteUserSpectraS3Request request =
                client.getRequest( DelegateDeleteUserSpectraS3Request.class );
        assertEquals("bob", request.getUserId(), "Shoulda deleted user.");
    }

    @Test
    public void testCreateTargetDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final Ds3Target targetArg = BeanFactory.newBean( Ds3Target.class );
        targetArg.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS );
        targetArg.setAdminAuthId( "aid" );
        targetArg.setAdminSecretKey( "ask" );
        targetArg.setDataPathEndPoint( "dpep" );
        targetArg.setId( UUID.randomUUID() );
        targetArg.setName( "name" );
        targetArg.setDataPathPort( Integer.valueOf( 80 ) );
        targetArg.setReplicatedUserDefaultDataPolicy( "dp" );

        client.setResponse(
                RegisterDs3TargetSpectraS3Request.class,
                new com.spectralogic.ds3client.models.Ds3Target(),
                201,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.createDs3Target( targetArg );

        final RegisterDs3TargetSpectraS3Request request =
                client.getRequest( RegisterDs3TargetSpectraS3Request.class );
        client.verifyRequestMatchesDao( Ds3Target.class, targetArg, request );
    }

    @Test
    public void testDeleteObjectsDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                DeleteObjectsRequest.class,
                new DeleteResult(),
                200,
                null );

        final Bucket bucket = BeanFactory.newBean( Bucket.class );
        bucket.setName( "b1" );
        bucket.setId( UUID.randomUUID() );

        final Set< S3Object > objects = new HashSet<>();

        objects.add( (S3Object)BeanFactory.newBean( S3Object.class ).setName("foo").setId( UUID.randomUUID() ) );
        objects.add( (S3Object)BeanFactory.newBean( S3Object.class ).setName("bar").setId( UUID.randomUUID() ) );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.deleteObjects( PreviousVersions.DELETE_SPECIFIC_VERSION, bucket, objects );

        final DeleteObjectsRequest request =
                client.getRequest( DeleteObjectsRequest.class );
        assertEquals("b1", request.getBucketName(), "Shoulda formed request properly.");
        assertEquals(2, request.getObjects().size(), "Shoulda formed request properly.");

        final List<String> objectNames = request.getObjects().stream().map(DeleteObject::getKey).collect(Collectors.toList());
        assertTrue(objectNames.contains( objects.iterator().next().getName() ), "Shoulda formed request properly.");
    }

    @Test
    public void testDeleteBucketAndObjectsDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                DeleteBucketSpectraS3Request.class,
                null,
                204,
                null );

        final String bucketName = "b1";
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.deleteBucket( bucketName, true );

        final DeleteBucketSpectraS3Request request =
                client.getRequest( DeleteBucketSpectraS3Request.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertTrue(request.getForce(), "Shoulda formed request properly.");
    }

    @Test
    public void testDeleteBucketIfEmptyDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                DeleteBucketSpectraS3Request.class,
                null,
                204,
                null );

        final String bucketName = "b1";
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.deleteBucket( bucketName, false );

        final DeleteBucketSpectraS3Request request =
                client.getRequest( DeleteBucketSpectraS3Request.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertFalse(request.getForce(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsBucketExistantReturnsTrueIfExists()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                GetBucketSpectraS3Request.class,
                new com.spectralogic.ds3client.models.Bucket(),
                200,
                null );

        final String bucketName = "mbucket";
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertTrue(connection.isBucketExistant( bucketName ), "Shoulda reported bucket exists.");

        final GetBucketSpectraS3Request request =
                client.getRequest( GetBucketSpectraS3Request.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsBucketExistantReturnsFalseIfExists()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                GetBucketSpectraS3Request.class,
                new com.spectralogic.ds3client.models.Bucket(),
                404,
                null );

        final String bucketName = "mbucket";
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertFalse(connection.isBucketExistant( bucketName ), "Shoulda reported bucket doesn't exist.");

        final GetBucketSpectraS3Request request =
                client.getRequest( GetBucketSpectraS3Request.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsBucketExistantThrowsIfOtherFailure()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                GetBucketSpectraS3Request.class,
                new com.spectralogic.ds3client.models.Bucket(),
                400,
                null );

        final String bucketName = "mbucket";
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 400 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.isBucketExistant( bucketName );
            }
        } );

        final GetBucketSpectraS3Request request =
                client.getRequest( GetBucketSpectraS3Request.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsJobExistantReturnsTrueIfExists()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                GetActiveJobSpectraS3Request.class,
                new ActiveJob(),
                200,
                null );

        final UUID jobId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertTrue(connection.isJobExistant( jobId ), "Shoulda reported job exists.");

        final GetActiveJobSpectraS3Request request =
                client.getRequest( GetActiveJobSpectraS3Request.class );
        assertEquals(jobId.toString(), request.getActiveJobId(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsJobExistantReturnsFalseIfExists()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                GetActiveJobSpectraS3Request.class,
                new ActiveJob(),
                404,
                null );

        final UUID jobId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertFalse(connection.isJobExistant( jobId ), "Shoulda reported job doesn't exist.");

        final GetActiveJobSpectraS3Request request =
                client.getRequest( GetActiveJobSpectraS3Request.class );
        assertEquals(jobId.toString(), request.getActiveJobId(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsJobExistantThrowsIfOtherFailure()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                GetActiveJobSpectraS3Request.class,
                new ActiveJob(),
                400,
                null );

        final UUID jobId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 400 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.isJobExistant( jobId );
            }
        } );

        final GetActiveJobSpectraS3Request request =
                client.getRequest( GetActiveJobSpectraS3Request.class );
        assertEquals(jobId.toString(), request.getActiveJobId(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsChunkAllocatedReturnsTrueIfAllocated()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final UUID jobId = UUID.randomUUID();
        final UUID chunkId = UUID.randomUUID();
        final MasterObjectList retval = new MasterObjectList();
        final Objects objects = new Objects();
        objects.setChunkId( chunkId );
        retval.setObjects( CollectionFactory.toList( objects ) );
        final Map< String, String > headers = new HashMap<>();
        headers.put( "Retry-After", "5" );
        client.setResponse(
                GetJobChunksReadyForClientProcessingSpectraS3Request.class,
                retval,
                200,
                headers );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertTrue(connection.isChunkAllocated( jobId, chunkId ), "Shoulda reported chunk allocated.");

        final GetJobChunksReadyForClientProcessingSpectraS3Request request =
                client.getRequest( GetJobChunksReadyForClientProcessingSpectraS3Request.class );
        assertEquals(jobId.toString(), request.getJob(), "Shoulda formed request properly.");
        assertEquals(chunkId.toString(), request.getJobChunk(), "Shoulda formed request properly.");
        assertEquals(12, request.getPreferredNumberOfChunks(), "Shoulda formed request properly.");
    }

    @Test
    public void testIsChunkAllocatedReturnsFalseIfNotAllocated()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final UUID jobId = UUID.randomUUID();
        final UUID chunkId = UUID.randomUUID();
        final MasterObjectList retval = new MasterObjectList();
        final Objects objects = new Objects();
        objects.setChunkId( UUID.randomUUID() );
        retval.setObjects( CollectionFactory.toList( objects ) );
        final Map< String, String > headers = new HashMap<>();
        headers.put( "Retry-After", "5" );
        client.setResponse(
                GetJobChunksReadyForClientProcessingSpectraS3Request.class,
                retval,
                200,
                headers );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertFalse(connection.isChunkAllocated( jobId, chunkId ), "Shoulda reported chunk not allocated.");

        final GetJobChunksReadyForClientProcessingSpectraS3Request request =
                client.getRequest( GetJobChunksReadyForClientProcessingSpectraS3Request.class );
        assertEquals(jobId.toString(), request.getJob(), "Shoulda formed request properly.");
    }

    @Test
    public void testGetChunkReadyToReadReturnsNullIfChunkDoesNotExist()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final UUID chunkId = UUID.randomUUID();
        final com.spectralogic.ds3client.models.JobChunk chunk =
                new com.spectralogic.ds3client.models.JobChunk();
        chunk.setPendingTargetCommit( false );
        client.setResponse(
                GetJobChunkDaoSpectraS3Request.class,
                chunk,
                404,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(null, connection.getChunkReadyToRead( chunkId ), "Shoulda reported no chunk.");

        final GetJobChunkDaoSpectraS3Request request =
                client.getRequest( GetJobChunkDaoSpectraS3Request.class );
        assertEquals(chunkId.toString(), request.getJobChunkDao(), "Shoulda formed request properly.");
    }

    @Test
    public void testGetChunkReadyToReadReturnsTrueIfChunkExistsAndIsReadyToRead()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final UUID chunkId = UUID.randomUUID();
        final com.spectralogic.ds3client.models.JobChunk chunk =
                new com.spectralogic.ds3client.models.JobChunk();
        chunk.setBlobStoreState( JobChunkBlobStoreState.COMPLETED );
        client.setResponse(
                GetJobChunkDaoSpectraS3Request.class,
                chunk,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(Boolean.TRUE, connection.getChunkReadyToRead( chunkId ), "Shoulda reported pending target commit.");

        final GetJobChunkDaoSpectraS3Request request =
                client.getRequest( GetJobChunkDaoSpectraS3Request.class );
        assertEquals(chunkId.toString(), request.getJobChunkDao(), "Shoulda formed request properly.");
    }

    @Test
    public void testGetChunkReadyToReadReturnsFalseIfChunkExistsAndNotReadyToRead() throws IOException {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final UUID chunkId = UUID.randomUUID();
        final com.spectralogic.ds3client.models.JobChunk chunk =
                new com.spectralogic.ds3client.models.JobChunk();
        chunk.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS );
        client.setResponse(
                GetJobChunkDaoSpectraS3Request.class,
                chunk,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(Boolean.FALSE, connection.getChunkReadyToRead( chunkId ), "Shoulda reported not pending target commit.");

        final GetJobChunkDaoSpectraS3Request request =
                client.getRequest( GetJobChunkDaoSpectraS3Request.class );
        assertEquals(chunkId.toString(), request.getJobChunkDao(), "Shoulda formed request properly.");
    }

    @Test
    public void testCreateBucketDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                PutBucketSpectraS3Request.class,
                new com.spectralogic.ds3client.models.Bucket(),
                201,
                null );

        final UUID bucketId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.createBucket( bucketId, "bname", "dp" );

        final PutBucketSpectraS3Request request =
                client.getRequest( PutBucketSpectraS3Request.class );
        assertEquals(bucketId.toString(), request.getId(), "Shoulda formed request properly.");
        assertEquals("bname", request.getName(), "Shoulda formed request properly.");
        assertEquals("dp", request.getDataPolicyId(), "Shoulda formed request properly.");
    }

    @Test
    public void testCreateWhenCannotGetJobToReplicateAfterCreationFails()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        final JobEntry e1 = entries.stream().filter(e -> e.getBlobId() == b1.getId()).toList().get(0);
        final UUID jobId = e1.getJobId();
        final Job job = mockDaoDriver.attain( Job.class, jobId );

        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                GetBulkJobSpectraS3Request.class,
                null,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );

        final String bucketName = dbSupport.getServiceManager().getService( BucketService.class ).attain( job.getBucketId() ).getName();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.createGetJob( job, CollectionFactory.toSet(e1), bucketName );
            }
        } );
    }

    @Test
    public void testCreateGetJobDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        final JobEntry e1 = entries.stream().filter(e -> e.getBlobId() == b1.getId()).toList().get(0);
        final UUID jobId = e1.getJobId();
        final Job job = mockDaoDriver.attain( Job.class, jobId );
        mockDaoDriver.updateBean( job.setName( "GET by localhost" ), NameObservable.NAME );

        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        final MasterObjectList createGetJobResponse = new MasterObjectList();
        createGetJobResponse.setJobId(UUID.randomUUID());
        client.setResponse(
                GetBulkJobSpectraS3Request.class,
                createGetJobResponse,
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );

        final String bucketName = dbSupport.getServiceManager().getService( BucketService.class ).attain( job.getBucketId() ).getName();
        final UUID remoteJobId = connection.createGetJob( job, CollectionFactory.toSet(e1), bucketName );

        final GetBulkJobSpectraS3Request request =
                client.getRequest( GetBulkJobSpectraS3Request.class );
        assertEquals(job.getPriority().toString(), request.getPriority().toString(), "Shoulda formed request properly.");
        assertEquals(bucketName, request.getBucket(), "Shoulda formed request properly.");
        int objectCount = 0;
        for (final Ds3Object ignored : request.getObjects()) {
            objectCount++;
        }
        assertEquals(1, objectCount, "Shoulda formed request properly.");
        assertEquals(Ds3Connection.GET_JOB_PREFIX + job.getName(), request.getName(), "Shoulda formed request properly.");
        assertEquals(
                job.getId().toString(),
                request.getHeaders().get(
                        S3HeaderType.SPECIFY_BY_ID.getHttpHeaderName() ).iterator().next(),
                "Shoulda formed request properly.");
        assertEquals(createGetJobResponse.getJobId(), remoteJobId, "Shoulda have returned remote job id.");
    }

    @Test
    public void testVerifySafeToCreatePutJobDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                VerifySafeToCreatePutJobSpectraS3Request.class,
                null,
                200,
                null );

        final String bucketName = "blah";
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.verifySafeToCreatePutJob( bucketName );

        final VerifySafeToCreatePutJobSpectraS3Request request =
                client.getRequest( VerifySafeToCreatePutJobSpectraS3Request.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
    }

    @Test
    public void testReplicatePutJobDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );
        final UUID jobId = entries.iterator().next().getJobId();
        final Job job = mockDaoDriver.attain( Job.class, jobId );

        final JobReplicationSupport support =
                new JobReplicationSupport( dbSupport.getServiceManager(), jobId );

        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                ReplicatePutJobSpectraS3Request.class,
                new MasterObjectList(),
                200,
                null );

        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.replicatePutJob( support.getJobToReplicate(), "bname" );

        final ReplicatePutJobSpectraS3Request request =
                client.getRequest( ReplicatePutJobSpectraS3Request.class );
        assertEquals(job.getPriority().toString(), request.getPriority().toString(), "Shoulda formed request properly.");
        assertEquals("bname", request.getBucketName(), "Shoulda formed request properly.");
    }

    @Test
    public void testCancelGetJobDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        client.setResponse(
                CancelJobSpectraS3Request.class,
                null,
                204,
                null );

        final UUID jobId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        connection.cancelGetJob( jobId );

        final CancelJobSpectraS3Request request =
                client.getRequest( CancelJobSpectraS3Request.class );
        assertEquals(jobId.toString(), request.getJobId(), "Shoulda formed request properly.");
    }

    @Test
    public void testGetBlobPersistenceDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();

        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse(
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final UUID blobId1 = UUID.randomUUID();
        final UUID blobId2 = UUID.randomUUID();
        final BlobPersistence blobPersistence = BeanFactory.newBean( BlobPersistence.class );
        blobPersistence.setId( blobId2 );
        blobPersistence.setAvailableOnPoolNow( true );
        final BlobPersistenceContainer container = BeanFactory.newBean( BlobPersistenceContainer.class );
        container.setBlobs( new BlobPersistence [] { blobPersistence } );
        client.setResponse(
                GetBlobPersistenceSpectraS3Request.class,
                container.toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                200,
                null );

        final UUID jobId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection =
                new Ds3ConnectionImpl( UUID.randomUUID(), target, "YQ==", client.getClient() );
        assertEquals(1, connection.getBlobPersistence(
                jobId,
                CollectionFactory.toSet(blobId1, blobId2)).getBlobs().length, "Shoulda reported response correctly.");

        final GetBlobPersistenceSpectraS3Request request =
                client.getRequest( GetBlobPersistenceSpectraS3Request.class );
        assertTrue(request.getRequestPayload().contains( jobId.toString() ), "Shoulda formed request properly.");
        assertTrue(request.getRequestPayload().contains( blobId1.toString() ), "Shoulda formed request properly.");
        assertTrue(request.getRequestPayload().contains( blobId2.toString() ), "Shoulda formed request properly.");
    }

    @Test
    public void testGetBlobFailsWhenRequestFails(TestInfo testInfo) throws IOException
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final String bucketName = "b1";
        final String objectName = "o1";
        final File fileInCache = File.createTempFile( testInfo.getDisplayName(), "fileincache" );
        fileInCache.deleteOnExit();
        final UUID jobId = UUID.randomUUID();
        final Blob blob = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 )
                .setLength( 88 )
                .setChecksum( "v3Ibvw==" )
                .setChecksumType( ChecksumType.CRC_32C );
        
        final Set< S3ObjectProperty > metadata = new HashSet<>();
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k1" ).setValue( "v1" ) );
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k2" ).setValue( "v2" ) );
        
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.getBlob( jobId, bucketName, objectName, blob, fileInCache );
            }
        } );
        
        fileInCache.delete();
    }

    @Test
    public void testGetBlobFailsWhenBlobLengthMismatch(TestInfo testInfo) throws IOException
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final String bucketName = "b1";
        final String objectName = "o1";
        final UUID objectId = UUID.randomUUID();
        final File fileInCache = File.createTempFile( testInfo.getDisplayName(), "fileincache" );
        fileInCache.deleteOnExit();
        final UUID jobId = UUID.randomUUID();
        final Blob blob = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 )
                .setLength( 89 )
                .setChecksum( "v3Ibvw==" )
                .setChecksumType( ChecksumType.MD5 )
                .setObjectId( objectId );
        
        final Set< S3ObjectProperty > metadata = new HashSet<>();
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k1" ).setValue( "v1" ) );
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k2" ).setValue( "v2" ) );

        client.setResponse( 
                GetObjectRequest.class,
                getBlobContent( 88 ),
                200,
                null );
        
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.getBlob( jobId, bucketName, objectName, blob, fileInCache );
            }
        } );
        
        final GetObjectRequest request =
                client.getRequest( GetObjectRequest.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertEquals(objectName, request.getObjectName(), "Shoulda formed request properly.");
        assertEquals(blob.getByteOffset(), request.getOffset(), "Shoulda formed request properly.");
        fileInCache.delete();
    }

    @Test
    public void testGetBlobFailsWhenChecksumMismatch(TestInfo testInfo) throws IOException
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final String bucketName = "b1";
        final String objectName = "o1";
        final File fileInCache = File.createTempFile( testInfo.getDisplayName(), "fileincache" );
        fileInCache.deleteOnExit();
        final UUID jobId = UUID.randomUUID();
        final Blob blob = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 )
                .setLength( 88 )
                .setChecksum( "v3Ibvw==" )
                .setChecksumType( ChecksumType.MD5 )
                .setObjectId( UUID.randomUUID() );
        
        final Set< S3ObjectProperty > metadata = new HashSet<>();
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k1" ).setValue( "v1" ) );
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k2" ).setValue( "v2" ) );

        client.setResponse( 
                GetObjectRequest.class,
                getBlobContent( 88 ),
                200,
                null );
        
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                connection.getBlob( jobId, bucketName, objectName, blob, fileInCache );
            }
        } );
        
        final GetObjectRequest request =
                client.getRequest( GetObjectRequest.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertEquals(objectName, request.getObjectName(), "Shoulda formed request properly.");
        assertEquals(blob.getByteOffset(), request.getOffset(), "Shoulda formed request properly.");
        fileInCache.delete();
    }

    @Test
    public void testGetBlobDoesSoWhenCacheFileExists(TestInfo testInfo) throws IOException
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final String bucketName = "b1";
        final String objectName = "o1";
        final File fileInCache = File.createTempFile( testInfo.getDisplayName(), "fileincache" );
        fileInCache.deleteOnExit();
        final UUID jobId = UUID.randomUUID();
        final Blob blob = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 )
                .setLength( 88 )
                .setChecksum( "v3Ibvw==" )
                .setChecksumType( ChecksumType.CRC_32C )
                .setObjectId( UUID.randomUUID() );
        
        final Set< S3ObjectProperty > metadata = new HashSet<>();
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k1" ).setValue( "v1" ) );
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k2" ).setValue( "v2" ) );

        client.setResponse( 
                GetObjectRequest.class,
                getBlobContent( 88 ),
                200,
                null );
        
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        connection.getBlob( jobId, bucketName, objectName, blob, fileInCache );
        
        final GetObjectRequest request =
                client.getRequest( GetObjectRequest.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertEquals(objectName, request.getObjectName(), "Shoulda formed request properly.");
        assertEquals(blob.getByteOffset(), request.getOffset(), "Shoulda formed request properly.");
        fileInCache.delete();
    }

    @Test
    public void testGetBlobDoesSoWhenCacheFileDoesNotExist(TestInfo testInfo) throws IOException
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final String bucketName = "b1";
        final String objectName = "o1";
        final File fileInCache = File.createTempFile( testInfo.getDisplayName(), "fileincache" );
        fileInCache.delete();
        fileInCache.deleteOnExit();
        final UUID jobId = UUID.randomUUID();
        final Blob blob = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 )
                .setLength( 88 )
                .setChecksum( "v3Ibvw==" )
                .setChecksumType( ChecksumType.CRC_32C )
                .setObjectId( UUID.randomUUID() );
        
        final Set< S3ObjectProperty > metadata = new HashSet<>();
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k1" ).setValue( "v1" ) );
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k2" ).setValue( "v2" ) );

        client.setResponse( 
                GetObjectRequest.class,
                getBlobContent( 88 ),
                200,
                null );
        
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        connection.getBlob( jobId, bucketName, objectName, blob, fileInCache );
        
        final GetObjectRequest request =
                client.getRequest( GetObjectRequest.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertEquals(objectName, request.getObjectName(), "Shoulda formed request properly.");
        assertEquals(blob.getByteOffset(), request.getOffset(), "Shoulda formed request properly.");
        fileInCache.delete();
    }

    @Test
    public void testPutBlobDoesSoWhenNullObjectCreationDate(TestInfo testInfo) throws IOException
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final String bucketName = "b1";
        final String objectName = "o1";
        final File fileInCache = File.createTempFile( testInfo.getDisplayName(), "fileincache" );
        fileInCache.deleteOnExit();
        final UUID jobId = UUID.randomUUID();
        final Blob blob = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 )
                .setLength( 88 )
                .setChecksum( "cv" )
                .setChecksumType( ChecksumType.values()[ 1 ] );
        
        final Set< S3ObjectProperty > metadata = new HashSet<>();
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k1" ).setValue( "v1" ) );
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k2" ).setValue( "v2" ) );
        
        client.setResponse( 
                PutObjectRequest.class,
                null,
                200,
                null );
        
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        connection.putBlob( jobId, bucketName, objectName, blob, fileInCache, null, metadata );
        
        final PutObjectRequest request =
                client.getRequest( PutObjectRequest.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertEquals(objectName, request.getObjectName(), "Shoulda formed request properly.");
        assertEquals(blob.getByteOffset(), request.getOffset(), "Shoulda formed request properly.");
        assertEquals(blob.getLength(), request.getSize(), "Shoulda formed request properly.");
        assertEquals(blob.getChecksumType().toString(), request.getChecksumType().toString(), "Shoulda formed request properly.");

        final Map< String, String > actualHeaders = new HashMap<>();
        for (final Map.Entry<String, String> entry : request.getHeaders().entries())
        {
            actualHeaders.put(entry.getKey(), entry.getValue());
        }
        final Map< String, String > expectedHeaders = new HashMap<>();
        expectedHeaders.put(
                S3HeaderType.JOB_CHUNK_LOCK_HOLDER.getHttpHeaderName(),
                sourceInstanceId.toString() );
        expectedHeaders.put(
                S3HeaderType.NAMING_CONVENTION.getHttpHeaderName(),
                "s3" );
        for ( final S3ObjectProperty kv : metadata )
        {
            expectedHeaders.put( kv.getKey(), kv.getValue() );
        }
        assertEquals(expectedHeaders, actualHeaders, "Shoulda formed request properly.");
        fileInCache.delete();
    }

    @Test
    public void testPutBlobDoesSoWhenNonNullObjectCreationDate(TestInfo testInfo) throws IOException
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final String bucketName = "b1";
        final String objectName = "o1";
        final File fileInCache = File.createTempFile( testInfo.getDisplayName(), "fileincache" );
        fileInCache.deleteOnExit();
        final UUID jobId = UUID.randomUUID();
        final Blob blob = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 )
                .setLength( 88 )
                .setChecksum( "cv" )
                .setChecksumType( ChecksumType.values()[ 1 ] );
        
        final Set< S3ObjectProperty > metadata = new HashSet<>();
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k1" ).setValue( "v1" ) );
        metadata.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "k2" ).setValue( "v2" ) );
        
        client.setResponse( 
                PutObjectRequest.class,
                null,
                200,
                null );
        
        final Date creationDate = new Date( 10001 );
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        connection.putBlob( jobId, bucketName, objectName, blob, fileInCache, creationDate, metadata );
        
        final PutObjectRequest request =
                client.getRequest( PutObjectRequest.class );
        assertEquals(bucketName, request.getBucketName(), "Shoulda formed request properly.");
        assertEquals(objectName, request.getObjectName(), "Shoulda formed request properly.");
        assertEquals(blob.getByteOffset(), request.getOffset(), "Shoulda formed request properly.");
        assertEquals(blob.getLength(), request.getSize(), "Shoulda formed request properly.");
        assertEquals(blob.getChecksumType().toString(), request.getChecksumType().toString(), "Shoulda formed request properly.");

        final Map< String, String > actualHeaders = new HashMap<>();
        for ( final Map.Entry< String, String > e : request.getHeaders().entries() )
        {
            actualHeaders.put( e.getKey(), e.getValue() );
        }
        final Map< String, String > expectedHeaders = new HashMap<>();
        expectedHeaders.put(
                S3HeaderType.JOB_CHUNK_LOCK_HOLDER.getHttpHeaderName(),
                sourceInstanceId.toString() );
        expectedHeaders.put(
                S3HeaderType.NAMING_CONVENTION.getHttpHeaderName(),
                "s3" );
        expectedHeaders.put(
                S3HeaderType.OBJECT_CREATION_DATE.getHttpHeaderName(),
                String.valueOf( creationDate.getTime() ) );
        for ( final S3ObjectProperty kv : metadata )
        {
            expectedHeaders.put( kv.getKey(), kv.getValue() );
        }
        assertEquals(expectedHeaders, actualHeaders, "Shoulda formed request properly.");
        fileInCache.delete();
    }

    @Test
    public void testKeepJobAliveDoesSo()
    {
        final MockDs3Client client = new MockDs3Client();
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        client.setResponse( 
                ModifyJobSpectraS3Request.class,
                new MasterObjectList(),
                200,
                null );
        
        final UUID jobId = UUID.randomUUID();
        final UUID sourceInstanceId = UUID.randomUUID();
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" );
        final Ds3Connection connection = 
                new Ds3ConnectionImpl( sourceInstanceId, target, "YQ==", client.getClient() );
        connection.keepJobAlive( jobId );
        
        final ModifyJobSpectraS3Request request =
                client.getRequest( ModifyJobSpectraS3Request.class );
        assertEquals(jobId.toString(), request.getJobId(), "Shoulda formed request properly.");
    }


    private byte[] getBlobContent( final int byteCount )
    {
        final ByteBuffer buffer = ByteBuffer.allocate( byteCount );
        for ( int i = 0; i < byteCount; ++i )
        {
            buffer.put( (byte)( i % 100 ) );
        }
        buffer.flip();
        return buffer.array();
    }
}
