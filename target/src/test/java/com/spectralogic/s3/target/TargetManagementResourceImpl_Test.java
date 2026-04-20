/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target;

import com.spectralogic.ds3client.commands.DeleteObjectsRequest;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.models.delete.DeleteObject;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.ds3client.models.Group;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3Region;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetAccessControlReplication;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.S3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.*;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreatePutJobParams;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.s3.common.rpc.dataplanner.domain.S3ObjectToCreate;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.target.*;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.common.testfrmwrk.target.MockAzureConnection;
import com.spectralogic.s3.common.testfrmwrk.target.MockS3Connection;
import com.spectralogic.s3.target.azuretarget.AzureSdkFailure;
import com.spectralogic.s3.target.azuretarget.DefaultAzureConnectionFactory;
import com.spectralogic.s3.target.ds3target.Ds3SdkFailure;
import com.spectralogic.s3.target.ds3target.MockDs3Client;
import com.spectralogic.s3.target.ds3target.MockDs3ConnectionFactory;
import com.spectralogic.s3.target.s3target.DefaultS3ConnectionFactory;
import com.spectralogic.s3.target.s3target.S3SdkFailure;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Tag( "public-cloud-integration" )
public final class TargetManagementResourceImpl_Test
{
    @Test
    public void testConstructorNullRpcServerNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                newTargetManagementResourceImpl(
                        null,
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                        dbSupport.getServiceManager() );
            }
        } );
    }
    
    @Test
    public void testConstructorNullConnectionFactoryNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                newTargetManagementResourceImpl(
                        InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                        (Ds3ConnectionFactory)null,
                        InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                newTargetManagementResourceImpl(
                        InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                        (AzureConnectionFactory)null,
                        InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                newTargetManagementResourceImpl(
                        InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                        (S3ConnectionFactory)null,
                        InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                        dbSupport.getServiceManager() );
            }
        } );
    }
    
    @Test
    public void testConstructorNullDataPlannerResourceNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                newTargetManagementResourceImpl(
                        InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        null,
                        dbSupport.getServiceManager() );
            }
        } );
    }
    
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                newTargetManagementResourceImpl(
                        InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                        InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                        InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                        null );
            }
        } );
    }
    
    @Test
    public void testHappyConstruction()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
    }
    
    @Test
    public void testRegisterDs3TargetConflictingDueToIdAlreadyRegisteredNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        mockDaoDriver.createDs3Target( instanceId, "blah" );
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
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "aid" )
                .setAdminSecretKey( "ask" )
                .setName( "target" )
                .setAccessControlReplication( Ds3TargetAccessControlReplication.USERS );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.registerDs3Target( target );
            }
        } );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                );
    }
    
    @Test
    public void testRegisterDs3TargetConflictingDueToIdSameAsSourceNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).getInstanceId();
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
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "aid" )
                .setAdminSecretKey( "ask" )
                .setName( "target" )
                .setAccessControlReplication( Ds3TargetAccessControlReplication.USERS );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.registerDs3Target( target );
            }
        } );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testRegisterDs3TargetConflictingDueToNameAlreadyRegisteredNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        mockDaoDriver.createDs3Target( "target" );
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
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "aid" )
                .setAdminSecretKey( "ask" )
                .setName( "target" )
                .setAccessControlReplication( Ds3TargetAccessControlReplication.USERS );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.registerDs3Target( target );
            }
        } );
    }
    
    @Test
    public void testRegisterDs3TargetAccessControlReplicationNoneWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user1.getId() );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.TAPE_ADMINS, user2.getId() );
        
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
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "aid" )
                .setAdminSecretKey( "ask" )
                .setName( "target" );
        resource.registerDs3Target( target );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }

    @Test
    public void testRegisterDs3TargetAccessControlReplicationUsersWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user1.getId() );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.TAPE_ADMINS, user3.getId() );
        
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
        
        final SpectraUser spectraUser1 = new SpectraUser();
        spectraUser1.setAuthId( "aid1" );
        spectraUser1.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser1.setId( UUID.randomUUID() );
        spectraUser1.setName( "Administrator" );
        spectraUser1.setSecretKey( "key1" );
        final SpectraUser spectraUser2 = new SpectraUser();
        spectraUser2.setAuthId( "aid2" );
        spectraUser2.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser2.setId( UUID.randomUUID() );
        spectraUser2.setName( "user2" );
        spectraUser2.setSecretKey( user2.getSecretKey() );
        final SpectraUser spectraUser3 = new SpectraUser();
        spectraUser3.setAuthId( "aid3" );
        spectraUser3.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser3.setId( UUID.randomUUID() );
        spectraUser3.setName( "user3" );
        spectraUser3.setSecretKey( "key2" );
        final SpectraUserList users = new SpectraUserList();
        users.setSpectraUsers( CollectionFactory.toList( spectraUser1, spectraUser2, spectraUser3 ) );
        client.setResponse( 
                GetUsersSpectraS3Request.class,
                users,
                200,
                null );
        
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                201,
                null );
        
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                200,
                null );
        
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "aid" )
                .setAdminSecretKey( "ask" )
                .setName( "target" )
                .setAccessControlReplication( Ds3TargetAccessControlReplication.USERS );
        resource.registerDs3Target( target );
        
        final DelegateCreateUserSpectraS3Request requestCreateUser = 
                client.getRequest( DelegateCreateUserSpectraS3Request.class );
        client.verifyRequestMatchesDao( User.class, user1, requestCreateUser );
        
        final ModifyUserSpectraS3Request requestModifyUser = 
                client.getRequest( ModifyUserSpectraS3Request.class );
        assertEquals(
                user3.getName(),
                requestModifyUser.getUserId(),
                "Shoulda updated secret key to match for user3."
                 );
        assertEquals(
                user3.getSecretKey(),
                requestModifyUser.getSecretKey(),
                "Shoulda updated secret key to match for user3."
                 );
        assertNotNull(
                dbSupport.getServiceManager().getRetriever( Ds3Target.class ).retrieve( Require.nothing() ),
                "Shoulda created target."
                 );
        assertEquals(
                "ask",
                mockDaoDriver.attain( target ).getAdminSecretKey(),
                "Shoulda retained original admin secret key."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }

    @Test
    public void testRegisterDs3TargetAccessControlReplicationUsersWorksWhenReplicatingAdminSecretKeyChange()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user1.getId() );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.TAPE_ADMINS, user3.getId() );
        
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
        
        final SpectraUser spectraUser1 = new SpectraUser();
        spectraUser1.setAuthId( user1.getAuthId() );
        spectraUser1.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser1.setId( UUID.randomUUID() );
        spectraUser1.setName( user1.getName() );
        spectraUser1.setSecretKey( "key1" );
        final SpectraUser spectraUser2 = new SpectraUser();
        spectraUser2.setAuthId( "aid2" );
        spectraUser2.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser2.setId( UUID.randomUUID() );
        spectraUser2.setName( "user2" );
        spectraUser2.setSecretKey( user2.getSecretKey() );
        final SpectraUser spectraUser3 = new SpectraUser();
        spectraUser3.setAuthId( "aid3" );
        spectraUser3.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser3.setId( UUID.randomUUID() );
        spectraUser3.setName( "user3" );
        spectraUser3.setSecretKey( "key2" );
        final SpectraUserList users = new SpectraUserList();
        users.setSpectraUsers( CollectionFactory.toList( spectraUser1, spectraUser2, spectraUser3 ) );
        client.setResponse( 
                GetUsersSpectraS3Request.class,
                users,
                200,
                null );
        
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                201,
                null );
        
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                200,
                null );
        
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( user1.getAuthId() )
                .setAdminSecretKey( "ask" )
                .setName( "target" )
                .setAccessControlReplication( Ds3TargetAccessControlReplication.USERS );
        resource.registerDs3Target( target );
        
        assertEquals(
                2,
                client.getRequestCount( ModifyUserSpectraS3Request.class ),
                "Shoulda sent 2 modifies."
                );
        assertNotNull(
                dbSupport.getServiceManager().getRetriever( Ds3Target.class ).retrieve( Require.nothing() ),
                "Shoulda created target."
                 );
        assertEquals(
                user1.getSecretKey(),
                mockDaoDriver.attain( target ).getAdminSecretKey(),
                "Shoulda updated admin secret key."
                );
    }
    
    @Test
    public void testRegisterDs3TargetAccessControlReplicationUsersWhereReplicationPartFailsDoesNotSucceed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user1.getId() );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.TAPE_ADMINS, user3.getId() );
        
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
        
        final SpectraUser spectraUser1 = new SpectraUser();
        spectraUser1.setAuthId( "aid1" );
        spectraUser1.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser1.setId( UUID.randomUUID() );
        spectraUser1.setName( "Administrator" );
        spectraUser1.setSecretKey( "key1" );
        final SpectraUser spectraUser2 = new SpectraUser();
        spectraUser2.setAuthId( "aid2" );
        spectraUser2.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser2.setId( UUID.randomUUID() );
        spectraUser2.setName( "user2" );
        spectraUser2.setSecretKey( user2.getSecretKey() );
        final SpectraUser spectraUser3 = new SpectraUser();
        spectraUser3.setAuthId( "aid3" );
        spectraUser3.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser3.setId( UUID.randomUUID() );
        spectraUser3.setName( "user3" );
        spectraUser3.setSecretKey( "key2" );
        final SpectraUserList users = new SpectraUserList();
        users.setSpectraUsers( CollectionFactory.toList( spectraUser1, spectraUser2, spectraUser3 ) );
        client.setResponse( 
                GetUsersSpectraS3Request.class,
                users,
                200,
                null );
        
        final SecureRandom random = new SecureRandom();
        final boolean failFirst = ( 0 == random.nextInt( 2 ) );
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                ( failFirst ) ? 410 : 201,
                null );
        
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                ( !failFirst ) ? 410 : 200,
                null );
        
        final Ds3Target target = BeanFactory.newBean( Ds3Target.class )
                .setDataPathEndPoint( "invalid.spectralogic.com" )
                .setAdminAuthId( "aid" )
                .setAdminSecretKey( "ask" )
                .setName( "target" )
                .setAccessControlReplication( Ds3TargetAccessControlReplication.USERS );
        
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 410 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.registerDs3Target( target );
            }
        } );
        
        final DelegateCreateUserSpectraS3Request requestCreateUser = 
                client.getRequest( DelegateCreateUserSpectraS3Request.class );
        if ( null != requestCreateUser )
        {
            client.verifyRequestMatchesDao( User.class, user1, requestCreateUser );
        }
        
        final ModifyUserSpectraS3Request requestModifyUser = 
                client.getRequest( ModifyUserSpectraS3Request.class );
        if ( null != requestModifyUser )
        {
            assertEquals(
                    user3.getName(),
                    requestModifyUser.getUserId(),
                    "Shoulda updated secret key to match for user3."
                    );
            assertEquals(
                    user3.getSecretKey(),
                    requestModifyUser.getSecretKey(),
                    "Shoulda updated secret key to match for user3."
                    );
        }
        assertNull(
                dbSupport.getServiceManager().getRetriever( Ds3Target.class ).retrieve( Require.nothing() ),
                "Should notta created target."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }

    @Test
    public void testModifyDs3TargetPropertyOtherThanAccessControlReplicationWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
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
        
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target1" );
        resource.modifyDs3Target(
                target.setDataPathEndPoint( "10.2.4.5" ), 
                new String [] { Ds3Target.DATA_PATH_END_POINT } );
        
        assertEquals(
                "10.2.4.5",
                mockDaoDriver.attain( target ).getDataPathEndPoint(),
                "Shoulda updated target."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }

    @Test
    public void testModifyDs3TargetPropertyOtherThanAccessControlReplicationFailsIfCannotConnect()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                503,
                null );
        
        final Group group = new Group();
        client.setResponse( 
                VerifyUserIsMemberOfGroupSpectraS3Request.class,
                group,
                200,
                null );
        
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target1" );
        final String originalValue = target.getDataPathEndPoint();
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 503 ), new BlastContainer()
        {
            @Override
            public void test() throws Throwable
            {
                resource.modifyDs3Target(
                        target.setDataPathEndPoint( "10.2.4.5" ), 
                        new String [] { Ds3Target.DATA_PATH_END_POINT } );
            }
        } );
        
        assertEquals(
                originalValue,
                mockDaoDriver.attain( target ).getDataPathEndPoint(),
                "Should notta updated target."
                 );
    }

    @Test
    public void testModifyDs3TargetAccessControlReplicationNoneWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
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
        
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target1" );
        mockDaoDriver.updateBean(
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        resource.modifyDs3Target(
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.NONE ), 
                new String [] { Ds3Target.ACCESS_CONTROL_REPLICATION } );
        
        assertEquals(
                Ds3TargetAccessControlReplication.NONE,
                mockDaoDriver.attain( target ).getAccessControlReplication(),
                "Shoulda updated target."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }

    @Test
    public void testModifyDs3TargetAccessControlReplicationUsersWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user1.getId() );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.TAPE_ADMINS, user3.getId() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "name" );
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
        
        final SpectraUser spectraUser1 = new SpectraUser();
        spectraUser1.setAuthId( "aid1" );
        spectraUser1.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser1.setId( UUID.randomUUID() );
        spectraUser1.setName( "Administrator" );
        spectraUser1.setSecretKey( "key1" );
        final SpectraUser spectraUser2 = new SpectraUser();
        spectraUser2.setAuthId( "aid2" );
        spectraUser2.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser2.setId( UUID.randomUUID() );
        spectraUser2.setName( "user2" );
        spectraUser2.setSecretKey( user2.getSecretKey() );
        final SpectraUser spectraUser3 = new SpectraUser();
        spectraUser3.setAuthId( "aid3" );
        spectraUser3.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser3.setId( UUID.randomUUID() );
        spectraUser3.setName( "user3" );
        spectraUser3.setSecretKey( "key2" );
        final SpectraUserList users = new SpectraUserList();
        users.setSpectraUsers( CollectionFactory.toList( spectraUser1, spectraUser2, spectraUser3 ) );
        client.setResponse( 
                GetUsersSpectraS3Request.class,
                users,
                200,
                null );
        
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                201,
                null );
        
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                200,
                null );
        
        resource.modifyDs3Target(
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ),
                new String [] { Ds3Target.ACCESS_CONTROL_REPLICATION } );
        
        final DelegateCreateUserSpectraS3Request requestCreateUser = 
                client.getRequest( DelegateCreateUserSpectraS3Request.class );
        client.verifyRequestMatchesDao( User.class, user1, requestCreateUser );
        
        final ModifyUserSpectraS3Request requestModifyUser = 
                client.getRequest( ModifyUserSpectraS3Request.class );
        assertEquals(
                user3.getName(),
                requestModifyUser.getUserId(),
                "Shoulda updated secret key to match for user3."
                );
        assertEquals(
                user3.getSecretKey(),
                requestModifyUser.getSecretKey(),
                "Shoulda updated secret key to match for user3."
               );
        assertEquals(
                Ds3TargetAccessControlReplication.USERS,
                dbSupport.getServiceManager().getRetriever( Ds3Target.class ).retrieve(
                        Require.nothing() ).getAccessControlReplication(),
                "Shoulda modified target."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testModifyDs3TargetAccessControlReplicationUsersWhereReplicationPartFailsDoesNotSucceed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user1.getId() );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.TAPE_ADMINS, user3.getId() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "name" );
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
        
        final SpectraUser spectraUser1 = new SpectraUser();
        spectraUser1.setAuthId( "aid1" );
        spectraUser1.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser1.setId( UUID.randomUUID() );
        spectraUser1.setName( "Administrator" );
        spectraUser1.setSecretKey( "key1" );
        final SpectraUser spectraUser2 = new SpectraUser();
        spectraUser2.setAuthId( "aid2" );
        spectraUser2.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser2.setId( UUID.randomUUID() );
        spectraUser2.setName( "user2" );
        spectraUser2.setSecretKey( user2.getSecretKey() );
        final SpectraUser spectraUser3 = new SpectraUser();
        spectraUser3.setAuthId( "aid3" );
        spectraUser3.setDefaultDataPolicyId( UUID.randomUUID() );
        spectraUser3.setId( UUID.randomUUID() );
        spectraUser3.setName( "user3" );
        spectraUser3.setSecretKey( "key2" );
        final SpectraUserList users = new SpectraUserList();
        users.setSpectraUsers( CollectionFactory.toList( spectraUser1, spectraUser2, spectraUser3 ) );
        client.setResponse( 
                GetUsersSpectraS3Request.class,
                users,
                200,
                null );
        
        final SecureRandom random = new SecureRandom();
        final boolean failFirst = ( 0 == random.nextInt( 2 ) );
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                ( failFirst ) ? 410 : 201,
                null );
        
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                ( !failFirst ) ? 410 : 200,
                null );
        
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 410 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.modifyDs3Target(
                        target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ),
                        new String [] { Ds3Target.ACCESS_CONTROL_REPLICATION } );
            }
        } );
        
        final DelegateCreateUserSpectraS3Request requestCreateUser = 
                client.getRequest( DelegateCreateUserSpectraS3Request.class );
        if ( null != requestCreateUser )
        {
            client.verifyRequestMatchesDao( User.class, user1, requestCreateUser );
        }
        
        final ModifyUserSpectraS3Request requestModifyUser = 
                client.getRequest( ModifyUserSpectraS3Request.class );
        if ( null != requestModifyUser )
        {
            assertEquals(
                    user3.getName(),
                    requestModifyUser.getUserId(),
                    "Shoulda updated secret key to match for user3."
                     );
            assertEquals(
                    user3.getSecretKey(),
                    requestModifyUser.getSecretKey(),
                    "Shoulda updated secret key to match for user3."
                     );
        }
        assertEquals(
                Ds3TargetAccessControlReplication.NONE,
                dbSupport.getServiceManager().getRetriever( Ds3Target.class ).retrieve(
                        Require.nothing() ).getAccessControlReplication(),
                "Should notta modified target."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testPairBackSendsRequestToPairBackTargetWithSource()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
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
        
        resource.pairBack( target.getId(), targetArg );
        
        final RegisterDs3TargetSpectraS3Request request =
                client.getRequest( RegisterDs3TargetSpectraS3Request.class );
        client.verifyRequestMatchesDao( Ds3Target.class, targetArg, request );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testPairBackWhenRequestToCreateTargetFailsFails()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
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
                409,
                null );
        
        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 409 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.pairBack( target.getId(), targetArg );
            }
        } );
        
        final RegisterDs3TargetSpectraS3Request request =
                client.getRequest( RegisterDs3TargetSpectraS3Request.class );
        client.verifyRequestMatchesDao( Ds3Target.class, targetArg, request );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testVerifyDs3TargetWithoutFullDetailsSuccessResultsInSuccess()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        resource.verifyDs3Target( target.getId(), false );
        assertEquals(
                1,
                client.getRequestCount( GetSystemInformationSpectraS3Request.class ),
                "Shoulda send a get system information for the admin."
                );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                );
    }
    
    @Test
    public void testVerifyDs3TargetWithFullDetailsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createDs3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), b1.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target.getId(), b3.getId() );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final BlobPersistenceContainer blobPersistence =
                BeanFactory.newBean( BlobPersistenceContainer.class );
        final BlobPersistence bp1 = BeanFactory.newBean( BlobPersistence.class );
        bp1.setId( b1.getId() );
        bp1.setChecksum( "cs" );
        bp1.setChecksumType( ChecksumType.values()[ 0 ] );
        final BlobPersistence bp2 = BeanFactory.newBean( BlobPersistence.class );
        bp2.setId( b2.getId() );
        blobPersistence.setBlobs( new BlobPersistence [] { bp1, bp2 } );
        client.setResponse( 
                GetBlobPersistenceSpectraS3Request.class,
                blobPersistence.toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                200,
                null );

        resource.verifyDs3Target( target.getId(), true );

        TargetFailure failure = mockDaoDriver.waitForFailure(Ds3TargetFailure.class);

        assertEquals(failure.getType(), TargetFailureType.VERIFY_COMPLETE);

        mockDaoDriver.deleteAll(Ds3TargetFailure.class);
        
        assertEquals(
                CollectionFactory.toSet( b2.getId(), b3.getId() ),
                BeanUtils.extractPropertyValues(
                        dbSupport.getServiceManager().getRetriever(
                                SuspectBlobDs3Target.class ).retrieveAll().toSet(),
                        BlobObservable.BLOB_ID ),
                "Shoulda noted blob loss for entries target does not have."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                );

        resource.verifyDs3Target( target.getId(), true );

        failure = mockDaoDriver.waitForFailure(Ds3TargetFailure.class);

        assertEquals(failure.getType(), TargetFailureType.VERIFY_COMPLETE);

        mockDaoDriver.deleteAll(Ds3TargetFailure.class);

        assertEquals(
                CollectionFactory.toSet( b2.getId(), b3.getId() ),
                BeanUtils.extractPropertyValues(
                        dbSupport.getServiceManager().getRetriever(
                                SuspectBlobDs3Target.class ).retrieveAll().toSet(),
                        BlobObservable.BLOB_ID ),
                "Shoulda noted no additional blob loss."
                );

        final BlobPersistence bp3 = BeanFactory.newBean( BlobPersistence.class );
        bp3.setId( b3.getId() );
        bp3.setChecksum( "cs" );
        bp3.setChecksumType( ChecksumType.values()[ 0 ] );
        bp2.setChecksum( "cs" );
        bp2.setChecksumType( ChecksumType.values()[ 0 ] );
        blobPersistence.setBlobs( new BlobPersistence [] { bp1, bp2, bp3 } );
        client.setResponse( 
                GetBlobPersistenceSpectraS3Request.class,
                blobPersistence.toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                200,
                null );

        resource.verifyDs3Target( target.getId(), true );

        failure = mockDaoDriver.waitForFailure(Ds3TargetFailure.class);

        assertEquals(failure.getType(), TargetFailureType.VERIFY_COMPLETE);

        assertEquals(
                CollectionFactory.toSet( b2.getId(), b3.getId() ),
                BeanUtils.extractPropertyValues(
                        dbSupport.getServiceManager().getRetriever(
                                SuspectBlobDs3Target.class ).retrieveAll().toSet(),
                        BlobObservable.BLOB_ID ),
                "Should notta automatically cleared blob loss."
                 );
        
        mockDaoDriver.deleteAll( SuspectBlobDs3Target.class );
        resource.verifyDs3Target( target.getId(), true );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( SuspectBlobDs3Target.class ).getCount(),
                "Should notta recorded any blob loss."
                 );
    }
    
    @Test
    public void testGetDataPoliciesDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
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
        dp1.setMaxVersionsToKeep( 42 );
        
        final com.spectralogic.ds3client.models.DataPolicy dp2 = 
                new com.spectralogic.ds3client.models.DataPolicy();
        dp2.setId( UUID.randomUUID() );
        dp2.setName( "name2" );
        dp2.setAlwaysForcePutJobCreation( false );
        dp2.setMaxVersionsToKeep( 5 );
    
        final DataPolicyList dataPolicies = new DataPolicyList();
        dataPolicies.setDataPolicies( CollectionFactory.toList( dp1, dp2 ) );
        client.setResponse( 
                GetDataPoliciesSpectraS3Request.class,
                dataPolicies,
                200,
                null );
        
        final TargetManagementResource resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target1" );
        
        final List< DataPolicy > daoDataPolicies = CollectionFactory.toList( 
                resource.getDataPolicies( target.getId() ).getWithoutBlocking().getDataPolicies() );
        assertEquals(
                2,
                daoDataPolicies.size(),
                "Shoulda unmarshaled data policies correctly."
                );
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
        
        assertEquals(
                dp1.getName(),
                daoDataPolicy1.getName(),
                "Shoulda populated data policies correctly."
                 );
        assertEquals(
                dp1.getId(),
                daoDataPolicy1.getId(),
                "Shoulda populated data policies correctly."
                 );
        assertEquals(
                dp1.getAlwaysForcePutJobCreation(),
                daoDataPolicy1.isAlwaysForcePutJobCreation(),
                "Shoulda populated data policies correctly."
                 );

        assertEquals(
                dp2.getName(),
                daoDataPolicy2.getName(),
                "Shoulda populated data policies correctly."
                 );
        assertEquals(
                dp2.getId(),
                daoDataPolicy2.getId(),
                "Shoulda populated data policies correctly."
                 );
        assertEquals(
                dp2.getAlwaysForcePutJobCreation(),
                daoDataPolicy2.isAlwaysForcePutJobCreation(),
                "Shoulda populated data policies correctly."
                 );
    }
    
    @Test
    public void testCreateUserWhenNoReplicationTargetsToReplicateToWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );
        
        final User user = 
                BeanFactory.newBean( User.class ).setAuthId( "aid" ).setName( "name" ).setSecretKey( "sk" );
        user.setId( UUID.randomUUID() );
        resource.createUser( false, user );
        
        mockDaoDriver.attainOneAndOnly( User.class );
    }
    
    @Test
    public void testCreateUserWhenNotCapableOfConnectingToReplicationTargetsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                503,
                null );

        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 503 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                final User user = 
                        BeanFactory.newBean( User.class ).setAuthId( "aid" )
                        .setName( "name" ).setSecretKey( "sk" );
                user.setId( UUID.randomUUID() );
                resource.createUser( false, user );
            }
        } );
        
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( User.class ).getCount(),
                "Should notta created any users."
                 );
        assertEquals(
                0,
                client.getRequestCount( DelegateCreateUserSpectraS3Request.class ),
                "Should notta made any create user calls."
                );
    }
    
    @Test
    public void 
    testCreateUserWhenNotCapableOfConnectingToReplicationTargetsAllowedWhenTargetPermittedGoOutOfSync()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setPermitGoingOutOfSync( true ), 
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                503,
                null );

        final User user = 
                BeanFactory.newBean( User.class ).setAuthId( "aid" )
                .setName( "name" ).setSecretKey( "sk" );
        user.setId( UUID.randomUUID() );
        resource.createUser( false, user );
        
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( User.class ).getCount(),
                "Shoulda created user."
               );
        assertEquals(
                0,
                client.getRequestCount( DelegateCreateUserSpectraS3Request.class ),
                "Should notta made any create user calls."
                 );
    }
    
    @Test
    public void 
    testCreateUserWhenNotCapableOfConnectingToReplicationTargetsAllowedWhenForced()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setPermitGoingOutOfSync( false ), 
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                503,
                null );

        final User user = 
                BeanFactory.newBean( User.class ).setAuthId( "aid" )
                .setName( "name" ).setSecretKey( "sk" );
        user.setId( UUID.randomUUID() );
        resource.createUser( true, user );
        
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( User.class ).getCount(),
                "Shoulda created user."
                 );
        assertEquals(
                0,
                client.getRequestCount( DelegateCreateUserSpectraS3Request.class ),
                "Should notta made any create user calls."
               );
    }
    
    @Test
    public void testCreateUserWhenNotCapableOfDoingSoInDaoFailsPriorToUpdatingDs3Targets()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                201,
                null );

        mockDaoDriver.createUser( "name" );
        
        final User user = 
                BeanFactory.newBean( User.class ).setAuthId( "aid" ).setName( "name" ).setSecretKey( "sk" );
        user.setId( UUID.randomUUID() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createUser( true, user );
            }
        } );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                0,
                client.getRequestCount( DelegateCreateUserSpectraS3Request.class ),
                "Should notta made any create user calls."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testCreateUserWhenCapableOfDoingSoOnRemoteTargetAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                201,
                null );
        client.setResponse(
                GetUsersSpectraS3Request.class,
                new SpectraUserList(),
                200,
                null );

        final User user = 
                BeanFactory.newBean( User.class ).setAuthId( "aid" ).setName( "name" ).setSecretKey( "sk" );
        user.setId( UUID.randomUUID() );
        resource.createUser( false, user );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                1,
                client.getRequestCount( DelegateCreateUserSpectraS3Request.class ),
                "Shoulda made any create user calls."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testCreateUserWhenNotCapableOfDoingSoOnRemoteTargetStillAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                DelegateCreateUserSpectraS3Request.class,
                new SpectraUser(), 
                409,
                null );
        client.setResponse(
                GetUsersSpectraS3Request.class,
                new SpectraUserList(),
                200,
                null );

        final User user = 
                BeanFactory.newBean( User.class ).setAuthId( "aid" ).setName( "name" ).setSecretKey( "sk" );
        user.setId( UUID.randomUUID() );
        resource.createUser( false, user );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                1,
                client.getRequestCount( DelegateCreateUserSpectraS3Request.class ),
                "Shoulda made any create user calls."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testModifyUserDoesSoWhenNotModifyingAdminUserForTarget()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user = mockDaoDriver.createUser( "user1" );
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );
        final String originalSecretKey = target.getAdminSecretKey();

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                200,
                null );

        resource.modifyUser( false, user.setSecretKey( "new" ), new String [] { User.SECRET_KEY } );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                1,
                client.getRequestCount( ModifyUserSpectraS3Request.class ),
                "Shoulda made modify user call."
                );
        assertEquals(
                "new",
                mockDaoDriver.attain( user ).getSecretKey(),
                "Should modified user."
                 );
        assertEquals(
                originalSecretKey,
                mockDaoDriver.attain( target ).getAdminSecretKey(),
                "Should notta modified admin secret key"
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                );
    }
    
    @Test
    public void testModifyUserDoesSoWhenModifyingAdminUserForTargetWhen403Failure()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user = mockDaoDriver.createUser( "user1" );
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getAuthId() ), Ds3Target.ADMIN_AUTH_ID );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getSecretKey() ), Ds3Target.ADMIN_SECRET_KEY );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                403,
                null );
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                403,
                null );

        resource.modifyUser( true, user.setSecretKey( "new" ), new String [] { User.SECRET_KEY } );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                0,
                client.getRequestCount( ModifyUserSpectraS3Request.class ),
                "Should notta made modify user call."
                 );
        assertEquals(
                "new",
                mockDaoDriver.attain( user ).getSecretKey(),
                "Should modified user."
                );
        assertEquals(
                "new",
                mockDaoDriver.attain( target ).getAdminSecretKey(),
                "Shoulda modified admin secret key"
                 );
    }
    
    @Test
    public void testModifyUserDoesSoWhenModifyingAdminUserForTargetWhenNon403FailureUponConnect()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user = mockDaoDriver.createUser( "user1" );
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getAuthId() ), Ds3Target.ADMIN_AUTH_ID );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getSecretKey() ), Ds3Target.ADMIN_SECRET_KEY );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                500,
                null );
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                500,
                null );

        resource.modifyUser( true, user.setSecretKey( "new" ), new String [] { User.SECRET_KEY } );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                0,
                client.getRequestCount( ModifyUserSpectraS3Request.class ),
                "Should notta made modify user call."
                 );
        assertEquals(
                "new",
                mockDaoDriver.attain( user ).getSecretKey(),
                "Should modified user."
                );
        assertEquals(
                "ask",
                mockDaoDriver.attain( target ).getAdminSecretKey(),
                "Should notta modified admin secret key"
                 );
    }
    
    @Test
    public void testModifyUserDoesSoWhenModifyingAdminUserForTargetWhenNon403FailureUponModify()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user = mockDaoDriver.createUser( "user1" );
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getAuthId() ), Ds3Target.ADMIN_AUTH_ID );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getSecretKey() ), Ds3Target.ADMIN_SECRET_KEY );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                500,
                null );

        resource.modifyUser( true, user.setSecretKey( "new" ), new String [] { User.SECRET_KEY } );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                1,
                client.getRequestCount( ModifyUserSpectraS3Request.class ),
                "Shoulda made modify user call."
                 );
        assertEquals(
                "new",
                mockDaoDriver.attain( user ).getSecretKey(),
                "Should modified user."
                 );
        assertEquals(
                "ask",
                mockDaoDriver.attain( target ).getAdminSecretKey(),
                "Should notta modified admin secret key"
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testModifyUserDoesSoWhenModifyingAdminUserForTarget()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final User user = mockDaoDriver.createUser( "user1" );
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getAuthId() ), Ds3Target.ADMIN_AUTH_ID );
        mockDaoDriver.updateBean( target.setAdminAuthId( user.getSecretKey() ), Ds3Target.ADMIN_SECRET_KEY );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                ModifyUserSpectraS3Request.class,
                new SpectraUser(), 
                200,
                null );

        resource.modifyUser( false, user.setSecretKey( "new" ), new String [] { User.SECRET_KEY } );
        
        mockDaoDriver.attainOneAndOnly( User.class );
        assertEquals(
                1,
                client.getRequestCount( ModifyUserSpectraS3Request.class ),
                "Shoulda made modify user call."
                 );
        assertEquals(
                "new",
                mockDaoDriver.attain( user ).getSecretKey(),
                "Should modified user."
                 );
        assertEquals(
                "new",
                mockDaoDriver.attain( target ).getAdminSecretKey(),
                "Shoulda modified admin secret key"
                );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testDeleteUserDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean( 
                target.setAccessControlReplication( Ds3TargetAccessControlReplication.USERS ), 
                Ds3Target.ACCESS_CONTROL_REPLICATION );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

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

        final User user = mockDaoDriver.createUser( "user1" );
        resource.deleteUser( false, user.getId() );
        
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( User.class ).getCount(),
                "Shoulda deleted user."
                 );
        assertEquals(
                1,
                client.getRequestCount( DelegateDeleteUserSpectraS3Request.class ),
                "Shoulda made modify user call."
                );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testDeleteObjectsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        RpcFuture.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                return new RpcResponse<>( 
                                        BeanFactory.newBean( DeleteObjectsResult.class )
                                        .setDaoModified( true ) );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

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

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        
        resource.deleteObjects( 
                null, PreviousVersions.DELETE_SPECIFIC_VERSION, new UUID [] { o1.getId(), o2.getId() } );

        final DeleteObjectsRequest deleteRequest = client.getRequest( DeleteObjectsRequest.class );
        assertNotNull( deleteRequest );
        assertEquals(2, deleteRequest.getObjects().size());
        System.out.println( deleteRequest.getObjects() );

        final List<UUID> objectIds = deleteRequest.getObjects().stream().map(DeleteObject::getVersionId).collect(Collectors.toList());
        assertTrue( objectIds.contains(o1.getId()) );
        assertTrue( objectIds.contains(o2.getId()) );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to delete objects."
                 );
        assertEquals(
                1,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Shoulda made delete objects call."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                );
    }
    
    @Test
    public void testDeleteObjectsThatDoesNotDeleteAnythingDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        RpcFuture.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                return new RpcResponse<>( 
                                        BeanFactory.newBean( DeleteObjectsResult.class )
                                        .setDaoModified( false ) );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        
        resource.deleteObjects( 
                null, PreviousVersions.DELETE_SPECIFIC_VERSION, new UUID [] { o1.getId(), o2.getId() } );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to delete objects."
                );
        assertEquals(
                0,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Should notta made delete objects call."
                );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testDeleteObjectsEmptyObjectListDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        RpcFuture.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                return new RpcResponse<>( 
                                        BeanFactory.newBean( DeleteObjectsResult.class )
                                        .setDaoModified( false ) );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        resource.deleteObjects( 
                null, PreviousVersions.DELETE_SPECIFIC_VERSION, (UUID[])Array.newInstance( UUID.class, 0 ) );
        
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta delegated to data planner resource to delete objects."
                 );
        assertEquals(
                0,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Should notta made delete objects call."
               );
    }
    
    @Test
    public void testDeleteBucketWithReplicateToDs3DoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

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

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );
        resource.deleteBucket( null, bucket.getId(), false );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to delete bucket."
                );
        assertEquals(
                1,
                client.getRequestCount( DeleteBucketSpectraS3Request.class ),
                "Shoulda made delete bucket call."
               );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testDeleteBucketWithoutReplicateToDs3DoesntReplicate()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

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

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final Ds3DataReplicationRule rule = mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        resource.deleteBucket( null, bucket.getId(), false );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to delete bucket."
               );
        assertEquals(
                0,
                client.getRequestCount( DeleteBucketSpectraS3Request.class ),
                "Should notta made delete bucket call."
                 );
    }
    
    @Test
    public void testCreatePutJobWithoutDs3TargetReplicationDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final UUID instanceId = UUID.randomUUID();
        mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );
        assertNotNull(
                resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking(),
                "Shoulda returned null."
                );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda created job."
                 );
    }
    
    @Test
    public void testCreatePutJobWithDs3TargetReplicationFailsWhenCannotConnectToDs3Target()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                409,
                null );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );

        TestUtil.assertThrows( null, Ds3SdkFailure.valueOf( 409 ), new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking();
            }
        } );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda whacked job."
                 );
    }
    
    @Test
    public void testCreatePutJobWithQuiescedTargetFails()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.updateBean(target.setQuiesced( Quiesced.YES ), ReplicationTarget.QUIESCED );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );

        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking();
            }
        } );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda whacked job."
                 );
    }
    
    @Test
    public void testCreatePutJobForciblyWithDs3TargetReplicationDoesSoWhenCannotConnectToDs3Target()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                409,
                null );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );

        resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setForce( true )
                .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking();
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda created job."
                );
    }
    
    @Test
    public void testCreatePutJobWithDs3TargetReplicationFailsWhenCannotReplicateJobCreation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setObjectsToCreate( new S3ObjectToCreate [] { data } ) );
            }
        } );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Should notta had job."
                );
    }
    
    @Test
    public void testCreatePutJobForcbiblyWithDs3TargetReplicationDoesSoWhenCannotReplicateJobCreation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );
        
        resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setForce( true )
                .setObjectsToCreate( new S3ObjectToCreate [] { data } ) );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda had a job."
                 );
    }
    
    @Test
    public void testCreatePutJobWithDs3TargetReplicationDoesSoJobAlreadyCreatedOnTarget()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

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
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );

        resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking();
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda created job."
                 );
    }
    
    @Test
    public void testCreatePutJobWithDs3TargetReplicationDoesSoWhenCanReplicateJobCreation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse( 
                GetActiveJobSpectraS3Request.class,
                null,
                404,
                null );
        client.setResponse(
                GetBucketSpectraS3Request.class,
                new com.spectralogic.ds3client.models.Bucket(),
                200, 
                null );
        client.setResponse(
                VerifySafeToCreatePutJobSpectraS3Request.class,
                null, 
                200, 
                null );
        client.setResponse(
                ReplicatePutJobSpectraS3Request.class,
                new MasterObjectList(),
                200, 
                null );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );

        resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking();
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda created job."
                 );

        client.getRequest( GetActiveJobSpectraS3Request.class );
        client.getRequest( GetBucketSpectraS3Request.class );
        client.getRequest( VerifySafeToCreatePutJobSpectraS3Request.class );
        client.getRequest( ReplicatePutJobSpectraS3Request.class );
    }
    
    @Test
    public void testCreatePutJobForciblyWithDs3TargetReplicationDoesSoWhenCanReplicateJobCreation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse( 
                GetActiveJobSpectraS3Request.class,
                null,
                404,
                null );
        client.setResponse(
                GetBucketSpectraS3Request.class,
                null, 
                200, 
                null );
        client.setResponse(
                ReplicatePutJobSpectraS3Request.class,
                null, 
                200, 
                null );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );

        resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                .setUserId( bucket.getUserId() )
                .setBucketId( bucket.getId() )
                .setForce( true )
                .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking();
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda created job."
                 );
        
        client.getRequest( GetActiveJobSpectraS3Request.class );
        client.getRequest( GetBucketSpectraS3Request.class );
        client.getRequest( ReplicatePutJobSpectraS3Request.class );
    }
    
    @Test
    public void testCreatePutJobWithDs3TargetReplicationDoesSoWhenJobIsAggregating()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDs3Client client = new MockDs3Client();
        final Ds3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory( client );
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final DataPlannerResource dpResource = mockDataPlannerResource( mockDaoDriver );
        final TargetManagementResource resource = newTargetManagementResourceImpl( 
                rpcServer, 
                ds3ConnectionFactory, 
                dpResource, 
                dbSupport.getServiceManager() );
        
        final S3ObjectToCreate data = 
                BeanFactory.newBean( S3ObjectToCreate.class ).setName( "some/data" ).setSizeInBytes( 12 );
        assertNotNull(
                resource.createPutJob( BeanFactory.newBean( CreatePutJobParams.class )
                        .setUserId( bucket.getUserId() )
                        .setBucketId( bucket.getId() )
                        .setAggregating( true )
                        .setObjectsToCreate( new S3ObjectToCreate [] { data } ) ).getWithoutBlocking(),
                "Shoulda returned null."
                 );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( Job.class ).getCount(),
                "Shoulda created job."
               );
    }
    
    @Test
    public void testCancelGetJobDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                return new HashSet< UUID >();
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

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
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.GET, b1, b2 );
        
        resource.cancelJob( null, entries.iterator().next().getJobId(), false );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                );
        assertEquals(
                1,
                client.getRequestCount( CancelJobSpectraS3Request.class ),
                "Shoulda made cancel job call."
               );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testCancelPutJobDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                mockDaoDriver.deleteAll( Blob.class );
                                mockDaoDriver.deleteAll( S3Object.class );
                                mockDaoDriver.deleteAll( Job.class );
                                return CollectionFactory.toSet( o1.getId(), o2.getId() );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );
        mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

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
        client.setResponse(
                GetBlobPersistenceSpectraS3Request.class,
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( 
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ).toJson(),
                200,
                null );
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );
        
        resource.cancelJob( null, entries.iterator().next().getJobId(), false );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                );
        assertEquals(
                1,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Shoulda made delete objects call."
               );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                );
    }
    
    @Test
    public void testCancelPutJobThatWouldNotWhackObjectsOnTargetAllowedWithoutForceFlag()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                mockDaoDriver.deleteAll( Blob.class );
                                mockDaoDriver.deleteAll( S3Object.class );
                                mockDaoDriver.deleteAll( Job.class );
                                return CollectionFactory.toSet( o1.getId(), o2.getId() );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );
        mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

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
        client.setResponse(
                GetBlobPersistenceSpectraS3Request.class,
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( new BlobPersistence [] {
                        BeanFactory.newBean( BlobPersistence.class ) } ).toJson(),
                200,
                null );
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        resource.cancelJob( null, entries.iterator().next().getJobId(), false );
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                 );
        assertEquals(
                1,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Shoulda made delete objects call."
                );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    
    //NOTE: We normally do replicate deletes without the force flag, but in this case
    //our local objects are not uploaded, only the remote ones, so we require the flag.
    @Test
    public void testCancelPutJobThatWouldWhackObjectsOnTargetNotAllowedWithoutForceFlag()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub ( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                mockDaoDriver.deleteAll( Blob.class );
                                mockDaoDriver.deleteAll( S3Object.class );
                                mockDaoDriver.deleteAll( Job.class );
                                return CollectionFactory.toSet( o1.getId(), o2.getId() );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );
        mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

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
        client.setResponse(
                GetBlobPersistenceSpectraS3Request.class,
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( new BlobPersistence [] {
                        BeanFactory.newBean( BlobPersistence.class )
                        .setChecksum( "value" ).setChecksumType( ChecksumType.values()[ 0 ] ) } ).toJson(),
                200,
                null );
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );
        
        TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.cancelJob( null, entries.iterator().next().getJobId(), false );
            }
        } );
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta delegated to data planner resource to cancel job."
                );

        resource.cancelJob( null, entries.iterator().next().getJobId(), true );
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                );
        assertEquals(
                1,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Shoulda made delete objects call."
                );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testCancelPutJobThatDoesNotDeleteAnyObjectsDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                return new HashSet<>();
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                GetBlobPersistenceSpectraS3Request.class,
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( 
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ).toJson(),
                200,
                null );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );
        
        resource.cancelJob( null, entries.iterator().next().getJobId(), false );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                 );
        assertEquals(
                0,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Should notta made delete objects call."
                );
    }
    
    @Test
    public void testCancelPutJobThatThrowsCancelJobFailedExceptionDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();

        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                mockDaoDriver.deleteAll( Blob.class );
                                mockDaoDriver.deleteAll( S3Object.class );
                                mockDaoDriver.deleteAll( Job.class );
                                throw new CancelJobFailedException(
                                        GenericFailure.values()[ 1 ], 
                                        "Oops.",
                                        CollectionFactory.toSet( o1.getId(), o2.getId() ) );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );
        mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );

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
        client.setResponse(
                GetBlobPersistenceSpectraS3Request.class,
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( 
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ).toJson(),
                200,
                null );
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );

        TestUtil.assertThrows( null, CancelJobFailedException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.cancelJob( null, entries.iterator().next().getJobId(), false );
            }
        } );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                 );
        assertEquals(
                1,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Shoulda made delete objects call."
                 );
        assertTrue(
                client.isShutdown(),
                "Shoulda shutdown connection."
                 );
    }
    
    @Test
    public void testCancelPutJobThatThrowsCancelJobFailedExceptionThatDoesNotDeleteAnyObjectsDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                throw new CancelJobFailedException( 
                                        GenericFailure.values()[ 1 ], 
                                        "Oops.", 
                                        new HashSet< UUID >() );
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        client.setResponse(
                GetBlobPersistenceSpectraS3Request.class,
                BeanFactory.newBean( BlobPersistenceContainer.class ).setBlobs( 
                        (BlobPersistence[])Array.newInstance( BlobPersistence.class, 0 ) ).toJson(),
                200,
                null );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        mockDaoDriver.createDs3DataReplicationRule(
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.PUT, b1, b2 );
        
        TestUtil.assertThrows( null, CancelJobFailedException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.cancelJob( null, entries.iterator().next().getJobId(), false );
            }
        } );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                );
        assertEquals(
                0,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Should notta made delete objects call."
                );
    }
    
    @Test
    public void testCancelVerifyJobDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType(
                        Set.class, 
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                return new HashSet<>();
                            }
                        }, null ) );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, btih ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target" );
        mockDaoDriver.createDs3Target( UUID.randomUUID(), "target2" );

        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                200,
                null );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        mockDaoDriver.createDs3DataReplicationRule( 
                bucket.getDataPolicyId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> entries = mockDaoDriver.createJobWithEntries( JobRequestType.VERIFY, b1, b2 );
        
        resource.cancelJob( null, entries.iterator().next().getJobId(), false );
        
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to data planner resource to cancel job."
                 );
        assertEquals(
                0,
                client.getRequestCount( DeleteObjectsRequest.class ),
                "Should notta made delete objects call."
                );
    }
    
    
    private DataPlannerResource mockDataPlannerResource( final MockDaoDriver mockDaoDriver )
    {
        final Method methodCreateJob = ReflectUtil.getMethod( JobResource.class, "createPutJob" );
        final Method methodCancelJob = 
                ReflectUtil.getMethod( DataPlannerResource.class, "cancelJobInternal" );
        return InterfaceProxyFactory.getProxy( DataPlannerResource.class, MockInvocationHandler.forMethod(
                methodCreateJob, 
                new InvocationHandler()
                {
                    public Object invoke( 
                            final Object proxy, 
                            final Method method,
                            final Object[] args ) throws Throwable
                    {
                        final CreatePutJobParams params = (CreatePutJobParams)args[ 0 ];
                        final Job job = mockDaoDriver.createJob(
                                params.getBucketId(), params.getUserId(), JobRequestType.PUT );
                        return new RpcResponse<>( job.getId() );
                    }
                }, 
                MockInvocationHandler.forMethod( 
                        methodCancelJob,
                        new InvocationHandler()
                        {
                            public Object invoke( 
                                    final Object proxy, 
                                    final Method method,
                                    final Object[] args ) throws Throwable
                            {
                                mockDaoDriver.deleteAll( Job.class );
                                return CollectionFactory.toSet( UUID.randomUUID(), UUID.randomUUID() );
                            }
                        },
                        null ) ) );
    }
        
    @Test
    public void testModifyDs3TargetPropsOnlyModifyableOnlineTargetOffline()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final UUID instanceId = UUID.randomUUID();
        final SystemInformation systemInformation = MockDs3Client.createSystemInformation( instanceId );
        client.setResponse( 
                GetSystemInformationSpectraS3Request.class,
                systemInformation,
                503,
                null );
        
        final Group group = new Group();
        client.setResponse( 
                VerifyUserIsMemberOfGroupSpectraS3Request.class,
                group,
                200,
                null );
        
        final String [] hardcodePropsInUnitTest = new String[] { 
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
                ReplicationTarget.QUIESCED };
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target1" );
        
        final Quiesced origQuiesced = target.getQuiesced();
        final Quiesced newQuiesced = Quiesced.PENDING;
        assertNotSame(
                origQuiesced,
                newQuiesced,
                "check default mock values "  );
        
        final boolean origPermitOutOfSync = target.isPermitGoingOutOfSync();       
        final boolean newPermitOutOfSync = !origPermitOutOfSync;
        
        final TargetReadPreferenceType origDefaultReadPref = target.getDefaultReadPreference();
        final TargetReadPreferenceType newDefaultReadPref = TargetReadPreferenceType.AFTER_NEARLINE_POOL;
        assertNotSame(
                origDefaultReadPref, newDefaultReadPref,
                "check default mock values " );
        
        target.setDefaultReadPreference( newDefaultReadPref );
        target.setPermitGoingOutOfSync( newPermitOutOfSync );
        target.setQuiesced( newQuiesced );
        
        resource.modifyDs3Target( target, hardcodePropsInUnitTest );
        
        final Ds3Target targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                newQuiesced,
                targetFromDb.getQuiesced(),
                "Should update these props because we do not care we cannot connect to target."
                 );
        assertEquals(
                newPermitOutOfSync,
                targetFromDb.isPermitGoingOutOfSync(),
                "Should update these props because we do not care we cannot connect to target."
                 );
        assertEquals(
                newDefaultReadPref,
                targetFromDb.getDefaultReadPreference(),
                "Should update these props because we do not care we cannot connect to target."
                 );
    }
    
    @Test
    public void testModifyDs3TargetPropertyBothModifyableOnlineAndOfflineTargetOnline()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
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
        
        final String [] hardcodePropsInUnitTest = new String[] { 
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
                ReplicationTarget.QUIESCED, 
                Ds3Target.DATA_PATH_END_POINT };

        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target1" );

        final Quiesced origQuiesced = target.getQuiesced();
        final Quiesced newQuiesced = Quiesced.PENDING;
        assertNotSame(
                origQuiesced,
                newQuiesced,
                "check default mock values "  );
        
        final boolean origPermitOutOfSync = target.isPermitGoingOutOfSync();       
        final boolean newPermitOutOfSync = !origPermitOutOfSync;
        
        final TargetReadPreferenceType origDefaultReadPref = target.getDefaultReadPreference();
        final TargetReadPreferenceType newDefaultReadPref = TargetReadPreferenceType.AFTER_NEARLINE_POOL;
        assertNotSame(
                origDefaultReadPref,
                newDefaultReadPref,
                "check default mock values " );
        
        target.setDataPathEndPoint( "10.2.4.5" );
        target.setDefaultReadPreference( newDefaultReadPref );
        target.setPermitGoingOutOfSync( newPermitOutOfSync );
        target.setQuiesced( newQuiesced );        
        
        resource.modifyDs3Target( target, hardcodePropsInUnitTest );
        
        final Ds3Target targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                "10.2.4.5", targetFromDb.getDataPathEndPoint(),
                "Shoulda updated ip addr." );
        assertEquals(
                newDefaultReadPref, targetFromDb.getDefaultReadPreference(),
                "Shoulda updated target read prefs."
                 );
        assertEquals(
                newPermitOutOfSync, targetFromDb.isPermitGoingOutOfSync(),
                        "Shoulda updated going out of sync."
                );
        assertEquals(
                newQuiesced, targetFromDb.getQuiesced(),
                "Shoulda updated quiesced." );

        assertTrue( client.isShutdown(),
                "Shoulda shutdown connection." );
    }

    @Test
    public void testModifyDs3TargetPropsAllModifyableOfflineTargetOnline()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockDs3Client client = new MockDs3Client();
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new MockDs3ConnectionFactory( client ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
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
        
        final String [] hardcodePropsInUnitTest = new String[] { 
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
                ReplicationTarget.QUIESCED };
        final Ds3Target target = mockDaoDriver.createDs3Target( instanceId, "target1" );
        
        final Quiesced origQuiesced = target.getQuiesced();
        final Quiesced newQuiesced = Quiesced.PENDING;
        assertNotSame(
                origQuiesced, newQuiesced,
                "check default mock values " );
        
        final boolean origPermitOutOfSync = target.isPermitGoingOutOfSync();       
        final boolean newPermitOutOfSync = !origPermitOutOfSync;
        
        final TargetReadPreferenceType origDefaultReadPref = target.getDefaultReadPreference();
        final TargetReadPreferenceType newDefaultReadPref = TargetReadPreferenceType.AFTER_NEARLINE_POOL;
        assertNotSame(
                origDefaultReadPref, newDefaultReadPref,
                "check default mock values " );
        
        target.setDefaultReadPreference( newDefaultReadPref );
        target.setPermitGoingOutOfSync( newPermitOutOfSync );
        target.setQuiesced( newQuiesced );        
        
        resource.modifyDs3Target( target, hardcodePropsInUnitTest );
        
        final Ds3Target targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                newDefaultReadPref,
                targetFromDb.getDefaultReadPreference(),
                "Shoulda updated target read prefs."
                 );
        assertEquals(
                newPermitOutOfSync,
                targetFromDb.isPermitGoingOutOfSync(),
                "Shoulda updated going out of sync."
                 );
        assertEquals(  newQuiesced,
                targetFromDb.getQuiesced(),
                "Shoulda updated quiesced." );
    }
    
    @Test
    public void testRegisterAzureTargetConflictingDueToNameAlreadyRegisteredNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target" );
        final AzureTarget target2 = BeanFactory.newBean( AzureTarget.class );
        BeanCopier.copy( target2, target );
        target2.setId( UUID.randomUUID() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.registerAzureTarget( target2 );
            }
        } );
    }
    
    @Test
    public void testRegisterAzureTargetDoesSoIfCanConnect()
    {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }
        
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new DefaultAzureConnectionFactory( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final AzureTarget target = BeanFactory.newBean( AzureTarget.class )
                .setAccountName( PublicCloudSupport.AZURE_ACCOUNT_NAME )
                .setAccountKey( PublicCloudSupport.AZURE_BAD_ACCOUNT_KEY )
                .setName( "target" );
        target.setId( UUID.randomUUID() );
        TestUtil.assertThrows( 
                null,
                AzureSdkFailure.valueOf( PublicCloudSupport.AZURE_403_FAILURE, 403 ) ,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.registerAzureTarget( target );
                    }
                } );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( AzureTarget.class ).getCount(),
                "Should notta been target created."
                );
        
        target.setAccountKey( PublicCloudSupport.AZURE_ACCOUNT_KEY );
        resource.registerAzureTarget( target );
        
        mockDaoDriver.attainOneAndOnly( AzureTarget.class );
    }
    
    @Test
    public void testModifyAzureTargetPropsModifyableWhileTargetOffline()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new DefaultAzureConnectionFactory( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final String [] hardcodePropsInUnitTest = new String[] { 
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
                ReplicationTarget.QUIESCED };
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        
        final Quiesced origQuiesced = target.getQuiesced();
        final Quiesced newQuiesced = Quiesced.PENDING;
        assertNotSame( origQuiesced, newQuiesced ,
                "check default mock values ");
        
        final boolean origPermitOutOfSync = target.isPermitGoingOutOfSync();       
        final boolean newPermitOutOfSync = !origPermitOutOfSync;
        
        final TargetReadPreferenceType origDefaultReadPref = target.getDefaultReadPreference();
        final TargetReadPreferenceType newDefaultReadPref = TargetReadPreferenceType.AFTER_NEARLINE_POOL;
        assertNotSame( origDefaultReadPref, newDefaultReadPref,
                "check default mock values " );
        
        target.setDefaultReadPreference( newDefaultReadPref );
        target.setPermitGoingOutOfSync( newPermitOutOfSync );
        target.setQuiesced( newQuiesced );
        
        resource.modifyAzureTarget( target, hardcodePropsInUnitTest );
        
        final AzureTarget targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                newQuiesced, targetFromDb.getQuiesced(),
                "Should update these props because we do not care we cannot connect to target."
                 );
        assertEquals( newPermitOutOfSync, targetFromDb.isPermitGoingOutOfSync(),
                "Should update these props because we do not care we cannot connect to target."
                 );
        assertEquals( newDefaultReadPref, targetFromDb.getDefaultReadPreference(),
                "Should update these props because we do not care we cannot connect to target."
                 );
    }
    
    @Test
    public void testModifyAzureTargetPropertyBothModifyableOnlineAndOfflineTargetOnline()
    {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }
        
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new DefaultAzureConnectionFactory( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final String [] hardcodePropsInUnitTest = new String[] { 
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
                ReplicationTarget.QUIESCED, 
                AzureTarget.ACCOUNT_KEY };

        final AzureTarget target = PublicCloudSupport.createAzureTarget( mockDaoDriver );
        
        final TargetReadPreferenceType origDefaultReadPref = target.getDefaultReadPreference();
        final TargetReadPreferenceType newDefaultReadPref = TargetReadPreferenceType.AFTER_NEARLINE_POOL;
        assertNotSame(
                origDefaultReadPref, newDefaultReadPref,
                "check default mock values "  );
        
        target.setAccountKey( PublicCloudSupport.AZURE_BAD_ACCOUNT_KEY );
        target.setDefaultReadPreference( newDefaultReadPref );
        
        TestUtil.assertThrows( 
                null,
                AzureSdkFailure.valueOf( PublicCloudSupport.AZURE_403_FAILURE, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.modifyAzureTarget( target, hardcodePropsInUnitTest );
                    }
                } );
        
        AzureTarget targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                PublicCloudSupport.AZURE_ACCOUNT_KEY,
                targetFromDb.getAccountKey(),
                "Should notta updated."
                );
        assertEquals(
                origDefaultReadPref,
                targetFromDb.getDefaultReadPreference(),
                "Should notta updated."
                 );

        target.setAccountKey( PublicCloudSupport.AZURE_ACCOUNT_KEY );
        resource.modifyAzureTarget( target, hardcodePropsInUnitTest );
        targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                PublicCloudSupport.AZURE_ACCOUNT_KEY,
                targetFromDb.getAccountKey(),
                "Shoulda updated."
                 );
        assertEquals(
                newDefaultReadPref,
                targetFromDb.getDefaultReadPreference(),
                "Shoulda updated."
                 );
    }
    
    @Test
    public void testRegisterS3TargetConflictingDueToNameAlreadyRegisteredNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final S3Target target = mockDaoDriver.createS3Target( "target" );
        final S3Target target2 = BeanFactory.newBean( S3Target.class );
        BeanCopier.copy( target2, target );
        target2.setId( UUID.randomUUID() );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.registerS3Target( target2 );
            }
        } );
    }
    
    @Test
    public void testRegisterS3TargetDoesSoIfCanConnect()
    {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }
        
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new DefaultS3ConnectionFactory( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final S3Target target = BeanFactory.newBean( S3Target.class )
                .setAccessKey( PublicCloudSupport.S3_ACCESS_KEY )
                .setSecretKey( PublicCloudSupport.S3_BAD_SECRET_KEY )
                .setName( "target" );
        target.setId( UUID.randomUUID() );
        TestUtil.assertThrows(
                null, 
                S3SdkFailure.valueOf( PublicCloudSupport.S3_403_AUTHENTICATION, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.registerS3Target( target );
                    }
                } );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( S3Target.class ).getCount(),
                "Should notta been target created."
                 );
        
        target.setSecretKey( PublicCloudSupport.S3_SECRET_KEY );
        target.setHttps(false);
        target.setRegion(S3Region.US_EAST_1);
        target.setDataPathEndPoint(PublicCloudSupport.S3_ENDPOINT);
        resource.registerS3Target( target );
        
        mockDaoDriver.attainOneAndOnly( S3Target.class );
    }
    
    @Test
    public void testModifyS3TargetPropsModifyableWhileTargetOffline()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new DefaultS3ConnectionFactory( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final String [] hardcodePropsInUnitTest = new String[] { 
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
                ReplicationTarget.QUIESCED };
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        
        final Quiesced origQuiesced = target.getQuiesced();
        final Quiesced newQuiesced = Quiesced.PENDING;
        assertNotSame(
                origQuiesced, newQuiesced,
                "check default mock values " );
        
        final boolean origPermitOutOfSync = target.isPermitGoingOutOfSync();       
        final boolean newPermitOutOfSync = !origPermitOutOfSync;
        
        final TargetReadPreferenceType origDefaultReadPref = target.getDefaultReadPreference();
        final TargetReadPreferenceType newDefaultReadPref = TargetReadPreferenceType.AFTER_NEARLINE_POOL;
        assertNotSame(
                origDefaultReadPref, newDefaultReadPref,
                "check default mock values "  );
        
        target.setDefaultReadPreference( newDefaultReadPref );
        target.setPermitGoingOutOfSync( newPermitOutOfSync );
        target.setQuiesced( newQuiesced );
        
        resource.modifyS3Target( target, hardcodePropsInUnitTest );
        
        final S3Target targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                newQuiesced, targetFromDb.getQuiesced(),
                "Should update these props because we do not care we cannot connect to target."
                 );
        assertEquals(
                newPermitOutOfSync, targetFromDb.isPermitGoingOutOfSync(),
                "Should update these props because we do not care we cannot connect to target."
                 );
        assertEquals(
                newDefaultReadPref, targetFromDb.getDefaultReadPreference(),
                "Should update these props because we do not care we cannot connect to target."
                 );
    }
    
    @Test
    public void testModifyS3TargetPropertyBothModifyableOnlineAndOfflineTargetOnline()
    {
        if ( !PublicCloudSupport.isPublicCloudSupported() )
        {
            return;
        }
        
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                new DefaultS3ConnectionFactory( dbSupport.getServiceManager() ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        final String [] hardcodePropsInUnitTest = new String[] { 
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC, 
                ReplicationTarget.QUIESCED, 
                S3Target.SECRET_KEY };

        final S3Target target = PublicCloudSupport.createS3Target( mockDaoDriver );
        
        final TargetReadPreferenceType origDefaultReadPref = target.getDefaultReadPreference();
        final TargetReadPreferenceType newDefaultReadPref = TargetReadPreferenceType.AFTER_NEARLINE_POOL;
        assertNotSame(
                origDefaultReadPref,
                newDefaultReadPref,
                "check default mock values " );
        
        target.setSecretKey( PublicCloudSupport.S3_BAD_SECRET_KEY );
        target.setDefaultReadPreference( newDefaultReadPref );
        
        TestUtil.assertThrows( 
                null,
                S3SdkFailure.valueOf( PublicCloudSupport.S3_403_AUTHENTICATION, 403),
               // S3SdkFailure.valueOf( PublicCloudSupport.S3_403_FAILURE, 403 ),
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        resource.modifyS3Target( target, hardcodePropsInUnitTest );
                    }
                } );
        
        S3Target targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                PublicCloudSupport.S3_SECRET_KEY,
                targetFromDb.getSecretKey(),
                "Should notta updated."
                );
        assertEquals(
                origDefaultReadPref,
                targetFromDb.getDefaultReadPreference(),
                "Should notta updated."
                 );

        target.setSecretKey( PublicCloudSupport.S3_SECRET_KEY );
        target.setAccessKey( PublicCloudSupport.S3_ACCESS_KEY );
        target.setHttps(false);
        target.setRegion(S3Region.US_EAST_1);
        target.setDataPathEndPoint(PublicCloudSupport.S3_ENDPOINT);
        resource.modifyS3Target( target, hardcodePropsInUnitTest );
        targetFromDb = mockDaoDriver.attain( target );
        assertEquals(
                PublicCloudSupport.S3_SECRET_KEY,
                targetFromDb.getSecretKey(),
                "Shoulda updated."
                 );
        assertEquals(
                newDefaultReadPref,
                targetFromDb.getDefaultReadPreference(),
                "Shoulda updated."
                 );
    }
    
    @Test
    public void testVerifyAzureTargetWithoutFullDetailsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );

        final BlobOnMedia b1 = discoverableSegments.get( 0 ).getObjects()[ 0 ].getBlobs()[ 0 ];
        b1.setId( UUID.randomUUID() );
        
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        resource.verifyAzureTarget( target.getId(), false );
        
        connection.assertNoDeletesRequested( false );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( SuspectBlobAzureTarget.class ).getCount(),
                "Should notta recorded suspect blob loss."
                 );
        
        resource.verifyPublicCloudTarget( target.getClass(), target.getId(), false );
        
        connection.assertNoDeletesRequested( false );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( SuspectBlobAzureTarget.class ).getCount(),
                "Should notta recorded suspect blob loss."
                 );
    }
    
    @Test
    public void testVerifyAzureTargetWithFullDetailsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );

        final BlobOnMedia b1 = discoverableSegments.get( 0 ).getObjects()[ 0 ].getBlobs()[ 0 ];
        b1.setId( UUID.randomUUID() );
        
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        resource.verifyAzureTarget( target.getId(), true );

        final TargetFailure failure = mockDaoDriver.waitForFailure(AzureTargetFailure.class);

        assertEquals(failure.getType(), TargetFailureType.VERIFY_COMPLETE);
        
        connection.assertDeletesRequestedCorrectly( new HashSet< UUID >() );

        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( SuspectBlobAzureTarget.class ).getCount(),
                "Shoulda recorded suspect blob loss."
                 );
    }
    
    @Test
    public void testDeleteBucketWithReplicateToS3DoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final List< BucketOnPublicCloud > discoverableSegments = 
                MockS3Connection.createDiscoverableSegments( dbSupport, target, 1 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        final MockS3Connection connection = 
                new MockS3Connection( dbSupport.getServiceManager(), discoverableSegments );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        connection.expectDeleteBucketCalls();
        resource.deleteBucket( null, bucket.getId(), true );
        connection.assertDeleteBucketCallCountEquals( 1 );
    }
    
    @Test
    public void testCreateBucketReplicatesToS3()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        final MockS3Connection connection = 
                new MockS3Connection( dbSupport.getServiceManager(), new ArrayList<>() );
        connection.cloudBucketDoesNotExist();
        final Bucket bucket = BeanFactory.newBean( Bucket.class );
        bucket.setName( "b1" );
        bucket.setDataPolicyId( dataPolicy.getId() );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        connection.expectCreateBucketCalls();
        resource.createBucket(bucket);
        connection.assertCreateBucketCallCountEquals( 1 );
    }
    
    @Test
    public void testCreateBucketReplicatesToAzure()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), new ArrayList<>() );
        connection.cloudBucketDoesNotExist();
        final Bucket bucket = BeanFactory.newBean( Bucket.class );
        bucket.setName( "b1" );
        bucket.setDataPolicyId( dataPolicy.getId() );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        connection.expectCreateBucketCalls();
        resource.createBucket(bucket);
        connection.assertCreateBucketCallCountEquals( 1 );
    }
    
    @Test
    public void testDoNotCreateBucketWhenCreateCloudBucketFails()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final List< BucketOnPublicCloud > discoverableSegments = 
                MockS3Connection.createDiscoverableSegments( dbSupport, target, 1 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        final MockS3Connection connection = 
                new MockS3Connection( dbSupport.getServiceManager(), discoverableSegments );
        connection.cloudBucketDoesNotExist();
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        //NOTE: we expect an unsupported operation exception because that's what a mock
        //public cloud connection throws when we do not tell it to expect create bucket calls.
        //Realistically, we would expect a 403, 409, or some other exception originating in
        //the SDK of the cloud target we failed to create a bucket on. - Kyle Hughart 5/25/2017         
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                resource.createBucket(bucket);
            }
        } );
        
    }
    
    @Test
    public void testDeleteBucketWithoutReplicateToS3DoesntReplicate()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final List< BucketOnPublicCloud > discoverableSegments = 
                MockS3Connection.createDiscoverableSegments( dbSupport, target, 1 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final S3DataReplicationRule rule = mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockS3Connection connection = 
                new MockS3Connection( dbSupport.getServiceManager(), discoverableSegments );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        resource.deleteBucket( null, bucket.getId(), true );
        connection.assertDeleteBucketCallCountEquals( 0 );
    }
    
    @Test
    public void testDeleteBucketWithReplicateToAzureDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 1 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        connection.expectDeleteBucketCalls();
        resource.deleteBucket( null, bucket.getId(), true );
        connection.assertDeleteBucketCallCountEquals( 1 );
    }
    
    @Test
    public void testDeleteBucketWithoutReplicateToAzureDoesntReplicate()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 1 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "b1" );
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        resource.deleteBucket( null, bucket.getId(), true );
        connection.assertDeleteBucketCallCountEquals( 0 );
    }
    
    @Test
    public void testVerifyS3TargetWithoutFullDetailsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments = 
                MockS3Connection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final S3DataReplicationRule rule = mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockS3Connection connection = 
                new MockS3Connection( dbSupport.getServiceManager(), discoverableSegments );

        final BlobOnMedia b1 = discoverableSegments.get( 0 ).getObjects()[ 0 ].getBlobs()[ 0 ];
        b1.setId( UUID.randomUUID() );
        
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        resource.verifyS3Target( target.getId(), false );
        
        connection.assertNoDeletesRequested( false );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( SuspectBlobS3Target.class ).getCount(),
                "Should notta recorded suspect blob loss."
                );
        
        resource.verifyPublicCloudTarget( target.getClass(), target.getId(), false );
        
        connection.assertNoDeletesRequested( false );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( SuspectBlobAzureTarget.class ).getCount(),
                "Should notta recorded suspect blob loss."
                 );
    }
    
    @Test
    public void testVerifyS3TargetWithFullDetailsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments = 
                MockS3Connection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final S3DataReplicationRule rule = mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockS3Connection connection = 
                new MockS3Connection( dbSupport.getServiceManager(), discoverableSegments );

        final BlobOnMedia b1 = discoverableSegments.get( 0 ).getObjects()[ 0 ].getBlobs()[ 0 ];
        b1.setId( UUID.randomUUID() );
        
        final TargetManagementResourceImpl resource = newTargetManagementResourceImpl(
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                connection.toConnectionFactory(),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                dbSupport.getServiceManager() );
        
        resource.verifyS3Target( target.getId(), true );

        final TargetFailure failure = mockDaoDriver.waitForFailure(S3TargetFailure.class);

        assertEquals(failure.getType(), TargetFailureType.VERIFY_COMPLETE);
        
        connection.assertDeletesRequestedCorrectly( new HashSet< UUID >() );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( SuspectBlobS3Target.class ).getCount(),
                "Shoulda recorded suspect blob loss."
               );
    }
    
    @Test
    public void testImportAzureTargetDelegatesToImporter()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TargetManagementResourceImpl resource = new TargetManagementResourceImpl( 
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                InterfaceProxyFactory.getProxy( DataPolicyManagementResource.class, null ),
                this.<ImportAzureTargetDirective>newPublicCloudTargetImporter( btih ),
                this.<ImportS3TargetDirective>newPublicCloudTargetImporter(),
                dbSupport.getServiceManager() );

        resource.importS3Target( null );
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta delegated to import handler yet."
               );
        resource.importAzureTarget( null );
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to import handler."
                 );
    }
    
    @Test
    public void testImportS3TargetDelegatesToImporter()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TargetManagementResourceImpl resource = new TargetManagementResourceImpl( 
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, null ),
                InterfaceProxyFactory.getProxy( DataPolicyManagementResource.class, null ),
                this.<ImportAzureTargetDirective>newPublicCloudTargetImporter(),
                this.<ImportS3TargetDirective>newPublicCloudTargetImporter( btih ),
                dbSupport.getServiceManager() );

        resource.importAzureTarget( null );
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta delegated to import handler yet."
                 );
        resource.importS3Target( null );
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda delegated to import handler."
                 );
    }
    
    
    private TargetManagementResourceImpl newTargetManagementResourceImpl( 
            final RpcServer rpcServer, 
            final Ds3ConnectionFactory ds3ConnectionFactory,
            final DataPlannerResource dataPlannerResource,
            final BeansServiceManager serviceManager )
    {
        return new TargetManagementResourceImpl( 
                rpcServer,
                ds3ConnectionFactory,
                InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
                dataPlannerResource,
                InterfaceProxyFactory.getProxy( DataPolicyManagementResource.class, null ),
                this.<ImportAzureTargetDirective>newPublicCloudTargetImporter(),
                this.<ImportS3TargetDirective>newPublicCloudTargetImporter(),
                serviceManager );
    }
    
    
    private TargetManagementResourceImpl newTargetManagementResourceImpl( 
            final RpcServer rpcServer, 
            final AzureConnectionFactory azureConnectionFactory,
            final DataPlannerResource dataPlannerResource,
            final BeansServiceManager serviceManager )
    {
        return new TargetManagementResourceImpl( 
                rpcServer,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                azureConnectionFactory,
                InterfaceProxyFactory.getProxy( S3ConnectionFactory.class, null ),
                dataPlannerResource,
                InterfaceProxyFactory.getProxy( DataPolicyManagementResource.class, null ),
                this.<ImportAzureTargetDirective>newPublicCloudTargetImporter(),
                this.<ImportS3TargetDirective>newPublicCloudTargetImporter(),
                serviceManager );
    }
    
    
    private TargetManagementResourceImpl newTargetManagementResourceImpl( 
            final RpcServer rpcServer, 
            final S3ConnectionFactory s3ConnectionFactory,
            final DataPlannerResource dataPlannerResource,
            final BeansServiceManager serviceManager )
    {
        return new TargetManagementResourceImpl( 
                rpcServer,
                InterfaceProxyFactory.getProxy( Ds3ConnectionFactory.class, null ),
                InterfaceProxyFactory.getProxy( AzureConnectionFactory.class, null ),
                s3ConnectionFactory,
                dataPlannerResource,
                InterfaceProxyFactory.getProxy( DataPolicyManagementResource.class, null ),
                this.<ImportAzureTargetDirective>newPublicCloudTargetImporter(),
                this.<ImportS3TargetDirective>newPublicCloudTargetImporter(),
                serviceManager );
    }
    
    
    private < ID extends ImportPublicCloudTargetDirective< ID > > 
    PublicCloudTargetImportScheduler< ID > newPublicCloudTargetImporter()
    {
        @SuppressWarnings( "unchecked" )
        final PublicCloudTargetImportScheduler< ID > retval = 
                InterfaceProxyFactory.getProxy( PublicCloudTargetImportScheduler.class, null );
        return retval;
    }
    
    
    private < ID extends ImportPublicCloudTargetDirective< ID > > 
    PublicCloudTargetImportScheduler< ID > newPublicCloudTargetImporter( 
            final BasicTestsInvocationHandler btih )
    {
        @SuppressWarnings( "unchecked" )
        final PublicCloudTargetImportScheduler< ID > retval = 
                InterfaceProxyFactory.getProxy( PublicCloudTargetImportScheduler.class, btih );
        return retval;
    }
    
    
    private abstract static class DeleteObjectsIh implements InvocationHandler
    {      
    	
    	public abstract void validate( DeleteObjectsRequest request );
    	
        public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
        {
            if ( null == args || null == args[ 0 ] || !DeleteObjectsRequest.class.isAssignableFrom( args[ 0 ].getClass() ) )
            {
                return null;
            }
            
            validate( ( DeleteObjectsRequest )( args[0] ) );
            return null;
        }
    } // end inner class def
}
