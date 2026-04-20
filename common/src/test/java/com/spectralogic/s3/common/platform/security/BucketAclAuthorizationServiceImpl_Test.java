/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BucketAclAuthorizationServiceImpl_Test 
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BucketAclAuthorizationServiceImpl(
                        null,
                        new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                        1 );
            }
        } );
    }
    

    @Test
    public void testConstructorNullEffectiveGroupMembershipsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BucketAclAuthorizationServiceImpl(
                        dbSupport.getServiceManager(),
                        null,
                        1 );
            }
        } );
    }
    

    @Test
    public void testVerifyHasAccessAdministratorOverrideWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.deleteAll( BucketAcl.class );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        /*
         * Verify administrator override works
         */
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket1.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.YES,
                new BucketAccessRequest( user1.getId(), bucket1.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user4.getId(), bucket1.getId(), BucketAclPermission.LIST ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.YES,
                new BucketAccessRequest( user4.getId(), bucket1.getId(), BucketAclPermission.LIST ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase1()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase2()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), group1.getId(), null, BucketAclPermission.LIST );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase3()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), group2.getId(), null, BucketAclPermission.LIST );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase4()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), group3.getId(), null, BucketAclPermission.LIST );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase5()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), group4.getId(), null, BucketAclPermission.LIST );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase6()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user2.getId(), BucketAclPermission.LIST );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase7()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user3.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket.getId(), group3.getId(), null, BucketAclPermission.LIST );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase8()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user3.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket.getId(), group3.getId(), null, BucketAclPermission.OWNER );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    

    @Test
    public void testVerifyHasAccessDoesSoCase9()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group group1 = mockDaoDriver.createGroup( "group1" );
        final Group group2 = mockDaoDriver.createGroup( "group2" );
        final Group group3 = mockDaoDriver.createGroup( "group3" );
        final Group group4 = mockDaoDriver.createGroup( "group4" );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group1.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group2.getId() );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user3.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( null, group3.getId(), null, BucketAclPermission.OWNER );
        
        final BucketAclAuthorizationServiceImpl service = new BucketAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user1.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user2.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
        assertPermissionGranted(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.LIST ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.READ ) );
        assertPermissionDenied(
                service,
                AdministratorOverride.NO,
                new BucketAccessRequest( user3.getId(), bucket.getId(), BucketAclPermission.OWNER ) );
    }
    
    
    private void assertPermissionGranted( 
            final BucketAclAuthorizationServiceImpl service, 
            final AdministratorOverride administratorOverride,
            final BucketAccessRequest request )
    {
        service.verifyHasAccess( request, administratorOverride );
    }
    
    
    private void assertPermissionDenied( 
            final BucketAclAuthorizationServiceImpl service, 
            final AdministratorOverride administratorOverride,
            final BucketAccessRequest request )
    {
        TestUtil.assertThrows( null, GenericFailure.FORBIDDEN, new BlastContainer()
        {
            public void test()
                {
                    service.verifyHasAccess( request, administratorOverride );
                }
            } );
    }
}
