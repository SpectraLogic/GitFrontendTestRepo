/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class GroupMembershipCalculator_Test 
{
    @Test
    public void testConstructorNullRetrieverManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

            public void test()
            {
                new GroupMembershipCalculator( null );
            }
        } );
    }
    
    
    @Test
    public void testAddGroupMemberNullMemberNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final User user = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
           public void test()
            {
                memberships.addGroupMember( null );
            }
        } );
    }
    
    
    @Test
    public void testAddGroupMemberAfterMembershipsCalculatedNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final User user = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        memberships.getGroups( user.getId() );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {

             public void test()
            {
                memberships.addGroupMember( BeanFactory.newBean( GroupMember.class ) );
            }
        } );
    }
    
    
    @Test
    public void testGetMembershipsWhenUserIdUnknownNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final User user = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

           public void test()
            {
                memberships.getGroups( UUID.randomUUID() );
            }
        } );
    }
    
    
    @Test
    public void testGetMembershipsWorksBasic()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final User user = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        assertEquals(1,  memberships.getGroups(user.getId()).size(), "Shoulda had single membership in everyone group.");
    }
    
    
    @Test
    public void testGetMembershipsWorksComplex1()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user1 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        final Group groupE = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                BuiltInGroup.EVERYONE );
        final Group group1 = mockDaoDriver.createGroup( "g1" );
        final Group group2 = mockDaoDriver.createGroup( "g2" );
        final Group group3 = mockDaoDriver.createGroup( "g3" );
        final Group group4 = mockDaoDriver.createGroup( "g4" );
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), group2.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group3.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group4.getId() );
        mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user3.getId() );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        final Object expected2 = CollectionFactory.toSet( groupE.getId(), group1.getId() );
        assertEquals(expected2, memberships.getGroups( user1.getId() ), "Shoulda had memberships in correct groups.");
        final Object expected1 = CollectionFactory.toSet( groupE.getId(), group1.getId(), group2.getId(), group3.getId() );
        assertEquals(expected1, memberships.getGroups( user2.getId() ), "Shoulda had memberships in correct groups.");
        final Object expected = CollectionFactory.toSet(
                groupE.getId(), group1.getId(), group2.getId(), group3.getId(), group4.getId() );
        assertEquals(expected, memberships.getGroups( user3.getId() ), "Shoulda had memberships in correct groups.");
    }
    
    
    @Test
    public void testGetMembershipsWorksComplex2()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user1 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        final Group groupE = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                BuiltInGroup.EVERYONE );
        final Group group1 = mockDaoDriver.createGroup( "g1" );
        final Group group2 = mockDaoDriver.createGroup( "g2" );
        final Group group3 = mockDaoDriver.createGroup( "g3" );
        final Group group4 = mockDaoDriver.createGroup( "g4" );
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), group2.getId() );
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), group3.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group3.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group4.getId() );
        mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user3.getId() );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        final Object expected2 = CollectionFactory.toSet( groupE.getId(), group1.getId(), group2.getId(),
                                 group3.getId(), group4.getId() );
        assertEquals(expected2, memberships.getGroups( user1.getId() ), "Shoulda had memberships in correct groups.");
        final Object expected1 = CollectionFactory.toSet( groupE.getId(), group1.getId(), group2.getId(), group3.getId() );
        assertEquals(expected1, memberships.getGroups( user2.getId() ), "Shoulda had memberships in correct groups.");
        final Object expected = CollectionFactory.toSet( groupE.getId(), group1.getId(), group2.getId(), group3.getId(),
                group4.getId() );
        assertEquals(expected, memberships.getGroups( user3.getId() ), "Shoulda had memberships in correct groups.");
    }
    
    
    @Test
    public void testGetMembershipsWorksComplex3()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user1 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        final Group groupE = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                BuiltInGroup.EVERYONE );
        final Group group1 = mockDaoDriver.createGroup( "g1" );
        final Group group2 = mockDaoDriver.createGroup( "g2" );
        final Group group3 = mockDaoDriver.createGroup( "g3" );
        final Group group4 = mockDaoDriver.createGroup( "g4" );
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), group2.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group3.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group4.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group4.getId() );
        mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user3.getId() );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        final Object expected2 = CollectionFactory.toSet( groupE.getId(), group1.getId(), group2.getId(),
                                 group3.getId() );
        assertEquals(expected2, memberships.getGroups( user1.getId() ), "Shoulda had memberships in correct groups.");
        final Object expected1 = CollectionFactory.toSet(
                groupE.getId(), group1.getId(), group2.getId(), group3.getId(), group4.getId() );
        assertEquals(expected1, memberships.getGroups( user2.getId() ), "Shoulda had memberships in correct groups.");
        final Object expected = CollectionFactory.toSet(
                groupE.getId(), group1.getId(), group2.getId(), group3.getId(), group4.getId() );
        assertEquals(expected, memberships.getGroups( user3.getId() ), "Shoulda had memberships in correct groups.");
    }
    
    
    @Test
    public void testGetMembershipsWorksComplex4()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user );
        final Group groupE = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                BuiltInGroup.EVERYONE );
        final Group groupA = dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                BuiltInGroup.ADMINISTRATORS );
        mockDaoDriver.addGroupMemberToGroup( groupA.getId(), groupE.getId() );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        final Object expected = CollectionFactory.toSet( groupE.getId(), groupA.getId() );
        assertEquals(expected, memberships.getGroups( user.getId() ), "Shoulda had memberships in correct groups.");
    }
    
    
    @Test
    public void testGetMembershipsWhenCycleInMembershipsAndNoUsersInGroupsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user1 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        final Group group1 = mockDaoDriver.createGroup( "g1" );
        final Group group2 = mockDaoDriver.createGroup( "g2" );
        final Group group3 = mockDaoDriver.createGroup( "g3" );
        final Group group4 = mockDaoDriver.createGroup( "g4" );
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), group2.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group3.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group4.getId() );
        mockDaoDriver.addGroupMemberToGroup( group4.getId(), group1.getId() );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
                {
                    memberships.getGroups( user1.getId() );
                }
            } );
    }
    
    
    @Test
    public void testGetMembershipsWhenCycleInMembershipsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user1 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        final Group group1 = mockDaoDriver.createGroup( "g1" );
        final Group group2 = mockDaoDriver.createGroup( "g2" );
        final Group group3 = mockDaoDriver.createGroup( "g3" );
        final Group group4 = mockDaoDriver.createGroup( "g4" );
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), group2.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group3.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group4.getId() );
        mockDaoDriver.addGroupMemberToGroup( group4.getId(), group1.getId() );
        mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user3.getId() );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
                {
                    memberships.getGroups( user1.getId() );
                }
            } );
    }
    
    
    @Test
    public void testGetMembershipsWhenCycleInSimulatedMembershipsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user1 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        final Group group1 = mockDaoDriver.createGroup( "g1" );
        final Group group2 = mockDaoDriver.createGroup( "g2" );
        final Group group3 = mockDaoDriver.createGroup( "g3" );
        final Group group4 = mockDaoDriver.createGroup( "g4" );
        mockDaoDriver.addGroupMemberToGroup( group1.getId(), group2.getId() );
        mockDaoDriver.addGroupMemberToGroup( group2.getId(), group3.getId() );
        mockDaoDriver.addGroupMemberToGroup( group3.getId(), group4.getId() );
        mockDaoDriver.addUserMemberToGroup( group1.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user1.getId() );
        mockDaoDriver.addUserMemberToGroup( group3.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( group4.getId(), user3.getId() );
        
        final GroupMembershipCalculator memberships = 
                new GroupMembershipCalculator( dbSupport.getServiceManager() );
        memberships.addGroupMember( BeanFactory.newBean( GroupMember.class )
                .setGroupId( group4.getId() ).setMemberGroupId( group1.getId() ) );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
                {
                    memberships.getGroups( user1.getId() );
                }
            } );
    }
}
