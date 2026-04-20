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
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class GroupMembershipCache_Test 
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {       
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new GroupMembershipCache( null, 10 );
            }
        } );
    }
    
    
    @Test
    public void testCacheDoesCacheMembershipsUntilDurationTimeout()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group groupAdmins = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group groupTapeAdmins = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.TAPE_ADMINS );
        final Group group2 = mockDaoDriver.createGroup( "group" );
        final User user = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        
        final GroupMembershipCache cache = 
                new GroupMembershipCache( dbSupport.getServiceManager(), 100 );
        assertFalse(cache.isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group yet.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group.");
        mockDaoDriver.addUserMemberToGroup( groupAdmins.getId(), user.getId() );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( groupTapeAdmins.getId(), user3.getId() );
        
        int i = 20;
        while ( cache.isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ) &&
                0 < i )
        {
            TestUtil.sleep( 100 );
            --i;
        }
        assertFalse(cache.isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ), "Shoulda serviced request out of cache.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group.");
        TestUtil.sleep( 200 );
        assertTrue(cache.isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ), "Shoulda refreshed cache.");
        assertFalse(cache.isMember( user.getId(), BuiltInGroup.TAPE_ADMINS ), "Shoulda refreshed cache.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.TAPE_ADMINS ), "User not part of administrators group.");
        assertFalse(cache.isMember( user3.getId(), BuiltInGroup.ADMINISTRATORS ), "Shoulda reported admin membership correctly.");
        assertTrue(cache.isMember( user3.getId(), BuiltInGroup.TAPE_ADMINS ), "Shoulda reported admin membership correctly.");
    }
    
    
    @Test
    public void testCacheDoesCacheMembershipsUntilInvalidation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group groupAdmins = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final Group groupTapeAdmins = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.TAPE_ADMINS );
        final Group group2 = mockDaoDriver.createGroup( "group" );
        final User user = BeanFactory.newBean( User.class )
                .setName( "a" ).setAuthId( "aid" ).setSecretKey( "ask" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user );
        final User user2 = BeanFactory.newBean( User.class )
                .setName( "b" ).setAuthId( "bid" ).setSecretKey( "bsk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user2 );
        final User user3 = BeanFactory.newBean( User.class )
                .setName( "c" ).setAuthId( "cid" ).setSecretKey( "csk" );
        dbSupport.getServiceManager().getService( UserService.class ).create( user3 );
        
        final GroupMembershipCache cache = 
                new GroupMembershipCache( dbSupport.getServiceManager(), 1000 );
        assertFalse(cache.isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group yet.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group.");
        mockDaoDriver.addUserMemberToGroup( groupAdmins.getId(), user.getId() );
        mockDaoDriver.addUserMemberToGroup( group2.getId(), user2.getId() );
        mockDaoDriver.addUserMemberToGroup( groupTapeAdmins.getId(), user3.getId() );
        assertFalse(cache.isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ), "Shoulda serviced request out of cache.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group.");
        cache.invalidate();
        assertTrue(cache.isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ), "Shoulda refreshed cache.");
        assertFalse(cache.isMember( user.getId(), BuiltInGroup.TAPE_ADMINS ), "Shoulda refreshed cache.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.ADMINISTRATORS ), "User not part of administrators group.");
        assertFalse(cache.isMember( user2.getId(), BuiltInGroup.TAPE_ADMINS ), "User not part of administrators group.");
        assertFalse(cache.isMember( user3.getId(), BuiltInGroup.ADMINISTRATORS ), "Shoulda reported admin membership correctly.");
        assertTrue(cache.isMember( user3.getId(), BuiltInGroup.TAPE_ADMINS ), "Shoulda reported admin membership correctly.");
    }
}
