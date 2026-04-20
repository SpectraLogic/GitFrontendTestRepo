/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BeansServiceManagerImpl;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class GroupServiceImpl_Test 
{
    @Test
    public void testBuiltInGroupsAreInitializedAndDefaultAccessEstablished()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final BeansServiceManager bsm = dbSupport.getServiceManager();
        final GroupService service = bsm.getService( GroupService.class );
        
        final User user = BeanFactory.newBean( User.class ).setName( "jason" )
                .setAuthId( "a" ).setSecretKey( "s" );
        bsm.getService( UserService.class ).create( user );
        
        for ( final BuiltInGroup type : BuiltInGroup.values() )
        {
            service.attain( service.getBuiltInGroup( type ).getId() );
        }
        assertEquals(null, bsm.getRetriever( DataPolicyAcl.class ).attain( Require.nothing() ).getDataPolicyId(), "Shoulda generated data policy access.");
        assertEquals(null, bsm.getRetriever( DataPolicyAcl.class ).attain( Require.nothing() ).getUserId(), "Shoulda generated data policy access.");
        final Object expected3 = service.getBuiltInGroup( BuiltInGroup.EVERYONE ).getId();
        assertEquals(expected3, bsm.getRetriever( DataPolicyAcl.class ).attain( Require.nothing() ).getGroupId(), "Shoulda generated data policy access.");
        assertEquals(null, bsm.getRetriever( BucketAcl.class ).attain( Require.nothing() ).getBucketId(), "Shoulda generated bucket access.");
        assertEquals(null, bsm.getRetriever( BucketAcl.class ).attain( Require.nothing() ).getUserId(), "Shoulda generated bucket access.");
        final Object expected2 = service.getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId();
        assertEquals(expected2, bsm.getRetriever( BucketAcl.class ).attain( Require.nothing() ).getGroupId(), "Shoulda generated bucket access.");

        /*
         * Run service initializers again
         */
        final Set< Class< ? > > seedClassesInPackagesToSearchForServices = new HashSet<>();
        seedClassesInPackagesToSearchForServices.add( DaoServicesSeed.class );
        BeansServiceManagerImpl.create(
                dbSupport.getServiceManager().getNotificationEventDispatcher(), 
                dbSupport.getDataManager(), 
                seedClassesInPackagesToSearchForServices );
        
        for ( final BuiltInGroup type : BuiltInGroup.values() )
        {
            assertNotNull(
                    service.getBuiltInGroup( type ),
                    "Shoulda created built-in groups."
                     );
            final Object actual = service.getBuiltInGroup( type ).getName();
            assertEquals(type.getName(), actual, "Shoulda created built-in groups.");
            assertEquals(true, service.getBuiltInGroup( type ).isBuiltIn(), "Shoulda created built-in groups.");
        }

        final User user2 = BeanFactory.newBean( User.class ).setName( "barry" )
                .setAuthId( "ab" ).setSecretKey( "s" );
        bsm.getService( UserService.class ).create( user2 );

        assertEquals(3,  dbSupport.getDataManager().getCount(Group.class, Require.nothing()), "Shoulda created all built-in groups.");

        assertEquals(1,  dbSupport.getDataManager().getCount(GroupMember.class, Require.all(
                Require.beanPropertyEquals(
                        GroupMember.GROUP_ID,
                        service.getBuiltInGroup(BuiltInGroup.EVERYONE).getId()),
                Require.beanPropertyEquals(
                        GroupMember.MEMBER_USER_ID,
                        user.getId()))), "All users should always belong to the everyone users group.");

        assertEquals(1,  dbSupport.getDataManager().getCount(GroupMember.class, Require.all(
                Require.beanPropertyEquals(
                        GroupMember.GROUP_ID,
                        service.getBuiltInGroup(BuiltInGroup.EVERYONE).getId()),
                Require.beanPropertyEquals(
                        GroupMember.MEMBER_USER_ID,
                        user2.getId()))), "All users should always belong to the everyone users group.");

        assertEquals(null, bsm.getRetriever( DataPolicyAcl.class ).attain( Require.nothing() ).getDataPolicyId(), "Shoulda generated data policy access.");
        assertEquals(null, bsm.getRetriever( DataPolicyAcl.class ).attain( Require.nothing() ).getUserId(), "Shoulda generated data policy access.");
        final Object expected1 = service.getBuiltInGroup( BuiltInGroup.EVERYONE ).getId();
        assertEquals(expected1, bsm.getRetriever( DataPolicyAcl.class ).attain( Require.nothing() ).getGroupId(), "Shoulda generated data policy access.");
        assertEquals(null, bsm.getRetriever( BucketAcl.class ).attain( Require.nothing() ).getBucketId(), "Shoulda generated bucket access.");
        assertEquals(null, bsm.getRetriever( BucketAcl.class ).attain( Require.nothing() ).getUserId(), "Shoulda generated bucket access.");
        final Object expected = service.getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId();
        assertEquals(expected, bsm.getRetriever( BucketAcl.class ).attain( Require.nothing() ).getGroupId(), "Shoulda generated bucket access.");
        dbSupport.getDataManager().deleteBeans( DataPolicyAcl.class, Require.nothing() );
        dbSupport.getDataManager().deleteBeans( BucketAcl.class, Require.nothing() );
        
        /*
         * Run service initializers again
         */
        BeansServiceManagerImpl.create(
                dbSupport.getServiceManager().getNotificationEventDispatcher(), 
                dbSupport.getDataManager(), 
                seedClassesInPackagesToSearchForServices );
        assertEquals(null, bsm.getRetriever( DataPolicyAcl.class ).retrieve( Require.nothing() ), "Should notta re-generated data policy access.");
        assertEquals(null, bsm.getRetriever( BucketAcl.class ).retrieve( Require.nothing() ), "Should notta re-generated bucket access.");
    }
}
