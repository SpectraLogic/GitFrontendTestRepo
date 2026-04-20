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
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class DataPolicyAclAuthorizationServiceImpl_Test 
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
                new DataPolicyAclAuthorizationServiceImpl(
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
                new DataPolicyAclAuthorizationServiceImpl(
                        dbSupport.getServiceManager(),
                        null,
                        1 );
            }
        } );
    }
    

    @Test
    public void testPermissionGrantedWhenAdministrator()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Group adminGroup = 
                dbSupport.getServiceManager().getService( GroupService.class ).getBuiltInGroup( 
                        BuiltInGroup.ADMINISTRATORS );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        mockDaoDriver.addUserMemberToGroup( adminGroup.getId(), user4.getId() );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        
        final DataPolicyAclAuthorizationServiceImpl service = new DataPolicyAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionDenied(
                service,
                new DataPolicyAccessRequest( user1.getId(), dataPolicy1.getId() ) );
        assertPermissionGranted(
                service,
                new DataPolicyAccessRequest( user4.getId(), dataPolicy1.getId() ) );
    }
    

    @Test
    public void testPermissionGrantedWhenAclAccessGranted()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user4 = mockDaoDriver.createUser( "user4" );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.addDataPolicyAcl( dataPolicy1.getId(), null, user4.getId() );
        
        final DataPolicyAclAuthorizationServiceImpl service = new DataPolicyAclAuthorizationServiceImpl(
                dbSupport.getServiceManager(),
                new GroupMembershipCache( dbSupport.getServiceManager(), 1 ),
                1 );
        
        assertPermissionDenied(
                service,
                new DataPolicyAccessRequest( user1.getId(), dataPolicy1.getId() ) );
        assertPermissionGranted(
                service,
                new DataPolicyAccessRequest( user4.getId(), dataPolicy1.getId() ) );
    }
    
    
    private void assertPermissionGranted( 
            final DataPolicyAclAuthorizationServiceImpl service, 
            final DataPolicyAccessRequest request )
    {
        service.verifyHasAccess( request );
    }
    
    
    private void assertPermissionDenied( 
            final DataPolicyAclAuthorizationServiceImpl service, 
            final DataPolicyAccessRequest request )
    {
        TestUtil.assertThrows( null, GenericFailure.FORBIDDEN, new BlastContainer()
        {
            public void test()
                {
                    service.verifyHasAccess( request );
                }
            } );
    }
}
