/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;


import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetBucketAclsRequestHandler_Test 
{
    @Test
    public void testGetForBucketWhenUserHasAccessToDoSoWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl( 
                null, null, mockDaoDriver.createUser( "user3" ).getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( BucketAcl.BUCKET_ID, bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( bucket.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( acl.getId().toString() );
    }
    
    
    @Test
    public void testGetForBucketWhenUserDoesNotHaveAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( BucketAcl.BUCKET_ID, bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( bucket.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket2.getId().toString() );
    }
    
    
    @Test
    public void testGetForUserWhereUserIdFilteredByIsUserMakingRequestReturnsAllBucketAcls()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( user3.getId(), "bucket3" );
        
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user2.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user3.getId(), BucketAclPermission.WRITE );

        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        mockDaoDriver.addBucketAcl( bucket3.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket3.getId(), null, user3.getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( UserIdObservable.USER_ID, user.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( bucket.getId().toString() );
        driver.assertResponseToClientContains( bucket2.getId().toString() );
        driver.assertResponseToClientContains( bucket3.getId().toString() );
        driver.assertResponseToClientContains( user.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user3.getId().toString() );
    }
    
    
    @Test
    public void testGetForUserOtherThanCurrentUserWithoutBucketIdNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( user3.getId(), "bucket3" );
        
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user2.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user3.getId(), BucketAclPermission.WRITE );

        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        mockDaoDriver.addBucketAcl( bucket3.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket3.getId(), null, user3.getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( UserIdObservable.USER_ID, user2.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testGetForUserReturnsOnlyBucketAclsUserHasAccessToSee()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( user3.getId(), "bucket3" );
        
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user2.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user3.getId(), BucketAclPermission.WRITE );

        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        mockDaoDriver.addBucketAcl( bucket3.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket3.getId(), null, user3.getId(), BucketAclPermission.OWNER );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( UserIdObservable.USER_ID, user2.getId().toString() )
            .addParameter( BucketAcl.BUCKET_ID, bucket.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( bucket.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user.getId().toString() );
        driver.assertResponseToClientContains( user2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user3.getId().toString() );

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( UserIdObservable.USER_ID, user2.getId().toString() )
            .addParameter( BucketAcl.BUCKET_ID, bucket2.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( bucket.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( user3.getId().toString() );
    }
    
    
    @Test
    public void testGetForGlobalAclsWhenInternalRequestWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl( 
                null, null, mockDaoDriver.createUser( "user3" ).getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( BucketAcl.BUCKET_ID, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( bucket.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket2.getId().toString() );
        driver.assertResponseToClientContains( acl.getId().toString() );
    }
    
    
    @Test
    public void testGetForGlobalAndNonGlobalAclsWhenUserHasAccessToDoSoWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl( 
                null, null, mockDaoDriver.createUser( "user3" ).getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( bucket.getId().toString() );
        driver.assertResponseToClientContains( bucket2.getId().toString() );
        driver.assertResponseToClientContains( acl.getId().toString() );
    }
    
    
    @Test
    public void testGetForGlobalAclsWhenUserHasAccessToDoSoWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl( 
                null, null, mockDaoDriver.createUser( "user3" ).getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( BucketAcl.BUCKET_ID, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( bucket.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket2.getId().toString() );
        driver.assertResponseToClientContains( acl.getId().toString() );
    }
    
    
    @Test
    public void testGetForGlobalAclsWhenUserDoesNotHaveAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
            .addParameter( BucketAcl.BUCKET_ID, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
