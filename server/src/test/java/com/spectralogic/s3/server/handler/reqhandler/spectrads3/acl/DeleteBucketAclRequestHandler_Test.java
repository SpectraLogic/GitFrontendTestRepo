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
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteBucketAclRequestHandler_Test 
{
    @Test
    public void testDeleteBucketAclWhenUserHasAccessToDoSoResultsInDelete()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.deleteAll( BucketAcl.class );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl(
                bucket.getId(), null, user3.getId(), BucketAclPermission.values()[ 0 ] );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.BUCKET_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                3,
                support.getDatabaseSupport().getServiceManager().getRetriever( BucketAcl.class ).getCount(),
                "Shoulda deleted bucket ACL."
                );
    }
    
    
    @Test
    public void testDeleteBucketAclWhenUserDoesNotHaveAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.deleteAll( BucketAcl.class );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl(
                bucket.getId(), null, user3.getId(), BucketAclPermission.values()[ 0 ] );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.BUCKET_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        assertEquals(
                4,
                support.getDatabaseSupport().getServiceManager().getRetriever( BucketAcl.class ).getCount(),
                "Should notta deleted bucket ACL."
                );
    }
    
    
    @Test
    public void testDeleteGlobalBucketAclWhenUserHasAccessToDoSoResultsInDelete()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.deleteAll( BucketAcl.class );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl(
                null, null, user3.getId(), BucketAclPermission.values()[ 0 ] );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.BUCKET_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                3,
                support.getDatabaseSupport().getServiceManager().getRetriever( BucketAcl.class ).getCount(),
                "Shoulda deleted bucket ACL."
               );
    }
    
    
    @Test
    public void testDeleteGlobalBucketAclWhenUserDoesNotHaveAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.deleteAll( BucketAcl.class );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final User user3 = mockDaoDriver.createUser( "user3" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        final BucketAcl acl = mockDaoDriver.addBucketAcl(
                null, null, user3.getId(), BucketAclPermission.values()[ 0 ] );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.BUCKET_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        assertEquals(
                4,
                support.getDatabaseSupport().getServiceManager().getRetriever( BucketAcl.class ).getCount(),
                "Should notta deleted bucket ACL."
                );
    }
}
