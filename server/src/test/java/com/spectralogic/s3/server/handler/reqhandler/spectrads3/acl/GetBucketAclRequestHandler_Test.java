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
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetBucketAclRequestHandler_Test 
{
    @Test
    public void testGetWhenUserHasAccessToDoSoResultsInDelete()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        final BucketAcl acl =
                mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( bucket.getId().toString() );
        driver.assertResponseToClientDoesNotContain( bucket2.getId().toString() );
    }
    
    
    @Test
    public void testGetWhenUserDoesNotHaveAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        final BucketAcl acl = 
                mockDaoDriver.addBucketAcl( bucket.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BUCKET_ACL + "/" + acl.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
