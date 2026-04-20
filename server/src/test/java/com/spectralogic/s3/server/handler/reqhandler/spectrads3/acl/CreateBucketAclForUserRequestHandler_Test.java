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
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CreateBucketAclForUserRequestHandler_Test 
{
    @Test
    public void testCreateWhenUserHasAccessToDoSoResultsInCreate()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.deleteAll( BucketAcl.class );
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
                RequestType.POST, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
                .addParameter(
                        BucketAcl.PERMISSION,
                        BucketAclPermission.values()[ 0 ].toString() )
                .addParameter(
                        BucketAcl.BUCKET_ID,
                        bucket.getName() )
                .addParameter(
                        UserIdObservable.USER_ID,
                        user2.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        assertEquals(
                4,
                support.getDatabaseSupport().getServiceManager().getRetriever( BucketAcl.class ).getCount(),
                "Shoulda created bucket ACL."
                 );
    }
    
    
    @Test
    public void testCreateWhenUserDoesNotHaveAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.deleteAll( BucketAcl.class );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( user2.getId(), "bucket2" );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user2.getId(), BucketAclPermission.OWNER );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
                .addParameter(
                        BucketAcl.PERMISSION,
                        BucketAclPermission.values()[ 0 ].toString() )
                .addParameter(
                        BucketAcl.BUCKET_ID,
                        bucket.getName() )
                .addParameter(
                        UserIdObservable.USER_ID,
                        user2.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        assertEquals(
                2,
                support.getDatabaseSupport().getServiceManager().getRetriever( BucketAcl.class ).getCount(),
                "Should notta created bucket ACL."
                 );
    }
}
