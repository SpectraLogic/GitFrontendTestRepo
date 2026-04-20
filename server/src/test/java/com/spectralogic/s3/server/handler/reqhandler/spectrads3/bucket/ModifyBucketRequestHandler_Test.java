/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.util.UUID;

import com.spectralogic.s3.common.dao.service.ds3.BucketService;


import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.mock.MockAnonymousAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class ModifyBucketRequestHandler_Test 
{
    @Test
    public void testModifyBucketModifiesTheBucket()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        mockDaoDriver.createBucket( userId, "new_bucket_name" );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = runModifyBucket( support, userId );
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testModifyBucketReturns404WhenBucketDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final UUID userId = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( MockDaoDriver.DEFAULT_USER_NAME )
                .getId();
        runModifyBucket( support, userId ).assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testModifyBucketOwnerOnSystemBucketAllowedIffInternalRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final User user1 = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( 
                user1.getId(), BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user1.getId() );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucket.getName() )
                        .addParameter( UserIdObservable.USER_ID, user2.getId().toString() )
                        .addHeader( S3HeaderType.IMPERSONATE_USER, user1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        assertEquals(
                user1.getId(),
                mockDaoDriver.attainOneAndOnly( Bucket.class ).getUserId(),
                "Should notta changed bucket owner."
                 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucket.getName() )
                        .addParameter( UserIdObservable.USER_ID, user2.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(
                user2.getId(),
                mockDaoDriver.attainOneAndOnly( Bucket.class ).getUserId(),
                "Shoulda changed bucket owner."
                 );
    }


    private static MockHttpRequestDriver runModifyBucket(
            final MockHttpRequestSupport support,
            final UUID userId )
    {
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/" + RestDomainType.BUCKET + "/new_bucket_name" )
                        .addParameter(
                                UserIdObservable.USER_ID,
                                userId.toString() );
        driver.run();
        return driver;
    }


    @Test
    public void testAddProtectedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();

        final String bucketName = "new_bucket_name";
        mockDaoDriver.createBucket( userId, bucketName );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final BucketService bucketService = support.getDatabaseSupport().getServiceManager().getService( BucketService.class );
        final Bucket beforeBucket = bucketService.retrieve( Bucket.NAME, bucketName );
        assertFalse(
                beforeBucket.isProtected(),
                "Protected flag should be disabled." );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucketName )
                .addParameter( UserIdObservable.USER_ID, userId.toString() )
                .addParameter( Bucket.PROTECTED, "true" );
        driver.run();

        driver.assertHttpResponseCodeEquals( 200 );

        final Bucket afterBucket = bucketService.retrieve( Bucket.NAME, bucketName );
        assertTrue(
                afterBucket.isProtected() ,
                "Protected flag should be enabled.");
    }

    @Test
    public void testRemoveProtectedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();

        final String bucketName = "new_bucket_name";
        mockDaoDriver.createBucket( userId, bucketName );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final BucketService bucketService = support.getDatabaseSupport().getServiceManager().getService( BucketService.class );
        final Bucket beforeBucket = bucketService.retrieve( Bucket.NAME, bucketName );
        bucketService.update( beforeBucket.setProtected( true ), Bucket.PROTECTED );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucketName )
                .addParameter( UserIdObservable.USER_ID, userId.toString() )
                .addParameter( Bucket.PROTECTED, "false" );
        driver.run();

        driver.assertHttpResponseCodeEquals( 200 );

        final Bucket afterBucket = bucketService.retrieve( Bucket.NAME, bucketName );
        assertFalse(
                afterBucket.isProtected(),
                "Protected flag should be disabled." );
    }
}
