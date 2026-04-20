/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.util.UUID;


import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public class GetBucketsRequestHandler_Test 
{
    @Test
    public void testGetBucketsReturnsNothingWhenNoBucketsExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final String userName = "test_user_name";
        
        final User testUser = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( userName );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "/" );
        driver.addHeader( S3HeaderType.IMPERSONATE_USER, userName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/ListAllMyBucketsResult/Owner/ID", 
                testUser.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/ListAllMyBucketsResult/Owner/DisplayName", 
                userName );
        driver.assertResponseToClientXPathEquals( "count(/ListAllMyBucketsResult/Buckets)", "1" );
        driver.assertResponseToClientXPathEquals( "count(/ListAllMyBucketsResult/Buckets/Bucket)", "0" );
    }
    
    
    @Test
    public void testGetBucketsReturnsOnlyBucketsForSpecifiedUser()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final String userName = "test_user_name";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User testUser = mockDaoDriver.createUser( userName );
        final Bucket bucket1 = mockDaoDriver.createBucket( testUser.getId(), "test_bucket_1" );
        mockDaoDriver.addBucketAcl( bucket1.getId(), null, testUser.getId(), BucketAclPermission.OWNER );
        final Bucket bucket2 =  mockDaoDriver.createBucket( testUser.getId(), "test_bucket_2" );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, testUser.getId(), BucketAclPermission.LIST );
        final Bucket bucketSystem =  mockDaoDriver.createBucket( 
                testUser.getId(), BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" );
        mockDaoDriver.addBucketAcl( bucketSystem.getId(), null, testUser.getId(), BucketAclPermission.LIST );
        final Bucket bucket3 = mockDaoDriver.createBucket( testUser.getId(), "test_bucket_3" );
        mockDaoDriver.addBucketAcl( bucket3.getId(), null, testUser.getId(), BucketAclPermission.WRITE );
        final UUID additionalUserId = mockDaoDriver.createUser( "additional_user_name" ).getId();
        mockDaoDriver.createBucket( additionalUserId, "test_bucket_4" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "/" );
        driver.addHeader( S3HeaderType.IMPERSONATE_USER, userName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/ListAllMyBucketsResult/Owner/ID", 
                testUser.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/ListAllMyBucketsResult/Owner/DisplayName", userName );
        driver.assertResponseToClientXPathEquals( "count(/ListAllMyBucketsResult/Buckets)", "1" );
        driver.assertResponseToClientXPathEquals( "count(/ListAllMyBucketsResult/Buckets/Bucket)", "5" );
        driver.assertResponseToClientXPathEquals(
                "/ListAllMyBucketsResult/Buckets/Bucket[1]/Name",
                "spectra-test" );
        driver.assertResponseToClientXPathEquals(
                "/ListAllMyBucketsResult/Buckets/Bucket[2]/Name",
                "test_bucket_1" );
        driver.assertResponseToClientXPathEquals(
                "/ListAllMyBucketsResult/Buckets/Bucket[3]/Name",
                "test_bucket_2" );
        driver.assertResponseToClientXPathEquals(
                "count(/ListAllMyBucketsResult/Buckets/Bucket[1]/CreationDate)",
                "1" );
        driver.assertResponseToClientXPathEquals(
                "count(/ListAllMyBucketsResult/Buckets/Bucket[2]/CreationDate)",
                "1" );
    }
}
