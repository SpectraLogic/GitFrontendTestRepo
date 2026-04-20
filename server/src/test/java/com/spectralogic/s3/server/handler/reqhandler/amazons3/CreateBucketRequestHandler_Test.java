/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;


import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public class CreateBucketRequestHandler_Test 
{
    @Test
    public void testCreateBucketDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final String bucketName = "testBucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( "somePolicy" );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.updateBean( 
                user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                bucketName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testCreateBucketFailsWhenTooManyCreated()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final String bucketName = "testBucket_";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "somePolicy" );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.updateBean( user.setMaxBuckets( 10 ), User.MAX_BUCKETS );
        
        mockDaoDriver.updateBean( user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        for ( int x = 1; x <= 10; ++x )
        {
            final MockHttpRequestDriver driver =
                    new MockHttpRequestDriver( support, true, new MockUserAuthorizationStrategy( user.getName() ),
                            RequestType.PUT, bucketName + x );
            driver.run();
            driver.assertHttpResponseCodeEquals( 200 );
        }
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockUserAuthorizationStrategy( user.getName() ),
                        RequestType.PUT, bucketName + "fail" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreateBucketThatAlreadyExistsNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final String bucketName = "testBucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( "somePolicy" );
        final User user = mockDaoDriver.createUser( "testUser" );
        final User user2 = mockDaoDriver.createUser( "testUser2" );
        mockDaoDriver.updateBean( 
                user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                bucketName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                bucketName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        driver.assertResponseToClientContains( AWSFailure.BUCKET_ALREADY_OWNED_BY_YOU.getCode() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.PUT, 
                bucketName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        driver.assertResponseToClientContains( AWSFailure.BUCKET_ALREADY_EXISTS.getCode() );
    }
    
    
    @Test
    public void testCreateBucketWhenUserDefaultDataPolicyIsNotDataPolicyUserHasAccessToNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final String bucketName = "testBucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( "somePolicy" );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.updateBean( 
                user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                bucketName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreateBucketWhenUserDoesNotHaveDefaultDataPolicyAssignedNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final String bucketName = "testBucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( "somePolicy" );
        mockDaoDriver.createDataPolicy( "somePolicy2" );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                bucketName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        mockDaoDriver.updateBean( 
                user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                bucketName );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testCreateSpectraNamespaceBucketAllowedIffDoneAsInternalRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( "somePolicy" );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.updateBean( 
                user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        mockDaoDriver.createBucket( user.getId(), BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy().impersonate( user.getName() ),
                RequestType.PUT, 
                "Spectra-Test" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "Spectra-Test-2" ).addHeader( S3HeaderType.REPLICATION_SOURCE_IDENTIFIER, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testCreateBucketWithInvalidCharactersNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( "somePolicy" );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.updateBean( 
                user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "test:name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateBucketWithNameTooLongNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( "somePolicy" );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.updateBean( 
                user.setDefaultDataPolicyId( dataPolicy.getId() ), User.DEFAULT_DATA_POLICY_ID );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.PUT, 
                "testsomeveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylongname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
}
