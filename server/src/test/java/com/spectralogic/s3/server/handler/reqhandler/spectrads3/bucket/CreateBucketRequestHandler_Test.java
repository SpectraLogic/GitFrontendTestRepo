/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CreateBucketRequestHandler_Test
{
    @Test
    public void testCreateBucketCreatesBucketWithProvidedAttributes()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = new MockDaoDriver( support.getDatabaseSupport() )
                .createUser( MockDaoDriver.DEFAULT_USER_NAME )
                .getId();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        final UUID id = UUID.randomUUID();
        final MockHttpRequestDriver driver = runCreateBucket( support, userId, id );
        
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getService( BucketService.class ).attain( id );
    }
    
    
    @Test
    public void testCreateBucketFailsWhenTooManyCreated()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.updateBean( user.setMaxBuckets( 10 ), User.MAX_BUCKETS );
        
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        mockDaoDriver.updateBean( user.setDefaultDataPolicyId( dp1.getId() ), User.DEFAULT_DATA_POLICY_ID );
        
        for ( int x = 1; x <= 10; ++x )
        {
            final MockHttpRequestDriver driver = new MockHttpRequestDriver( support, true,
                    new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ), RequestType.POST,
                    "_rest_/" + RestDomainType.BUCKET ).addParameter( Bucket.NAME, "new_bucket_name_" + x );
            driver.run();
            driver.assertHttpResponseCodeEquals( 201 );
        }
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( support, true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ), RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET ).addParameter( Bucket.NAME, "new_bucket_name_fail" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreateBucketWithoutNameNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "aaa" );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "zzz" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        mockDaoDriver.updateBean( user.setDefaultDataPolicyId( dp1.getId() ), User.DEFAULT_DATA_POLICY_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                        .addParameter( Bucket.NAME, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateBucketWithoutSpecifyingUserIdCreatesBucketWithUserIdForRequestInvoker()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "aaa" );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        mockDaoDriver.createUser( "zzz" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        
        final MockHttpRequestDriver driver = runCreateBucket( support, null );
        driver.assertHttpResponseCodeEquals( 201 );
        
        final Bucket bucket = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( BucketService.class )
                .retrieve( Bucket.NAME, "new_bucket_name" );
        
        assertEquals(
                userId,
                bucket.getUserId(),
                "Shoulda created bucket with userid equal to the user who made the S3 request."
               );
    }
    
    
    @Test
    public void testCreateBucketUsingDataPolicyUserDoesNotHaveAccessToUseNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "aaa" );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        mockDaoDriver.createUser( "zzz" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = runCreateBucket( support, null );
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreateBucketRelyingOnImplicitDataPolicyWhenNoDataPoliciesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "aaa" );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        mockDaoDriver.createUser( "zzz" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPolicy( "dp2" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                        .addParameter( Bucket.NAME, "new_bucket_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }
    
    
    @Test
    public void testCreateBucketRelyingOnImplicitDataPolicyWhenMultipleDataPoliciesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "aaa" );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        mockDaoDriver.createUser( "zzz" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.grantDataPolicyAccessToEveryUser();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                        .addParameter( Bucket.NAME, "new_bucket_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateBucketRelyingOnImplicitDataPolicyWhenDefaultDataPolicyAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "aaa" );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "zzz" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        mockDaoDriver.updateBean( user.setDefaultDataPolicyId( dp1.getId() ), User.DEFAULT_DATA_POLICY_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                        .addParameter( Bucket.NAME, "new_bucket_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
    }
    
    
    @Test
    public void testCreateBucketRelyingOnImplicitDataPolicyWhenSingleDataPolicyAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "aaa" );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createUser( "zzz" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.addDataPolicyAcl( dp1.getId(), null, user.getId() );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                        .addParameter( Bucket.NAME, "new_bucket_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
    }


    private static MockHttpRequestDriver runCreateBucket(
            final MockHttpRequestSupport support,
            final UUID userId )
    {
        return runCreateBucket( support, userId, null );
    }


    private static MockHttpRequestDriver runCreateBucket(
            final MockHttpRequestSupport support,
            final UUID userId,
            final UUID explicitBucketId )
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = 
                mockDaoDriver.createDataPolicy( "datapolicy" );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                        .addParameter(
                                Bucket.NAME,
                                "new_bucket_name" )
                        .addParameter(
                                Bucket.DATA_POLICY_ID,
                                dataPolicy.getName() );
        if ( null != userId )
        {
            driver.addParameter(
                    UserIdObservable.USER_ID,
                    userId.toString() );
        }
        if ( null != explicitBucketId )
        {
            driver.addParameter( Identifiable.ID, explicitBucketId.toString() );
        }
        driver.run();
        return driver;
    }
    

    @Test
    public void testCreateSpectraNamespaceBucketAllowedIffDoneAsInternalRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "testUser" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        mockDaoDriver.grantDataPolicyAccessToEveryUser();

        final DataPolicy dataPolicy = 
                mockDaoDriver.createDataPolicy( "datapolicy" );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.BUCKET )
            .addParameter( Bucket.NAME, BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" )
            .addParameter( Bucket.DATA_POLICY_ID, dataPolicy.getName() )
            .addParameter( UserIdObservable.USER_ID, user.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy().impersonate( user.getName() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.BUCKET )
            .addParameter( Bucket.NAME, BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" )
            .addParameter( Bucket.DATA_POLICY_ID, dataPolicy.getName() )
            .addParameter( UserIdObservable.USER_ID, user.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.POST, 
                "_rest_/" + RestDomainType.BUCKET )
            .addParameter( Bucket.NAME, BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test2" )
            .addParameter( Bucket.DATA_POLICY_ID, dataPolicy.getName() )
            .addParameter( UserIdObservable.USER_ID, user.getId().toString() )
            .addHeader( S3HeaderType.REPLICATION_SOURCE_IDENTIFIER, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
    }


    @Test
    public void testCreateBucketWithoutProtectedFlagDefaultsToFalse()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.grantDataPolicyAccessToEveryUser();

        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "datapolicy" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                .addParameter( Bucket.NAME, "new_bucket_name" )
                .addParameter( Bucket.DATA_POLICY_ID, dataPolicy.getName() );
        driver.run();

        driver.assertHttpResponseCodeEquals( 201 );

        final Bucket bucket = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( BucketService.class )
                .retrieve( Bucket.NAME, "new_bucket_name" );

        assertFalse(
                bucket.isProtected(),
                "Should not have protected flag set." );
    }


    @Test
    public void testCreateBucketWithProtectedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.grantDataPolicyAccessToEveryUser();

        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "datapolicy" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.POST,
                "_rest_/" + RestDomainType.BUCKET )
                .addParameter( Bucket.NAME, "new_bucket_name" )
                .addParameter( Bucket.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( Bucket.PROTECTED, "true" );
        driver.run();

        driver.assertHttpResponseCodeEquals( 201 );

        final Bucket bucket = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( BucketService.class )
                .retrieve( Bucket.NAME, "new_bucket_name" );

        assertTrue(
                bucket.isProtected(),
                "Should have protected flag set." );
    }
}
