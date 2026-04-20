/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class GetObjectRequestHandler_Test 
{
    @Test
    public void testGetObjectWhenUserHasNoAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final S3Object object = mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                "_rest_/object/" + object.getName() )
            .addParameter( S3Object.BUCKET_ID, object.getBucketId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testGetObjectWithoutBucketSpecificationNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final S3Object object = mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME );
        mockDaoDriver.addBucketAcl( object.getBucketId(), null, user.getId(), BucketAclPermission.LIST );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                "_rest_/object/" + object.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetObjectByIdDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final S3Object object = mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME );
        mockDaoDriver.addBucketAcl( object.getBucketId(), null, user.getId(), BucketAclPermission.LIST );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                "_rest_/object/" + object.getId().toString() )
            .addParameter( S3Object.BUCKET_ID, MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Id", object.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/BucketId", object.getBucketId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", object.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/Type", object.getType().toString() );
    }
    
    
    @Test
    public void testGetObjectByNameDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final S3Object object = mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME );
        mockDaoDriver.addBucketAcl( object.getBucketId(), null, user.getId(), BucketAclPermission.LIST );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                "_rest_/object/" + object.getName() )
            .addParameter( S3Object.BUCKET_ID, MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Id", object.getId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/BucketId", object.getBucketId().toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Name", object.getName() );
        driver.assertResponseToClientXPathEquals( "/Data/Type", object.getType().toString() );
    }
}
