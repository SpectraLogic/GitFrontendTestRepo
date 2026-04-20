/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.mock.MockAnonymousAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class GetObjectsRequestHandler_Test 
{
    @Test
    public void testGetObjectsRespondsCorrectlyWhenAnonymousAuthentication()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final UUID barryId = mockDaoDriver.createUser( "barry" ).getId();
        final UUID bucketJasonId = mockDaoDriver.createBucket( jasonId, "bucketjason" ).getId();
        final UUID bucketBarryId = mockDaoDriver.createBucket( barryId, "bucketbarry" ).getId();
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );

        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasonsong.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasonmusic.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasoncover.jpg" );

        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrysong.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrymusic.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrycover.jpg" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "%mp3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        assertFalse(driver.getResponseToClientAsString().contains( "jasonsong" ), "Shoulda required authorization.");
        assertFalse(driver.getResponseToClientAsString().contains( "jasonmusic" ), "Shoulda required authorization.");
        assertFalse(driver.getResponseToClientAsString().contains( "jasoncover" ), "Shoulda required authorization.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrysong" ), "Shoulda required authorization.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrymusic" ), "Shoulda required authorization.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrycover" ), "Shoulda required authorization.");
    }
    
    
    @Test
    public void testGetObjectsRespondsCorrectlyWhenUserAuthentication()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final UUID barryId = mockDaoDriver.createUser( "barry" ).getId();
        final UUID bucketJasonId = mockDaoDriver.createBucket( jasonId, "bucketjason" ).getId();
        final UUID bucketBarryId = mockDaoDriver.createBucket( barryId, "bucketbarry" ).getId();
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );
        mockDaoDriver.addBucketAcl( bucketJasonId, null, jasonId, BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucketBarryId, null, jasonId, BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucketJasonId, null, barryId, BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucketBarryId, null, barryId, BucketAclPermission.LIST );

        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasonsong.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasonmusic.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasoncover.jpg" );

        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrysong.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrymusic.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrycover.jpg" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "%mp3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertTrue(driver.getResponseToClientAsString().contains( "jasonsong" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "jasonmusic" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "jasoncover" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "barrysong" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "barrymusic" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrycover" ), "Shoulda notta filtered by user.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "%mp3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertTrue(driver.getResponseToClientAsString().contains( "jasonsong" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "jasonmusic" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "jasoncover" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "barrysong" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "barrymusic" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrycover" ), "Shoulda notta filtered by user.");
    }
    
    
    @Test
    public void testGetObjectsRespondsCorrectlyWhenUserAuthenticationAndAccessIsRestricted()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final UUID barryId = mockDaoDriver.createUser( "barry" ).getId();
        final UUID bucketJasonId = mockDaoDriver.createBucket( jasonId, "bucketjason" ).getId();
        final UUID bucketBarryId = mockDaoDriver.createBucket( barryId, "bucketbarry" ).getId();
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );
        mockDaoDriver.addBucketAcl( bucketJasonId, null, jasonId, BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucketBarryId, null, jasonId, BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucketJasonId, null, barryId, BucketAclPermission.LIST );

        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasonsong.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasonmusic.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketJasonId, "jasoncover.jpg" );

        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrysong.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrymusic.mp3" );
        createDataObject(
                support.getDatabaseSupport(), bucketBarryId, "barrycover.jpg" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "%mp3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertTrue(driver.getResponseToClientAsString().contains( "jasonsong" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "jasonmusic" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "jasoncover" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "barrysong" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "barrymusic" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrycover" ), "Shoulda notta filtered by user.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "%mp3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertTrue(driver.getResponseToClientAsString().contains( "jasonsong" ), "Shoulda notta filtered by user.");
        assertTrue(driver.getResponseToClientAsString().contains( "jasonmusic" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "jasoncover" ), "Shoulda notta filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrysong" ), "Shoulda filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrymusic" ), "Shoulda filtered by user.");
        assertFalse(driver.getResponseToClientAsString().contains( "barrycover" ), "Shoulda filtered by user.");
    }
    
    @Test
    public void testFilterByObjectNameFiltersUsingSpecifiedRegularExpression()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );
        mockDaoDriver.createObject( bucket1.getId(), "foo" );
        mockDaoDriver.createObject( bucket1.getId(), "foobar" );
        mockDaoDriver.createObject( bucket1.getId(), "barfoo" );
        mockDaoDriver.createObject( bucket1.getId(), "barfoobar" );
        mockDaoDriver.createObject( bucket2.getId(), "maxbarfoobar" );
        mockDaoDriver.createObject( bucket2.getId(), "maxbars" );
        
        MockHttpRequestDriver driver;
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "foo" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "\"foo\"" );
        driver.assertResponseToClientDoesNotContain( "\"foobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"barfoo\"" );
        driver.assertResponseToClientDoesNotContain( "\"barfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbarfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbars\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "foo%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "\"foo\"" );
        driver.assertResponseToClientContains( "\"foobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"barfoo\"" );
        driver.assertResponseToClientDoesNotContain( "\"barfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbarfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbars\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "%foo___" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "\"foo\"" );
        driver.assertResponseToClientContains( "\"foobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"barfoo\"" );
        driver.assertResponseToClientContains( "\"barfoobar\"" );
        driver.assertResponseToClientContains( "\"maxbarfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbars\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "%foo%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "\"foo\"" );
        driver.assertResponseToClientContains( "\"foobar\"" );
        driver.assertResponseToClientContains( "\"barfoo\"" );
        driver.assertResponseToClientContains( "\"barfoobar\"" );
        driver.assertResponseToClientContains( "\"maxbarfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbars\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                    .addParameter( "name", "%foo%" )
                    .addParameter( "bucketId", bucket1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "\"foo\"" );
        driver.assertResponseToClientContains( "\"foobar\"" );
        driver.assertResponseToClientContains( "\"barfoo\"" );
        driver.assertResponseToClientContains( "\"barfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbarfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbars\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                    .addParameter( "name", "%foo%" + BaseGetBeansRequestHandler.AND_DELIMITER + "%bar%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "\"foo\"" );
        driver.assertResponseToClientContains( "\"foobar\"" );
        driver.assertResponseToClientContains( "\"barfoo\"" );
        driver.assertResponseToClientContains( "\"barfoobar\"" );
        driver.assertResponseToClientContains( "\"maxbarfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbars\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                    .addParameter( "name", "%foo%" + BaseGetBeansRequestHandler.AND_DELIMITER + "%bar%" )
                    .addParameter( "bucketId", bucket1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "\"foo\"" );
        driver.assertResponseToClientContains( "\"foobar\"" );
        driver.assertResponseToClientContains( "\"barfoo\"" );
        driver.assertResponseToClientContains( "\"barfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbarfoobar\"" );
        driver.assertResponseToClientDoesNotContain( "\"maxbars\"" );
    }
    
    
    @Test
    public void testFilterByNestedObjectNameFiltersUsingSpecifiedRegularExpression()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );
        mockDaoDriver.createObject( null, "movies/raw/chapter1.mov" );
        mockDaoDriver.createObject( null, "movies/raw/chapter2.mov" );
        mockDaoDriver.createObject( null, "movies/raw/chapter3.mov" );
        mockDaoDriver.createObject( null, "movies/raw/chapter10.mov" );
        mockDaoDriver.createObject( null, "movies/final/standard.mov" );
        mockDaoDriver.createObject( null, "movies/final/hd.mov" );
        mockDaoDriver.createObject( null, "movies/final/trailer.mov" );
        mockDaoDriver.createObject( null, "movies/readme.txt" );
        mockDaoDriver.createObject( null, "audio/cover.mp3" );
        mockDaoDriver.createObject( null, "audio/1.mp3" );
        mockDaoDriver.createObject( null, "audio/2.mp3" );
        mockDaoDriver.createObject( null, "audio/3.mp3" );
        
        MockHttpRequestDriver driver;
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "movies/final/%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter3.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "movies/raw/chapter_.mov" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter3.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "movies/%/%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter3.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter(
                        "name", "movies/%/%" + BaseGetBeansRequestHandler.AND_DELIMITER + "%raw%"
                                + BaseGetBeansRequestHandler.OR_DELIMITER + "%something" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter(
                        "name", "movies/%/%" + BaseGetBeansRequestHandler.AND_DELIMITER + "%raw%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter3.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter(
                        "name", "movies/%/%" + BaseGetBeansRequestHandler.OR_DELIMITER + "%raw%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter3.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientContains( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" ).addParameter( "name", "%p%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter3.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientContains( "audio\\/cover.mp3" );
        driver.assertResponseToClientContains( "audio\\/1.mp3" );
        driver.assertResponseToClientContains( "audio\\/2.mp3" );
        driver.assertResponseToClientContains( "audio\\/3.mp3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                .addParameter( "name", "%p%" )
                .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" )
                .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientContains( "audio\\/1.mp3" );
        driver.assertResponseToClientContains( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter3.mov" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                .addParameter( "name", "%p%" )
                .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" )
                .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientContains( "audio\\/2.mp3" );
        driver.assertResponseToClientContains( "audio\\/3.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter3.mov" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                .addParameter( "name", "%p%" )
                .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" )
                .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "2" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientContains( "audio\\/3.mp3" );
        driver.assertResponseToClientContains( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter3.mov" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                .addParameter( "name", "%p%" )
                .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" )
                .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "4" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter3.mov" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                .addParameter( "name", "%p%" )
                .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" )
                .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "6" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientContains( "movies\\/raw\\/chapter3.mov" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object.json" )
                .addParameter( "name", "%p%" )
                .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" )
                .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "8" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/standard.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/hd.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/final\\/trailer.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/readme.txt" );
        driver.assertResponseToClientDoesNotContain( "audio\\/1.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/2.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/3.mp3" );
        driver.assertResponseToClientDoesNotContain( "audio\\/cover.mp3" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter1.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter10.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter2.mov" );
        driver.assertResponseToClientDoesNotContain( "movies\\/raw\\/chapter3.mov" );
    }
        
    
    @Test
    public void testGetObjectsHonorsCreationDateFilters()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        mockDaoDriver.createObject( bucket.getId() , "o1", new Date(1000) );
        mockDaoDriver.createObject( bucket.getId() , "o2", new Date(2000) );
        mockDaoDriver.createObject( bucket.getId() , "o3", new Date(3000) );
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "bucketId", bucket.getName() )
                        .addParameter( "startDate", "1500" )
                        .addParameter( "endDate", "2500" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertFalse(driver.getResponseToClientAsString().contains( "o1" ), "Shoulda notta reported o1.");
        assertTrue(driver.getResponseToClientAsString().contains( "o2" ), "Shoulda reported o2.");
        assertFalse(driver.getResponseToClientAsString().contains( "o3" ), "Shoulda notta reported o3.");
    }
    
    
    private void createDataObject( 
            final DatabaseSupport dbSupport,
            final UUID bucketId,
            final String objectName )
    {
        final S3Object object = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucketId )
                .setName( objectName )
                .setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( object );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Blob.class )
                .setByteOffset( 0 ).setLength( 1 ).setObjectId( object.getId() ) );
    }
}
