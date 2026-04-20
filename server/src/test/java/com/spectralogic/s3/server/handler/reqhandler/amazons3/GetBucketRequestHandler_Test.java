/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.domain.BucketObjectsApiBean;
import com.spectralogic.s3.server.handler.reqhandler.amazons3.GetBucketRequestHandler.ResultPage;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockHttpRequestUtil;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public final class GetBucketRequestHandler_Test
{

    @Test
    public void testGetBucketReturnsAppropriately1() throws IOException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final UUID jasonBucketId = mockDaoDriver.createBucket( jasonId, "bucket1" ).getId();
        createBucketAclForUser( 
                support.getDatabaseSupport(), jasonId, jasonBucketId, BucketAclPermission.OWNER );
        mockDaoDriver.createObject( jasonBucketId, "movies/", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/raw/", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/raw/chapters/", 0, new Date( 0 ) );
        

        mockDaoDriver.createObject( jasonBucketId, "scores/batman/original.mp3", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "song.mp3", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "music.mp3", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/cover.jpg", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/movie1.mov", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/movie2.mov", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/movie3.mov", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/raw/raw1.mov", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/raw/raw2.mov", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/raw/special/raw3.mov", 0, new Date( 0 ) );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() );
        driver.run();
        final InputStream propertiesIs = getClass().getResourceAsStream( "/expectedresponses.props" );
        final Properties properties = new Properties();
        properties.load( propertiesIs );
        
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.1" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.2" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.PREFIX, "scores/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.3" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.PREFIX, "movies/" )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.4" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.MAX_KEYS, "2" )
                        .addParameter( BucketObjectsApiBean.PREFIX, "movies/" )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.5" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.MAX_KEYS, "2" )
                        .addParameter( BucketObjectsApiBean.MARKER, "movies/cover.jpg" )
                        .addParameter( BucketObjectsApiBean.PREFIX, "movies/" )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.6" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.MAX_KEYS, "2" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.7" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.MAX_KEYS, "2" )
                        .addParameter( BucketObjectsApiBean.MARKER, "movies/movie1.mov" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.1.8" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
    }
    

    @Test
    public void testGetBucketReturnsAppropriately2() throws IOException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final UUID jasonBucketId = mockDaoDriver.createBucket( jasonId, "bucket1" ).getId();
        createBucketAclForUser( 
                support.getDatabaseSupport(), jasonId, jasonBucketId, BucketAclPermission.OWNER );

        mockDaoDriver.createObject( jasonBucketId, "README", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "LICENSE", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/avater.mpg", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/starwars.mpg", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "photos/one.jpg", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "photos/two.jpg", 0, new Date( 0 ) );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() );
        driver.run();
        final InputStream propertiesIs = getClass().getResourceAsStream( "/expectedresponses.props" );
        final Properties properties = new Properties();
        properties.load( propertiesIs );
        
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.1" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.2" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );

        mockDaoDriver.createObject( jasonBucketId, "nested/folder/object", 0, new Date( 0 ) );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.3" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.4" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
    }
    

    @Test
    public void testGetBucketReturnsAppropriately3() throws IOException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final UUID jasonBucketId = mockDaoDriver.createBucket( jasonId, "bucket1" ).getId();
        createBucketAclForUser( 
                support.getDatabaseSupport(), jasonId, jasonBucketId, BucketAclPermission.OWNER );

        mockDaoDriver.createObject( jasonBucketId, "README", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "LICENSE", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/avater.mpg", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "movies/starwars.mpg", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "photos/one.jpg", 0, new Date( 0 ) );
        mockDaoDriver.createObject( jasonBucketId, "photos/two.jpg", 0, new Date( 0 ) );

        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( com.spectralogic.s3.common.dao.domain.ds3.S3Object.class )
                .setBucketId( jasonBucketId )
                .setName( "LICENSE" )
                .setType( S3ObjectType.DATA )
                .setLatest( false ) );
        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( com.spectralogic.s3.common.dao.domain.ds3.S3Object.class )
                .setBucketId( jasonBucketId )
                .setName( "movies/avater.mpg" )
                .setType( S3ObjectType.DATA )
                .setLatest( false ) );
        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( com.spectralogic.s3.common.dao.domain.ds3.S3Object.class )
                .setBucketId( jasonBucketId )
                .setName( "photos/two.jpg" )
                .setType( S3ObjectType.DATA )
                .setLatest( false ) );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() );
        driver.run();
        final InputStream propertiesIs = getClass().getResourceAsStream( "/expectedresponses.props" );
        final Properties properties = new Properties();
        properties.load( propertiesIs );
        
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.1" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.2" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );

        mockDaoDriver.createObject( jasonBucketId, "nested/folder/object", 0, new Date( 0 ) );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.3" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                   .addParameter( BucketObjectsApiBean.DELIMITER, "" )
                   .addParameter( BucketObjectsApiBean.PREFIX, "" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.3" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "bucket1" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( BucketObjectsApiBean.DELIMITER, "/" );
        driver.run();
        MockHttpRequestUtil.assertResponseAsExpected(
                properties.getProperty( "getbucket.2.4" ).replace( "jason_UUID", jasonId.toString() ), 
                driver.getResponseToClientAsString() );
    }
    

    @Test
    public void testGetBucketWhenDelimiterImmediatelyAfterPrefix()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket" ).getId();
        mockDaoDriver.createObject( bucketId, "food", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo/", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo/baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo/bar/baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "baz/bat", 0, new Date( 0 ) );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "test_bucket" )
                        .addParameter( "prefix", "foo" )
                        .addParameter( "delimiter", "/" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/Contents)", "1" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/Contents[1]/Key", "food" );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/CommonPrefixes/Prefix)", "1" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/CommonPrefixes[1]/Prefix", "foo/" );
    }

    
    @Test
    public void testGetBucketWhenStandardDelimiterAndPrefixUsage()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, "test_bucket" ).getId();
        mockDaoDriver.createObject( bucketId, "food", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo/", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo/baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo/bar/baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "baz/bat", 0, new Date( 0 ) );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "test_bucket" )
                        .addParameter( "prefix", "foo/" )
                        .addParameter( "delimiter", "/" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/Contents)", "2" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/Contents[1]/Key", "foo/" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/Contents[2]/Key", "foo/baz" );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/CommonPrefixes/Prefix)", "1" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/CommonPrefixes[1]/Prefix", "foo/bar/" );
    }

    
    @Test
    public void testGetBucketWhenPrefixContainsNonStandardDelimiter()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, "test_bucket" ).getId();
        mockDaoDriver.createObject( bucketId, "food", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_quux", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_quux_deluxe", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_mux_foo", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_mux_bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "baz_bat", 0, new Date( 0 ) );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "test_bucket" )
                        .addParameter( "prefix", "foo_bar_" )
                        .addParameter( "delimiter", "_" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/Contents)", "2" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/Contents[1]/Key", "foo_bar_baz" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/Contents[2]/Key", "foo_bar_quux" );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/CommonPrefixes/Prefix)", "2" );
        driver.assertResponseToClientXPathEquals(
                "/ListBucketResult/CommonPrefixes[1]/Prefix", "foo_bar_mux_" );
        driver.assertResponseToClientXPathEquals(
                "/ListBucketResult/CommonPrefixes[2]/Prefix", "foo_bar_quux_" );
    }

    
    @Test
    public void testGetBucketWhenPrefixContainsNonStandardDelimiterAndUsingPaging()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, "test_bucket" ).getId();
        mockDaoDriver.createObject( bucketId, "food", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_quux", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_quux_deluxe", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_mux_foo", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_mux_bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "baz_bat", 0, new Date( 0 ) );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "test_bucket" )
                        .addParameter( "prefix", "foo_bar_" )
                        .addParameter( "delimiter", "_" )
                        .addParameter( "max-keys", "3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/Contents)", "2" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/Contents[1]/Key", "foo_bar_baz" );
        driver.assertResponseToClientXPathEquals( "/ListBucketResult/Contents[2]/Key", "foo_bar_quux" );
        driver.assertResponseToClientXPathEquals( "count(/ListBucketResult/CommonPrefixes/Prefix)", "1" );
        driver.assertResponseToClientXPathEquals(
                "/ListBucketResult/CommonPrefixes[1]/Prefix", "foo_bar_mux_" );
    }

    
    @Test
    public void testGetGetBucketAsJsonWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, "test_bucket" ).getId();
        mockDaoDriver.createObject( bucketId, "food", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_baz", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_quux", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_quux_deluxe", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_mux_foo", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "foo_bar_mux_bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "bar", 0, new Date( 0 ) );
        mockDaoDriver.createObject( bucketId, "baz_bat", 0, new Date( 0 ) );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "test_bucket" )
                        .addParameter( "prefix", "foo_bar_" )
                        .addParameter( "delimiter", "_" )
                        .addHeader( S3HeaderType.CONTENT_TYPE, "application/json" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "foo_bar_baz" );
        driver.assertResponseToClientContains( "foo_bar_quux" );
        driver.assertResponseToClientContains( "foo_bar_mux_" );
        driver.assertResponseToClientContains( "foo_bar_quux_" );
    }
    
    
    @Test
    public void testListObjectsInBucket1()
    {
        new GetBucketTestAdapter()
            .createObjects( "foo" )
            .getBucket( null, null, null, 1000 )
            .returns( new String[] { }, new String[] { "foo" }, null );
    }
    
    
    @Test
    public void testListObjectsInBucket2()
    {
        final GetBucketTestAdapter testAdapter = new GetBucketTestAdapter().createObjects(
                 "bar",
                 "baz_bat",
                 "foo_",
                 "foo_bar_baz",
                 "foo_bar_mux_bar",
                 "foo_bar_mux_foo",
                 "foo_bar_quux",
                 "foo_bar_quux_deluxe",
                 "foo_baz",
                 "food" );
        testAdapter
            .getBucket( "foo_bar_", "_", null, 3 )
            .returns(
                    new String[] { "foo_bar_mux_" },
                    new String[] { "foo_bar_baz", "foo_bar_quux" },
                    "foo_bar_quux" );
        testAdapter
            .getBucket( "foo_bar_", "_", "foo_bar_quux", 3 )
            .returns(
                    new String[] { "foo_bar_quux_" },
                    new String[] { },
                    null );
    }
    
    
    @Test
    public void testListObjectsInBucket3()
    {
        final GetBucketTestAdapter testAdapter = new GetBucketTestAdapter().createObjects(
                 "bar",
                 "baz_bat",
                 "foo_",
                 "foo_bar_baz",
                 "foo_bar_mux",
                 "foo_bar_mux_bar",
                 "foo_bar_mux_foo",
                 "foo_bar_quux",
                 "foo_bar_quux_deluxe",
                 "foo_baz",
                 "food" );
        testAdapter
            .getBucket( "foo_bar_", "_", null, 3 )
            .returns(
                    new String[] { "foo_bar_mux_" },
                    new String[] { "foo_bar_baz", "foo_bar_mux" },
                    "foo_bar_mux_" );
        testAdapter
            .getBucket( "foo_bar_", "_", "foo_bar_mux_", 3 )
            .returns(
                    new String[] { "foo_bar_quux_" },
                    new String[] { "foo_bar_quux" },
                    null );
    }
    
    
    @Test
    public void testListObjectsInBucket4()
    {
        final GetBucketTestAdapter testAdapter = new GetBucketTestAdapter().createObjects(
                 "bar",
                 "baz_bat",
                 "foo_",
                 "foo_bar_baz",
                 "foo_bar_mux",
                 "foo_bar_mux_",
                 "foo_bar_mux_bar",
                 "foo_bar_mux_foo",
                 "foo_bar_quux",
                 "foo_bar_quux_deluxe",
                 "foo_baz",
                 "food" );
        testAdapter
            .getBucket( "foo_bar_", "_", null, 3 )
            .returns(
                    new String[] { "foo_bar_mux_" },
                    new String[] { "foo_bar_baz", "foo_bar_mux" },
                    "foo_bar_mux_" );
        testAdapter
            .getBucket( "foo_bar_", "_", "foo_bar_mux_", 3 )
            .returns(
                    new String[] { "foo_bar_quux_" },
                    new String[] { "foo_bar_quux" },
                    null );
        testAdapter
            .getBucket( "foo_bar_mux_", "_", null, 3 )
            .returns(
                    new String[] {},
                    new String[] { "foo_bar_mux_", "foo_bar_mux_bar", "foo_bar_mux_foo" },
                    null );
    }
    
    
    private static final class GetBucketTestAdapter
    {
        GetBucketTestAdapter()
        {
            final DatabaseSupport dbSupport =
                    DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            m_mockDaoDriver = new MockDaoDriver( dbSupport );
            m_objectService = dbSupport.getServiceManager().getService( S3ObjectService.class );
            m_bucketId = m_mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME ).getId();
        }


        public GetBucketTestAdapter createObjects( final String ... names )
        {
            for ( final String name : names )
            {
                m_mockDaoDriver.createObject( m_bucketId, name );
            }
            return this;
        }


        public GetBucketTestResultVerifier getBucket(
                final String prefix,
                final String delimiter,
                final String marker,
                final int maxKeys )
        {
            return new GetBucketTestResultVerifier( GetBucketRequestHandler.listObjectsInBucket(
                    m_objectService,
                    m_bucketId,
                    prefix,
                    delimiter,
                    marker,
                    maxKeys,
                    false ) );
        }


        private final MockDaoDriver m_mockDaoDriver;
        private final BeansRetriever< S3Object > m_objectService;
        private final UUID m_bucketId;
    }//end inner class
    

    private static final class GetBucketTestResultVerifier
    {
        GetBucketTestResultVerifier( final ResultPage resultPage )
        {
            m_resultPage = resultPage;
        }
        
        
        public void returns(
                final String[] expectedCommonPrefixes,
                final String[] expectedObjects,
                final String expectedNextMarker )
        {
            final Object expected1 = CollectionFactory.toList( expectedCommonPrefixes );
            assertEquals(expected1,  new ArrayList<String>(m_resultPage.getPrefixes()), "Shoulda returned the expected common prefixes.");
            final List< String > objectNames = new ArrayList<>();
            for ( final S3Object object : m_resultPage.getObjects() )
            {
                objectNames.add( object.getName() );
            }
            final Object expected = CollectionFactory.toList( expectedObjects );
            assertEquals(expected,  objectNames, "Shoulda returned the expected object names.");
            assertEquals( expectedNextMarker,  m_resultPage.getNextMarker(), "Shoulda returned the expected next marker.");
        }
        
        
        private final ResultPage m_resultPage;
    }//end inner class

    
    @Test
    public void testExtractPrefix()
    {
        final Object actual2 = GetBucketRequestHandler.extractPrefix( "foo/", "/", "foo/bar/baz" );
        assertEquals( "foo/bar/", actual2, "Shoulda returned the common prefix based on a prefix and delimiter.");
        final Object actual1 = GetBucketRequestHandler.extractPrefix( "foo", "/", "foo/bar/baz" );
        assertEquals( "foo/", actual1, "Shoulda returned the common prefix based on a prefix and delimiter.");
        final Object actual = GetBucketRequestHandler.extractPrefix( null, "/", "foo/bar/baz" );
        assertEquals( "foo/", actual, "Shoulda returned the common prefix based on a null prefix and a delimiter.");
        assertEquals(null,  GetBucketRequestHandler.extractPrefix(null, "_", "foo/bar/baz"), "Shoulda returned null because the delimiter didn't exist.");
        assertEquals(null,  GetBucketRequestHandler.extractPrefix("foo/bar/", "/", "foo/bar/baz"), "Shoulda returned null because the delimiter didn't exist.");
    }
    
    
    void createBucketAclForUser( final DatabaseSupport dbSupport,
            final UUID userId, 
            final UUID bucketId, 
            final BucketAclPermission permission )
    {
        final BucketAcl acl = BeanFactory.newBean( BucketAcl.class )
                .setUserId( userId )
                .setBucketId( bucketId )
                .setPermission( permission );
        dbSupport.getDataManager().createBean( acl );
    }
}
