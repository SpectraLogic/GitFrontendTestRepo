/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectPropertyService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.object.BaseGetObjectsRequestHandler.DetailedS3Object;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;

public final class GetObjectsWithFullDetailsRequestHandler_Test 
{
    @Test
    public void testGetObjectsRespondsCorrectlyWhenFullDetails()
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

        mockDaoDriver.createObject( bucketJasonId, "jasonsong.mp3", -1 );
        mockDaoDriver.createObject( bucketJasonId, "jasonmusic.mp3", 0 );
        mockDaoDriver.createObject( bucketJasonId, "jasoncover.jpg", 87678 );

        final S3Object o = mockDaoDriver.createObject( bucketBarryId, "barrysong.mp3", -1 );
        mockDaoDriver.createBlobs( o.getId(), 1, 65455 );
        mockDaoDriver.createBlobs( o.getId(), 1, 1, 1 );
        mockDaoDriver.createObject( bucketBarryId, "barrymusic.mp3", 76567 );
        mockDaoDriver.createObject( bucketBarryId, "barrycover.jpg", 98789 );
        
        final BeansServiceManager transaction =
                support.getDatabaseSupport().getServiceManager().startTransaction();
        try
        {
            transaction.getService( S3ObjectPropertyService.class ).create(
                    CollectionFactory.toSet( BeanFactory.newBean( S3ObjectProperty.class )
                            .setObjectId( o.getId() )
                            .setKey( S3HeaderType.ETAG.getHttpHeaderName() ).setValue( "myetag" ) ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "%" )
                        .addParameter( RequestParameterType.FULL_DETAILS.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "myetag" );
        driver.assertResponseToClientContains( "98789" );
        driver.assertResponseToClientContains( "87678" );
        driver.assertResponseToClientContains( "76567" );
        driver.assertResponseToClientContains( "65456" );
        driver.assertResponseToClientContains( ">jason<" );
        driver.assertResponseToClientContains( ">barry<" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "admin" ),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "myetag" );
        driver.assertResponseToClientDoesNotContain( "98789" );
        driver.assertResponseToClientDoesNotContain( "87678" );
        driver.assertResponseToClientDoesNotContain( "76567" );
        driver.assertResponseToClientDoesNotContain( "65456" );
        driver.assertResponseToClientDoesNotContain( ">jason<" );
        driver.assertResponseToClientDoesNotContain( ">barry<" );
    }
    
    
    @Test
    public void testGetObjectsCalculatesBlobsNotYetCompletelyPersistedCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 5, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 7, 100 );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        
        mockDaoDriver.createJobWithEntries(
                JobRequestType.PUT, 
                blobs1.get( 1 ), blobs1.get( 2 ), blobs1.get( 3 ), blobs2.get( 6 ) );
        mockDaoDriver.createJobWithEntries(
                JobRequestType.GET, 
                blobs2.get( 0 ), blobs2.get( 1 ), blobs2.get( 2 ), blobs1.get( 0 ) );
        mockDaoDriver.createJobWithEntries(
                JobRequestType.VERIFY, 
                blobs2.get( 3 ), blobs2.get( 4 ), blobs2.get( 5 ), blobs1.get( 0 ) );
        mockDaoDriver.createDegradedBlob( blobs2.get( 0 ).getId(), rule.getId() );
        mockDaoDriver.createDegradedBlob( blobs2.get( 0 ).getId(), rule2.getId() );
        mockDaoDriver.createDegradedBlob( blobs2.get( 1 ).getId(), rule.getId() );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.name() )
                        .addParameter( "name", "o1" )
                        .addParameter( RequestParameterType.FULL_DETAILS.toString(), "1" )
                        .addParameter( RequestParameterType.INCLUDE_PHYSICAL_PLACEMENT.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "o1" );
        driver.assertResponseToClientDoesNotContain( "o2" );
        
        driver.assertResponseToClientContains( DetailedS3Object.BLOBS_BEING_PERSISTED + ">3</" );
        driver.assertResponseToClientContains( DetailedS3Object.BLOBS_DEGRADED + ">0</" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.name() )
                        .addParameter( "name", "o2" )
                        .addParameter( RequestParameterType.FULL_DETAILS.toString(), "1" )
                        .addParameter( RequestParameterType.INCLUDE_PHYSICAL_PLACEMENT.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "o1" );
        driver.assertResponseToClientContains( "o2" );
        
        driver.assertResponseToClientContains( DetailedS3Object.BLOBS_BEING_PERSISTED + ">1</" );
        driver.assertResponseToClientContains( DetailedS3Object.BLOBS_DEGRADED + ">2</" );
    }
}
