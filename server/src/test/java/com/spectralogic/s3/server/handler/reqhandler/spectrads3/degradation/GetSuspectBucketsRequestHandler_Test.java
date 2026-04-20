/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetSuspectBucketsRequestHandler_Test 
{
     @Test
    public void testGetSuspectBucketsOnlyReturnsBucketsThatHaveSuspectDataLossThatUserAllowedToSee()
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
        mockDaoDriver.addBucketAcl( bucketJasonId, null, barryId, BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( bucketBarryId, null, barryId, BucketAclPermission.LIST );

        mockDaoDriver.createObject( bucketJasonId, "o1" );
        final S3Object jasonObject = mockDaoDriver.createObject( bucketJasonId, "o2" );
        final Blob jasonBlob = mockDaoDriver.getBlobFor( jasonObject.getId() );
        mockDaoDriver.createObject( bucketBarryId, "o1" );
        final S3Object barryObject = mockDaoDriver.createObject( bucketBarryId, "o2" );
        final Blob barryBlob = mockDaoDriver.getBlobFor( barryObject.getId() );
        
        final Tape tape = mockDaoDriver.createTape();
        final BlobTape barryBlobTape = mockDaoDriver.putBlobOnTape( tape.getId(), barryBlob.getId() );
        final BlobTape jasonBlobTape = mockDaoDriver.putBlobOnTape( tape.getId(), jasonBlob.getId() );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_BUCKET );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "bucketjason" );
        driver.assertResponseToClientDoesNotContain( "bucketbarry" );
        
        mockDaoDriver.makeSuspect( jasonBlobTape );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_BUCKET );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "bucketjason" );
        driver.assertResponseToClientDoesNotContain( "bucketbarry" );

        mockDaoDriver.makeSuspect( barryBlobTape );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_BUCKET );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "bucketjason" );
        driver.assertResponseToClientDoesNotContain( "bucketbarry" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_BUCKET );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "bucketjason" );
        driver.assertResponseToClientContains( "bucketbarry" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_BUCKET ).addParameter( Bucket.NAME, "%barry%" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "bucketjason" );
        driver.assertResponseToClientContains( "bucketbarry" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DEGRADED_BUCKET );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "bucketjason" );
        driver.assertResponseToClientDoesNotContain( "bucketbarry" );
    }
}
