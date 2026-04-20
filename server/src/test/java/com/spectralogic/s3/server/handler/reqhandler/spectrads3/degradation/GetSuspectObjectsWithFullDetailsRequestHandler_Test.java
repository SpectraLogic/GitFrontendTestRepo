/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;


public final class GetSuspectObjectsWithFullDetailsRequestHandler_Test
{
     @Test
    public void testGetDoesSoWhenPermitted()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User jason = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.addUserMemberToGroup( 
                BuiltInGroup.ADMINISTRATORS, 
                mockDaoDriver.createUser( "testUser" ).getId() );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember tsdm1 =
                mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tape1.getPartitionId(), TapeType.LTO5 );
        final StorageDomainMember psdm2 =
                mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), pool1.getPartitionId() );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( tsdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( tsdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( psdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( psdm2.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        mockDaoDriver.addBucketAcl( bucket1.getId(), null, jason.getId(), BucketAclPermission.LIST );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "object1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final BlobTape btape = mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() );
        final BlobPool bpool = mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        final BlobDs3Target btarget = mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() ) );
        
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "object2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), b2.getId() );
        mockDaoDriver.putBlobOnDs3Target( target1.getId(), b2.getId() );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool2.getId(), b2.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnDs3Target( target2.getId(), b2.getId() ) );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_OBJECT )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "testUser" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_OBJECT )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( o1.getName() );
        driver.assertResponseToClientContains( o2.getName() );
        driver.assertResponseToClientContains( bucket1.getName() );
        driver.assertResponseToClientContains( bucket2.getName() );
        driver.assertResponseToClientDoesNotContain( tape1.getId().toString() );
        driver.assertResponseToClientContains( tape2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool1.getId().toString() );
        driver.assertResponseToClientContains( pool2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target1.getId().toString() );
        driver.assertResponseToClientContains( target2.getId().toString() );
        driver.assertResponseToClientContains( bucket1.getName() );
        
        mockDaoDriver.makeSuspect( btape );
        mockDaoDriver.makeSuspect( bpool );
        mockDaoDriver.makeSuspect( btarget );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "testUser" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_OBJECT )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( o1.getName() );
        driver.assertResponseToClientContains( o2.getName() );
        driver.assertResponseToClientContains( bucket1.getName() );
        driver.assertResponseToClientContains( bucket2.getName() );
        driver.assertResponseToClientContains( tape1.getId().toString() );
        driver.assertResponseToClientContains( tape2.getId().toString() );
        driver.assertResponseToClientContains( pool1.getId().toString() );
        driver.assertResponseToClientContains( pool2.getId().toString() );
        driver.assertResponseToClientContains( target1.getId().toString() );
        driver.assertResponseToClientContains( target2.getId().toString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_OBJECT )
            .addParameter( PersistenceTarget.BUCKET_ID, bucket1.getName() )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( o1.getName() );
        driver.assertResponseToClientDoesNotContain( o2.getName() );
        driver.assertResponseToClientContains( bucket1.getName() );
        driver.assertResponseToClientDoesNotContain( bucket2.getName() );
        driver.assertResponseToClientContains( tape1.getId().toString() );
        driver.assertResponseToClientContains( tape2.getId().toString() );
        driver.assertResponseToClientContains( pool1.getId().toString() );
        driver.assertResponseToClientContains( pool2.getId().toString() );
        driver.assertResponseToClientContains( target1.getId().toString() );
        driver.assertResponseToClientContains( target2.getId().toString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_OBJECT )
            .addParameter( PersistenceTarget.BUCKET_ID, bucket2.getName() )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "testUser" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_OBJECT )
            .addParameter( PersistenceTarget.BUCKET_ID, bucket2.getName() )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( o1.getName() );
        driver.assertResponseToClientContains( o2.getName() );
        driver.assertResponseToClientDoesNotContain( bucket1.getName() );
        driver.assertResponseToClientContains( bucket2.getName() );
        driver.assertResponseToClientDoesNotContain( tape1.getId().toString() );
        driver.assertResponseToClientContains( tape2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool1.getId().toString() );
        driver.assertResponseToClientContains( pool2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target1.getId().toString() );
        driver.assertResponseToClientContains( target2.getId().toString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "testUser" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_OBJECT )
            .addParameter( PersistenceTarget.BUCKET_ID, bucket2.getName() )
            .addParameter( RequestParameterType.STORAGE_DOMAIN.toString(), sd1.getId().toString() )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( o1.getName() );
        driver.assertResponseToClientContains( o2.getName() );
        driver.assertResponseToClientDoesNotContain( bucket1.getName() );
        driver.assertResponseToClientContains( bucket2.getName() );
        driver.assertResponseToClientDoesNotContain( tape1.getId().toString() );
        driver.assertResponseToClientContains( tape2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target2.getId().toString() );
    }
}
