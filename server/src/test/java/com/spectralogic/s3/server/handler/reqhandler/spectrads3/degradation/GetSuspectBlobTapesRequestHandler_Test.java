/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;



import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetSuspectBlobTapesRequestHandler_Test 
{
     @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "jason" );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target target2 = mockDaoDriver.createDs3Target( "t2" );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final BlobTape btape = mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() );
        final BlobPool bpool = mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() );
        final BlobDs3Target btarget = mockDaoDriver.putBlobOnDs3Target( target1.getId(), b1.getId() );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnDs3Target( target2.getId(), b1.getId() ) );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_BLOB_TAPE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( tape1.getId().toString() );
        driver.assertResponseToClientContains( tape2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target2.getId().toString() );
        
        mockDaoDriver.makeSuspect( btape );
        mockDaoDriver.makeSuspect( bpool );
        mockDaoDriver.makeSuspect( btarget );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SUSPECT_BLOB_TAPE )
            .addParameter( BlobTape.TAPE_ID, tape1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( tape1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( tape2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( pool2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( target2.getId().toString() );
    }
}
