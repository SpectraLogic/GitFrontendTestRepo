/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;

public final class GetJobChunkDaoRequestHandler_Test 
{
    @Test
    public void testGetJobChunkReturnsValidResponse()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket" );
        final Blob blob = mockDaoDriver.getBlobFor( 
                mockDaoDriver.createObject( bucket.getId(), "foo", 1024L ).getId() );
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.JOB_CHUNK_DAO + "/" + chunk.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "foo" );
        driver.assertResponseToClientContains( JobChunkBlobStoreState.PENDING.toString() );
    }
    
    
    @Test
    public void testGetJobChunkWhenUserCreatedJobAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket" );
        final Blob blob = mockDaoDriver.getBlobFor( 
                mockDaoDriver.createObject( bucket.getId(), "foo", 1024L ).getId() );
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.JOB_CHUNK_DAO + "/" + chunk.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testGetJobChunkWhenUserDoesNotHaveAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket" );
        final Blob blob = mockDaoDriver.getBlobFor( 
                mockDaoDriver.createObject( bucket.getId(), "foo", 1024L ).getId() );
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.JOB_CHUNK_DAO + "/" + chunk.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
