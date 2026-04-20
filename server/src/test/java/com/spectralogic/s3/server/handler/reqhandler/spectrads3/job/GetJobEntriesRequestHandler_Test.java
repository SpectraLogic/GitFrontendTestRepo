/*******************************************************************************
 *
 * Copyright C 2025, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetJobEntriesRequestHandler_Test
{
    @Test
    public void testGetJobEntriesReturnsAllEntriesForJob()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final User user = mockDaoDriver.createUser( "user1" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket" );

        // Create first job with 2 entries
        final Blob blob1 = mockDaoDriver.getBlobFor(
                mockDaoDriver.createObject( bucket.getId(), "obj1", 1024L ).getId() );
        final Blob blob2 = mockDaoDriver.getBlobFor(
                mockDaoDriver.createObject( bucket.getId(), "obj2", 2048L ).getId() );
        final JobEntry entry1 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, blob1 );
        final UUID jobId = entry1.getJobId();
        final JobEntry entry2 = mockDaoDriver.createJobEntry( jobId, blob2 );

        // Create second job with 1 entry (to verify filtering works)
        final Blob blob3 = mockDaoDriver.getBlobFor(
                mockDaoDriver.createObject( bucket.getId(), "obj3", 512L ).getId() );
        final JobEntry entry3 = mockDaoDriver.createJobWithEntry( JobRequestType.GET, blob3 );

        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        // Request entries for the first job
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET,
                "_rest_/" + RestDomainType.JOB_CHUNK_DAO )
            .addParameter( JobEntry.JOB_ID, jobId.toString() );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        // Should contain both entries from first job
        driver.assertResponseToClientContains( entry1.getId().toString() );
        driver.assertResponseToClientContains( entry2.getId().toString() );

        // Should NOT contain entry from second job
        driver.assertResponseToClientDoesNotContain( entry3.getId().toString() );
    }
}
