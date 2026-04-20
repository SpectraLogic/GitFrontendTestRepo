/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public final class GetJobSummaryRequestHandler_Test 
{
    @Test
    public void testGetJobInfoReturnsCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final UUID jobId = 
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b ).getJobId();
        mockDaoDriver.attain(Job.class, jobId);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString())
                .addParameter( RequestParameterType.SUMMARY.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
    }

    @Test
    public void testPutJobInfoReturnsCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b );
        mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());
        final UUID jobId = chunk.getJobId();
        mockDaoDriver.attain(Job.class, jobId);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/job/" + jobId.toString())
                .addParameter( RequestParameterType.SUMMARY.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( chunk.getId().toString() );
        driver.assertResponseToClientContains( mockDaoDriver.attainOneAndOnly(StorageDomain.class).getId().toString() );
        driver.assertResponseToClientContains( mockDaoDriver.attainOneAndOnly(StorageDomain.class).getName() );
    }

    @Test
    public void testPutJobInfoWithTargetReturnsCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Ds3Target ds3Target = mockDaoDriver.createDs3Target("ds3Target1");
        final Ds3Target ds3Target2 = mockDaoDriver.createDs3Target("ds3Target2");
        final S3Target s3Target = mockDaoDriver.createS3Target("s3TargetA");
        mockDaoDriver.createDs3DataReplicationRule(dp.getId(), DataReplicationRuleType.PERMANENT, ds3Target.getId());
        mockDaoDriver.createDs3DataReplicationRule(dp.getId(), DataReplicationRuleType.PERMANENT, ds3Target2.getId());
        mockDaoDriver.createS3DataReplicationRule(dp.getId(), DataReplicationRuleType.PERMANENT, s3Target.getId());

        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b );
        mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());
        mockDaoDriver.createReplicationTargetsForChunk(Ds3DataReplicationRule.class, Ds3BlobDestination.class, chunk.getId());
        mockDaoDriver.createReplicationTargetsForChunk(S3DataReplicationRule.class, S3BlobDestination.class, chunk.getId());

        final S3Target s3Target2 = mockDaoDriver.createS3Target("s3TargetB"); //should not be in response

        final UUID jobId = chunk.getJobId();
        mockDaoDriver.attain(Job.class, jobId);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/job/" + jobId.toString())
                .addParameter( RequestParameterType.SUMMARY.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( jobId.toString() );
        driver.assertResponseToClientContains( chunk.getId().toString() );
        driver.assertResponseToClientContains( mockDaoDriver.attainOneAndOnly(StorageDomain.class).getName());
        driver.assertResponseToClientContains( ds3Target.getName() );
        driver.assertResponseToClientContains( ds3Target2.getName());
        driver.assertResponseToClientContains( s3Target.getName() );
        driver.assertResponseToClientDoesNotContain( s3Target2.getName() );
    }
}
