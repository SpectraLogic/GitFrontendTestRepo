/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.marshal.JsonMarshaler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class GetJobToReplicateRequestHandler_Test 
{

    @Test
    public void testGetReturnsCorrectlyWhenXmlFormatRequested()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final UUID jobId = 
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b ).getJobId();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() )
            .addParameter( RequestParameterType.REPLICATE.toString(), "" )
            .addHeader( S3HeaderType.CONTENT_TYPE, "application/xml" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final JobToReplicate jobToReplicate = 
                JsonMarshaler.unmarshal( JobToReplicate.class, driver.getResponseToClientAsString() );
        assertEquals( 1,  jobToReplicate.getBlobs().length, "Shoulda generated proper response of job information.");

        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        final JobReplicationSupport jrs = new JobReplicationSupport( 
                support.getDatabaseSupport().getServiceManager(),
                BeanFactory.newBean( DetailedJobToReplicate.class )
                .setJob( jobToReplicate ).setUserId( user.getId() ).setBucketId( bucket.getId() ) );
        final Object expected = b.getId();
        assertEquals(expected,  jrs.getBlobs().iterator().next().getId(), "Shoulda generated proper response of job information.");
    }
    
    
    @Test
    public void testGetReturnsCorrectlyWhenJsonFormatRequested()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob b = mockDaoDriver.getBlobFor( o.getId() );
        final UUID jobId = 
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT,  b ).getJobId();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/job/" + jobId.toString() )
            .addParameter( RequestParameterType.REPLICATE.toString(), "" )
            .addHeader( S3HeaderType.CONTENT_TYPE, "application/json" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final JobToReplicate jobToReplicate = 
                JsonMarshaler.unmarshal( JobToReplicate.class, driver.getResponseToClientAsString() );
        assertEquals( 1,  jobToReplicate.getBlobs().length, "Shoulda generated proper response of job information.");

        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        final JobReplicationSupport jrs = new JobReplicationSupport( 
                support.getDatabaseSupport().getServiceManager(),
                BeanFactory.newBean( DetailedJobToReplicate.class )
                .setJob( jobToReplicate ).setUserId( user.getId() ).setBucketId( bucket.getId() ) );
        final Object expected = b.getId();
        assertEquals(expected,  jrs.getBlobs().iterator().next().getId(), "Shoulda generated proper response of job information.");
    }
}
