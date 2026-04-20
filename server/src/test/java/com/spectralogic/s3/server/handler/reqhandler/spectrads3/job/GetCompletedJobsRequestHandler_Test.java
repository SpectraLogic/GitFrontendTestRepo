/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.CompletedJob;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class GetCompletedJobsRequestHandler_Test 
{
    @Test
    public void testGetCompletedJobsDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final UUID jobId1 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 ).getJobId();
        final UUID jobId2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId();
        final UUID jobId3 = mockDaoDriver.createJob( null, null, JobRequestType.GET ).getId();
        
        // user does not have access
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.COMPLETED_JOB );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( jobId1.toString() );
        driver.assertResponseToClientDoesNotContain( jobId2.toString() );
        driver.assertResponseToClientDoesNotContain( jobId3.toString() );
        
        // user has access
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.COMPLETED_JOB );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( jobId1.toString() );
        driver.assertResponseToClientContains( jobId2.toString() );
        driver.assertResponseToClientContains( jobId3.toString() );
        
        // shoulda filtered results
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.COMPLETED_JOB )
            .addParameter( JobObservable.REQUEST_TYPE, JobRequestType.PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( jobId1.toString() );
        driver.assertResponseToClientContains( jobId2.toString() );
        driver.assertResponseToClientDoesNotContain( jobId3.toString() );
        
        // verify sort order
        final CompletedJob job2 = mockDaoDriver.attain( CompletedJob.class, jobId2 );
        final CompletedJob job3 = mockDaoDriver.attain( CompletedJob.class, jobId3 );
        mockDaoDriver.updateBean(
                job2.setDateCompleted( new Date( 2000 ) ),
                CompletedJob.DATE_COMPLETED );
        mockDaoDriver.updateBean(
                job3.setDateCompleted( new Date( 1000 ) ),
                CompletedJob.DATE_COMPLETED );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.COMPLETED_JOB );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final int location1 = driver.getResponseToClientAsString().indexOf( jobId2.toString() );
        final int location2 = driver.getResponseToClientAsString().indexOf( jobId3.toString() );
        assertTrue(
                location2 > location1,
                "Shoulda sorted by completion date."
                );
    }
}
