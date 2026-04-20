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
import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
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

public final class GetCanceledJobsRequestHandler_Test 
{
    @Test
    public void testGetCanceledJobsDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final UUID jobId1 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 ).getJobId();
        final UUID jobId2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId();
        final UUID jobId3 =
                mockDaoDriver.createCanceledJob( null, null, JobRequestType.GET, new Date() ).getId();
        final UUID jobId4 = 
                mockDaoDriver.createCanceledJob( null, null, JobRequestType.PUT, new Date() ).getId();
        
        // user does not have access
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( jobId1.toString() );
        driver.assertResponseToClientDoesNotContain( jobId2.toString() );
        driver.assertResponseToClientDoesNotContain( jobId3.toString() );
        driver.assertResponseToClientDoesNotContain( jobId4.toString() );
        
        // user has access
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( jobId1.toString() );
        driver.assertResponseToClientDoesNotContain( jobId2.toString() );
        driver.assertResponseToClientContains( jobId3.toString() );
        driver.assertResponseToClientContains( jobId4.toString() );
        
        // shoulda filtered results
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB )
            .addParameter( JobObservable.REQUEST_TYPE, JobRequestType.PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( jobId1.toString() );
        driver.assertResponseToClientDoesNotContain( jobId2.toString() );
        driver.assertResponseToClientDoesNotContain( jobId3.toString() );
        driver.assertResponseToClientContains( jobId4.toString() );
        
        // verify sort order
        final CanceledJob job3 = mockDaoDriver.attain( CanceledJob.class, jobId3 );
        final CanceledJob job4 = mockDaoDriver.attain( CanceledJob.class, jobId4 );
        mockDaoDriver.updateBean(
                job3.setDateCanceled( new Date( 2000 ) ),
                CanceledJob.DATE_CANCELED );
        mockDaoDriver.updateBean(
                job4.setDateCanceled( new Date( 1000 ) ),
                CanceledJob.DATE_CANCELED );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final int location1 = driver.getResponseToClientAsString().indexOf( jobId3.toString() );
        final int location2 = driver.getResponseToClientAsString().indexOf( jobId4.toString() );
        assertTrue(
                location2 > location1,
                "Shoulda sorted by cancelation date."
                 );
    }
}
