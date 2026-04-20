/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetCompletedJobRequestHandler_Test 
{
    @Test
    public void testGetDoesSoWhenUserHasAccess()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final User otherUser = mockDaoDriver.createUser( "userbob" );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final UUID jobId1 = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 ).getJobId();
        final UUID jobId2 = mockDaoDriver.createJob( null, null, JobRequestType.GET ).getId();
        
        // user does not have access
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.COMPLETED_JOB + "/" + jobId1 );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( otherUser.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.COMPLETED_JOB + "/" + jobId2 );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.COMPLETED_JOB + "/" + jobId2 );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
