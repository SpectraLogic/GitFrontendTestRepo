/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.Date;
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

public final class GetCanceledJobRequestHandler_Test 
{
    @Test
    public void testGetDoesSoWhenUserHasAccess()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final User otherUser = mockDaoDriver.createUser( "userbob" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID jobId1 = 
                mockDaoDriver.createCanceledJob( null, null, JobRequestType.PUT, new Date() ).getId();
        final UUID jobId2 = mockDaoDriver.createJob( null, null, JobRequestType.GET ).getId();
        final UUID jobId3 = mockDaoDriver.createJobWithEntry( JobRequestType.GET, b2 ).getJobId();
        
        // user does not have access
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( otherUser.getName() ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB + "/" + jobId1 );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        // user has access
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB + "/" + jobId1 );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB + "/" + jobId2 );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CANCELED_JOB + "/" + jobId3 );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
}
