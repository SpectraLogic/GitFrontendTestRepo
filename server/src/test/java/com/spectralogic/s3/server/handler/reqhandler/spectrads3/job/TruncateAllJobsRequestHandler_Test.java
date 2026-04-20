/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.HashSet;

import com.spectralogic.s3.server.mock.CancelJobInvocationHandler;
import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class TruncateAllJobsRequestHandler_Test 
{
    @Test
    public void testCancelWithoutFiltersCancelsAllWhenInvocationsToCancelSucceed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        final Job job1 = mockDaoDriver.createJob( bucket1.getId(), null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( bucket1.getId(), null, JobRequestType.PUT );
        final Job job3 = mockDaoDriver.createJob( bucket2.getId(), null, JobRequestType.PUT );
        
        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/job/" )
            .addParameter( RequestParameterType.FORCE.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver.assertResponseToClientDoesNotContain( job1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );
        
        assertEquals(
                CollectionFactory.toSet( job1.getId(), job2.getId(), job3.getId() ),
                new HashSet<>( cancelJobInvocationHandler.getJobIds() ),
                "Shoulda been called once with the expected job id."
                 );
    }
}
