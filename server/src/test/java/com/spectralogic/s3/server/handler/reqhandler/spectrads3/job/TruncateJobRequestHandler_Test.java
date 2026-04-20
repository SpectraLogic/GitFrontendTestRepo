/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.server.mock.CancelJobInvocationHandler;
import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class TruncateJobRequestHandler_Test 
{
    @Test
    public void testCancelJobCreatedByRequestingUserCallsCancelJobOnDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.values()[ 0 ], blob );
        
        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        cancelJobInvocationHandler.setDeleteJobUponCancel( true );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE, 
                "_rest_/job/" + chunk.getJobId() )
            .addParameter( RequestParameterType.FORCE.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver.assertResponseToClientDoesNotContain( chunk.getJobId().toString() );
        
        assertEquals(
                CollectionFactory.toList( chunk.getJobId() ),
                cancelJobInvocationHandler.getJobIds(),
                "Shoulda been called once with the expected job id."
                 );
    }
}
