/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.server.mock.CancelJobInvocationHandler;
import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CancelJobRequestHandler_Test 
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
        cancelJobInvocationHandler.m_deleteJobUponCancel = true;
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE, 
                "_rest_/job/" + chunk.getJobId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver.assertResponseToClientDoesNotContain( chunk.getJobId().toString() );

        final Object expected = CollectionFactory.toList( chunk.getJobId() );
        assertEquals(expected, cancelJobInvocationHandler.getJobIds(), "Shoulda been called once with the expected job id.");
    }
    
    
    @Test
    public void testCancelJobCreatedByDifferentUserAllowedOnlyIfUserHasPermissionToDoSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.values()[ 0 ], blob );
        
        final User user2 = mockDaoDriver.createUser( "user2" );
        
        final CancelJobInvocationHandler cancelJobInvocationHandler = 
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.DELETE, 
                "_rest_/job/" + chunk.getJobId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        mockDaoDriver.addBucketAcl( bucket.getId(), null, user2.getId(), BucketAclPermission.JOB );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user2.getName() ),
                RequestType.DELETE, 
                "_rest_/job/" + chunk.getJobId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        final Object expected = CollectionFactory.toList( chunk.getJobId() );
        assertEquals(expected, cancelJobInvocationHandler.getJobIds(), "Shoulda been called once with the expected job id.");
    }
    
    
    @Test
    public void testCancelJobWhereJobNotCanceledReturnsNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        
        final CancelJobInvocationHandler cancelJobInvocationHandler = 
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.DELETE, 
                "_rest_/job/" + job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver.assertResponseToClientDoesNotContain( job.getId().toString() );

        final Object expected = CollectionFactory.toList( job.getId() );
        assertEquals(expected, cancelJobInvocationHandler.getJobIds(), "Shoulda been called once with the expected job id.");
    }
    
    
    @Test
    public void testCancelReplicatedJobWhereJobNotCanceledReturnsNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        
        final CancelJobInvocationHandler cancelJobInvocationHandler = 
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.DELETE, 
                "_rest_/job/" + job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver.assertResponseToClientDoesNotContain( job.getId().toString() );

        final Object expected = CollectionFactory.toList( job.getId() );
        assertEquals(expected, cancelJobInvocationHandler.getJobIds(), "Shoulda been called once with the expected job id.");
    }


    @Test
    public void testCancelJobWithProtectedFlagFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );

        mockDaoDriver.updateBean( job.setProtected( true ), Job.PROTECTED );

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );

        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.DELETE,
                "_rest_/job/" + job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        final Object expected = CollectionFactory.toList();
        assertEquals(expected, cancelJobInvocationHandler.getJobIds(), "Should not have called cancellation invocation handler.");
    }


}
