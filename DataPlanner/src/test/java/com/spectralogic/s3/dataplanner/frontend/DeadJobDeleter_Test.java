/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashSet;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.rpc.dataplanner.CancelJobFailedException;
import com.spectralogic.util.exception.GenericFailure;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class DeadJobDeleter_Test 
{
    @Test
    public void testCancelCalledOnResourceForDeadJobs()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );
        
        final Job job1 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.GET ).setBucketId( bucket.getId() )
                .setUserId( user.getId() ).setCreatedAt( new Date() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job1 );
        
        final Job job2 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() ).setCreatedAt( new Date() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job2 );
        
        final CanceledJob canceledJob = BeanFactory.newBean( CanceledJob.class )
                .setBucketId( bucket.getId() )
                .setRequestType( job2.getRequestType() )
                .setUserId( user.getId() );
        canceledJob.setId( job2.getId() );
        dbSupport.getDataManager().createBean( canceledJob );
        assertFalse(
                mockDaoDriver.attain( canceledJob ).isCanceledDueToTimeout(),
                "Should notta marked job as canceled due to timeout yet."
                 );
        
        final Job job3 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() ).setCreatedAt( new Date() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job3 );
        
        final Job job4 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() ).setCreatedAt( new Date() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job4 );

        final S3Object o0 = mockDaoDriver.createObject(null, "o0");
        final Blob blob0 = mockDaoDriver.getBlobFor(o0.getId());
        final JobEntry chunk0 = BeanFactory.newBean( JobEntry.class )
                .setBlobId(blob0.getId())
                .setJobId( job1.getId() ).setChunkNumber( 1 )
                .setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS );
        dbSupport.getDataManager().createBean( chunk0 );

        final S3Object o1 = mockDaoDriver.createObject(null, "o1");
        final Blob blob1 = mockDaoDriver.getBlobFor(o1.getId());
        final JobEntry chunk1 = BeanFactory.newBean( JobEntry.class )
                .setBlobId(blob1.getId())
                .setJobId( job3.getId() ).setChunkNumber( 1 );
        dbSupport.getDataManager().createBean( chunk1 );

        final S3Object o2 = mockDaoDriver.createObject(null, "o2");
        final Blob blob2 = mockDaoDriver.getBlobFor(o2.getId());
        final JobEntry chunk2 = BeanFactory.newBean( JobEntry.class )
                .setBlobId(blob2.getId())
                .setJobId( job4.getId() ).setChunkNumber( 1 );
        dbSupport.getDataManager().createBean( chunk2 );

        final S3Object o3 = mockDaoDriver.createObject(null, "o3");
        final Blob blob3 = mockDaoDriver.getBlobFor(o3.getId());
        final JobEntry chunk3 = BeanFactory.newBean( JobEntry.class )
                .setBlobId(blob3.getId())
                .setJobId( job4.getId() ).setChunkNumber( 2 )
                .setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS );
        dbSupport.getDataManager().createBean( chunk3 );
        
        final BasicTestsInvocationHandler monitorBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        boolean.class, 
                        new InvocationHandler()
        {
            public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
            {
                return Boolean.valueOf( job2.getId().equals( args[ 0 ] ) );
            }
        }, null ) );
        
        final Method methodIsDead = 
                ReflectUtil.getMethod( DeadJobMonitor.class, "isDead" );
        final BasicTestsInvocationHandler resourceBtih = new BasicTestsInvocationHandler(
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        return null;
                    }
                } );
        final DeadJobDeleter deleter = new DeadJobDeleter(
                200, 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, monitorBtih ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, resourceBtih ) );
        assertEquals(0,  monitorBtih.getMethodCallCount(methodIsDead), "Should notta made any calls on monitor yet.");
        assertEquals(0,  resourceBtih.getTotalCallCount(), "Should notta tried to delete anything yet.");

        TestUtil.sleep( 300 );
        assertEquals(4,  monitorBtih.getMethodCallCount(methodIsDead), "Shoulda made a call for each job as to whether or not it's dead.");
        assertEquals(2,  resourceBtih.getTotalCallCount(), "Shoulda made two calls, one to cleanup and one to cancel job on job2.");
        MethodInvokeData mid = resourceBtih.getMethodInvokeData().get( 0 );
        assertEquals("cleanUpCompletedJobsAndJobChunks", mid.getMethod().getName(), "Shoulda made single call to cleanup chunks and jobs.");
        mid = resourceBtih.getMethodInvokeData().get( 1 );
        assertEquals("cancelJob", mid.getMethod().getName(), "Shoulda made single call to cancel job on job2.");
        final Object expected = job2.getId();
        assertEquals(expected, mid.getArgs().get( 1 ), "Shoulda made single call to cancel job on job2.");
        assertTrue(mockDaoDriver.attain( canceledJob ).isCanceledDueToTimeout(), "Shoulda marked job as canceled due to timeout.");

        deleter.shutdown();
    }


    @Test
    public void testFailedCancelDoesNotAbortCleanup()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );

        final Job job1 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() ).setCreatedAt( new Date() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job1 );

        final Job job2 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() ).setCreatedAt( new Date() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job2 );

        final BasicTestsInvocationHandler monitorBtih =
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        boolean.class,
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                            {
                                return Boolean.TRUE;
                            }
                        }, null ) );

        final Method methodIsDead =
                ReflectUtil.getMethod( DeadJobMonitor.class, "isDead" );
        final BasicTestsInvocationHandler resourceBtih = new BasicTestsInvocationHandler(
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        if (method.getName().equals("cleanUpCompletedJobsAndJobChunks")) {
                            return null;
                        }
                        throw new CancelJobFailedException(GenericFailure.FORCE_FLAG_REQUIRED_OK, "Failed to cancel job", new HashSet<>());
                    }
                } );
        final DeadJobDeleter deleter = new DeadJobDeleter(
                200,
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, monitorBtih ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, resourceBtih ) );
        assertEquals(0,  monitorBtih.getMethodCallCount(methodIsDead), "Should notta made any calls on monitor yet.");
        assertEquals(0,  resourceBtih.getTotalCallCount(), "Should notta tried to delete anything yet.");

        TestUtil.assertEventually(1, () -> {
            assertEquals(2, monitorBtih.getMethodCallCount(methodIsDead), "Shoulda made a call for each job as to whether or not it's dead.");
            assertEquals(3, resourceBtih.getTotalCallCount(), "Shoulda canceled both jobs and called to cleanup.");
        });
        final MethodInvokeData mid0 = resourceBtih.getMethodInvokeData().get( 0 );
        final MethodInvokeData mid1 = resourceBtih.getMethodInvokeData().get( 1 );
        final MethodInvokeData mid2 = resourceBtih.getMethodInvokeData().get( 2 );
        TestUtil.assertEventually(1, () -> {
                    assertEquals("cleanUpCompletedJobsAndJobChunks", mid0.getMethod().getName(), "Shoulda made one call to cleanup.");
                    assertEquals("cancelJob", mid1.getMethod().getName(), "Shoulda made two calls to cancel job1 and job2.");
                    assertEquals("cancelJob", mid2.getMethod().getName(), "Shoulda made two calls to cancel job1 and job2.");
        });
        deleter.shutdown();
    }
    
    
    @Test
    public void testCancelNotCalledOnReplicatedJobs()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );
        
        final Job job2 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() ).setCreatedAt( new Date() )
                .setDeadJobCleanupAllowed( false );
        dbSupport.getServiceManager().getService( JobService.class ).create( job2 );
        
        final BasicTestsInvocationHandler monitorBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        boolean.class, 
                        new InvocationHandler()
        {
            public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
            {
                return Boolean.valueOf( job2.getId().equals( args[ 0 ] ) );
            }
        }, null ) );
        
        final Method methodIsDead = 
                ReflectUtil.getMethod( DeadJobMonitor.class, "isDead" );
        final BasicTestsInvocationHandler resourceBtih = new BasicTestsInvocationHandler(
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        return null;
                    }
                } );
        final DeadJobDeleter deleter = new DeadJobDeleter(
                200, 
                dbSupport.getServiceManager(), 
                InterfaceProxyFactory.getProxy( DeadJobMonitor.class, monitorBtih ),
                InterfaceProxyFactory.getProxy( DataPlannerResource.class, resourceBtih ) );
        TestUtil.sleep( 300 );
        assertEquals(1,  monitorBtih.getMethodCallCount(methodIsDead), "Shoulda made a call for job as to whether or not it's dead.");
        assertEquals(1,  resourceBtih.getTotalCallCount(), "Shoulda made one call to cleanup.");

        deleter.shutdown();
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
