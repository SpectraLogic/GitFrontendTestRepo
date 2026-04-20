/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.target.task.MockDs3ConnectionFactory;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeadJobMonitorImpl_Test 
{
     @Test
    public void testConstructorNullRetrieverManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test()
            {
                new DeadJobMonitorImpl(
                        0,
                        1,
                        null,
                        new MockDs3ConnectionFactory() );
            }
        } );
    }
    

     @Test
    public void testConstructorNullDs3ConnectionFactoryNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DeadJobMonitorImpl(
                        0,
                        1,
                        dbSupport.getServiceManager(),
                        null );
            }
        } );
    }
    

     @Test
    public void testHappyConstruction()
    {
        new DeadJobMonitorImpl(
                0,
                1,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory() );
    }
    

     @Test
    public void testIsDeadReturnsFalseForNonExistantJobsSinceTheyArentDeadTheyAreDeleted()
    {
        final DeadJobMonitor monitor = new DeadJobMonitorImpl(
                0,
                200,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory() );
        assertEquals(false, monitor.isDead( UUID.randomUUID() ), "Shoulda reported the new job as alive.");
    }
    

     @Test
    public void testIsDeadInitiallyReturnsForNewJobs()
    {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );
        
        final Job job = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT )
                .setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job );
        
        final DeadJobMonitor monitor = new DeadJobMonitorImpl(
                0,
                200,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory() );
        assertEquals(false, monitor.isDead( job.getId() ), "Shoulda reported the new job as alive.");
    }
    

     @Test
    public void testDeadJobsDetectedCorrectlyWhenActivityIsNotedWithJobId()
    {
        

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );
        
        final Job job1 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job1 );
        
        final Job job2 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job2 );
        
        final Job job3 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job3 );
        
        final DeadJobMonitor monitor = new DeadJobMonitorImpl(
                0,
                200,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory() );
        monitor.activityOccurred( job1.getId(), UUID.randomUUID() );
        
        final Job job4 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job4 );
        
        TestUtil.sleep( 500 );

        assertEquals(true, monitor.isDead( job1.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(true, monitor.isDead( job2.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(true, monitor.isDead( job3.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(false, monitor.isDead( job4.getId() ), "Monitor should notta been aware of this job long enough to know it's dead yet.");

        monitor.activityOccurred( job1.getId(), UUID.randomUUID() );

        assertEquals(false, monitor.isDead( job1.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(true, monitor.isDead( job2.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(true, monitor.isDead( job3.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(false, monitor.isDead( job4.getId() ), "Monitor should notta been aware of this job long enough to know it's dead yet.");

        monitor.activityOccurred( job3.getId(), UUID.randomUUID() );

        assertEquals(false, monitor.isDead( job1.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(true, monitor.isDead( job2.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(false, monitor.isDead( job3.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(false, monitor.isDead( job4.getId() ), "Monitor should notta been aware of this job long enough to know it's dead yet.");
    }
    

     @Test
    public void testDeadJobsDetectedCorrectlyWhenActivityIsNotedWithoutJobId()
    {
        


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );
        
        new MockCacheFilesystemDriver( dbSupport, 1, 1 ).shutdown();

        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1", 22 );
        final Blob b1 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2", 22 );
        final Blob b2 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", 22 );
        final Blob b3 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, o3.getId() );
        final S3Object o4 = mockDaoDriver.createObject( bucket.getId(), "o4", 22 );
        final Blob b4 = dbSupport.getServiceManager().getRetriever( Blob.class ).attain( 
                Blob.OBJECT_ID, o4.getId() );
        
        final Job job1 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job1 );
        mockDaoDriver.createJobEntries(job1.getId(), b1 );
        
        final Job job2 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.GET ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job2 );
        mockDaoDriver.createJobEntries(job2.getId(), b1, b2 );
        
        final Job job3 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job3 );
        mockDaoDriver.createJobEntries(job3.getId(), CollectionFactory.toSet( b3, b4 ) );
        
        final DeadJobMonitor monitor = new DeadJobMonitorImpl(
                0,
                200,
                dbSupport.getServiceManager(),
                new MockDs3ConnectionFactory() );
        monitor.activityOccurred( null, b1.getId() );
        
        final Job job4 = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.GET ).setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job4 );
        mockDaoDriver.createJobEntries(job4.getId(), CollectionFactory.toSet( b4 ) );
        
        TestUtil.sleep( 250 );

        assertEquals(true, monitor.isDead( job1.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(true, monitor.isDead( job2.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(true, monitor.isDead( job3.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(false, monitor.isDead( job4.getId() ), "Monitor should notta been aware of this job long enough to know it's dead yet.");

        monitor.activityOccurred( null, b1.getId() );

        assertEquals(false, monitor.isDead( job1.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(false, monitor.isDead( job2.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(true, monitor.isDead( job3.getId() ), "Non-recent activity shoulda made job dead.");
        assertEquals(false, monitor.isDead( job4.getId() ), "Monitor should notta been aware of this job long enough to know it's dead yet.");

        monitor.activityOccurred( null, b3.getId() );

        assertEquals(false, monitor.isDead( job1.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(false, monitor.isDead( job2.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(false, monitor.isDead( job3.getId() ), "Recent activity shoulda kept job alive.");
        assertEquals(false, monitor.isDead( job4.getId() ), "Monitor should notta been aware of this job long enough to know it's dead yet.");
    }
    
    
     @Test
    public void testActivityCalledThrottlesKeepAlivesToDs3Targets()
   {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "b1" );
        
        final Job job = BeanFactory.newBean( Job.class )
                .setRequestType( JobRequestType.PUT )
                .setBucketId( bucket.getId() )
                .setUserId( user.getId() );
        dbSupport.getServiceManager().getService( JobService.class ).create( job );
        
        final MockDs3ConnectionFactory connection = new MockDs3ConnectionFactory();
        connection.setKeepJobAliveException( null );
        
        mockDaoDriver.createDs3Target( "t1" );
        mockDaoDriver.createDs3Target( "t2" );
        
        final DeadJobMonitor monitor = new DeadJobMonitorImpl(
                0,
                20000,
                dbSupport.getServiceManager(),
                connection,
                10 );

        assertEquals(0,  connection.getBtih().getTotalCallCount(), "Should notta made any calls on connection yet.");
        monitor.activityOccurred( job.getId(), null );
        
        int i = 1000;
        while ( --i > 0 && 4 != connection.getBtih().getTotalCallCount() )
        {
            TestUtil.sleep( 10 );
        }

        assertEquals(2,  connection.getConnectCallCount(), "Shoulda connected for each target.");
        assertEquals(4,  connection.getBtih().getTotalCallCount(), "Shoulda made call on connection to keep job alive.");

        monitor.activityOccurred( job.getId(), null );
        TestUtil.sleep( 10 );
        assertEquals(2,  connection.getConnectCallCount(), "Shoulda throttled call on connection to keep job alive.");
        assertEquals(4,  connection.getBtih().getTotalCallCount(), "Shoulda throttled call on connection to keep job alive.");

        TestUtil.sleep( 250 );
        
        connection.setKeepJobAliveException( new RuntimeException( "I like to throw exceptions." ) );
        monitor.activityOccurred( job.getId(), null );
        
        i = 1000;
        while ( --i > 0 && 8 > connection.getBtih().getTotalCallCount() )
        {
            TestUtil.sleep( 10 );
        }

        assertEquals(4,  connection.getConnectCallCount(), "Shoulda connected for each target.");

        assertEquals(8,  connection.getBtih().getTotalCallCount(), "Shoulda made call on connection to keep job alive.");

        monitor.activityOccurred( job.getId(), null );
        TestUtil.sleep( 10 );
        assertEquals(4,  connection.getConnectCallCount(), "Shoulda throttled call on connection to keep job alive.");
        assertEquals(8,  connection.getBtih().getTotalCallCount(), "Shoulda throttled call on connection to keep job alive.");

        TestUtil.sleep( 250 );
        
        connection.setConnectException( new RuntimeException( "You can't see me." ) );
        monitor.activityOccurred( job.getId(), null );
        
        i = 1000;
        while ( --i > 0 && 6 != connection.getConnectCallCount() )
        {
            TestUtil.sleep( 10 );
        }
        assertEquals(6,  connection.getConnectCallCount(), "Shoulda connected for each target.");
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
