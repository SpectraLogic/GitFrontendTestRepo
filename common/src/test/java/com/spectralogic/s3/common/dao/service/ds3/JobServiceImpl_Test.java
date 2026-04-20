/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class JobServiceImpl_Test 
{
    @Test
    public void testMigrateNotInsideTransactionNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID objectId =
                mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME, -1 ).getId();
        mockDaoDriver.createJobWithEntries( JobRequestType.PUT,
                mockDaoDriver.createBlobs( objectId, 10, 12L ) );
        final Job job1 = dbSupport.getServiceManager().getRetriever( Job.class ).attain( Require.nothing() );
        final Job job2 = 
                mockDaoDriver.createJob( job1.getBucketId(), job1.getUserId(), job1.getRequestType() );
        
        final JobService jobService =
                dbSupport.getServiceManager().getService( JobService.class );
        TestUtil.assertThrows( null, IllegalStateException.class,
                () -> jobService.migrate( job2.getId(), job1.getId() ) );
    }
    
    
    @Test
    public void testMigrateInsideTransactionDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID objectId =
                mockDaoDriver.createObject( null, MockDaoDriver.DEFAULT_OBJECT_NAME, -1 ).getId();
        mockDaoDriver.createJobWithEntries( JobRequestType.PUT,
                mockDaoDriver.createBlobs( objectId, 10, 12L ) );
        final Job job1 = dbSupport.getServiceManager().getRetriever( Job.class ).attain( Require.nothing() );
        final Job job2 = 
                mockDaoDriver.createJob( job1.getBucketId(), job1.getUserId(), job1.getRequestType() );
        
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        final JobService jobService = transaction.getService( JobService.class );
        jobService.migrate( job2.getId(), job1.getId() );
        transaction.commitTransaction();
        
        dbSupport.getServiceManager().getRetriever( Job.class ).attain( Require.nothing() );
        assertEquals(10,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(), "All job entries shoulda been moved to new job.");
        assertEquals(10,  dbSupport.getServiceManager().getRetriever(JobEntry.class).getCount(
                Require.beanPropertyEquals(JobEntry.JOB_ID, job2.getId())), "All job entries shoulda been moved to new job.");
    }
    
    
    @Test
    public void testCloseOldAggregatingJobsOutsideTransactionNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        mockDaoDriver.updateBean( 
                job1.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 6 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        mockDaoDriver.updateBean( 
                job2.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 6 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 1 ] );
        mockDaoDriver.updateBean( 
                job3.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 4 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        final Job job4 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 1 ] );
        mockDaoDriver.updateBean( 
                job4.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 4 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        assertEquals(0,  service.getCount(Job.AGGREGATING, Boolean.FALSE), "All jobs shoulda been aggregating initially.");

        TestUtil.assertThrows( null, IllegalStateException.class, () -> service.closeOldAggregatingJobs( 5 ) );
    }
    
    
    @Test
    public void testCloseOldAggregatingJobsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        final JobService service = transaction.getService( JobService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        mockDaoDriver.updateBean( 
                job1.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 6 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        mockDaoDriver.updateBean( 
                job2.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 6 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 1 ] );
        mockDaoDriver.updateBean( 
                job3.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 4 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        final Job job4 = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 1 ] );
        mockDaoDriver.updateBean( 
                job4.setAggregating( true )
                    .setCreatedAt( new Date( System.currentTimeMillis() - 4 * 60000L ) ),
                Job.AGGREGATING, JobObservable.CREATED_AT );
        assertEquals(0,  service.getCount(Job.AGGREGATING, Boolean.FALSE), "All jobs shoulda been aggregating initially.");

        assertEquals(2,  service.closeOldAggregatingJobs(5).size(), "Shoulda cleared job aggregation for eligible jobs.");
        assertEquals(2,  service.getCount(Job.AGGREGATING, Boolean.FALSE), "Shoulda cleared job aggregation for eligible jobs.");

        assertEquals(2,  service.closeOldAggregatingJobs(3).size(), "Shoulda cleared job aggregation for eligible jobs.");
        assertEquals(4,  service.getCount(Job.AGGREGATING, Boolean.FALSE), "Shoulda cleared job aggregation for eligible jobs.");
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testCloseAggregatingJobIndividuallyOutsideTransactionNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean( job1.setAggregating( true ), Job.AGGREGATING );
        TestUtil.assertThrows( null, IllegalStateException.class, () -> service.closeAggregatingJob( job1.getId() ) );
    }
    
    
    @Test
    public void testCloseAggregatingJobsIndividuallyDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        final JobService service = transaction.getService( JobService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean( job1.setAggregating( true ), Job.AGGREGATING );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.updateBean( job2.setAggregating( true ), Job.AGGREGATING );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job4 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        assertEquals(2,  service.getCount(Job.AGGREGATING, Boolean.TRUE), "2 jobs shoulda been aggregating initially.");
        service.closeAggregatingJob( job1.getId() );
        assertEquals(1,  service.getCount(Job.AGGREGATING, Boolean.TRUE), "1 jobs should still be aggregating.");
        service.closeAggregatingJob( job2.getId() );
        assertEquals(0,  service.getCount(Job.AGGREGATING, Boolean.TRUE), "No jobs should still be aggregating.");

        TestUtil.assertThrows( null, DaoException.class, () -> service.closeAggregatingJob( job3.getId() ) );
        TestUtil.assertThrows( null, DaoException.class, () -> service.closeAggregatingJob( job4.getId() ) );
        
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testCleanUpCompletedJobsAndJobChunksDoesNothingWhenNothingToCleanUp()
            throws InterruptedException
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
            
        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( b2, b3 ) );
        mockDaoDriver.createJobEntry( job1.getId(),  b1 );
        mockDaoDriver.createJobEntry( job2.getId(),  b1 );
        mockDaoDriver.createJobEntry( job3.getId(),  b1 );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        mockDaoDriver.updateBean( 
                storageDomain.setAutoEjectUponJobCompletion( true ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( 
                tape.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ), 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        assertEquals(5,  dbSupport.getServiceManager().getRetriever(Job.class).getCount(), "Should notta whacked any jobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Should notta whacked any jobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Should notta whacked any jobs.");

        final BasicTestsInvocationHandler jpBtih = new BasicTestsInvocationHandler( null );
        final BasicTestsInvocationHandler teBtih = getTapeEjectorBtih( dbSupport );
        final CountDownLatch latch = new CountDownLatch( 10 );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        final Runnable r = () -> {
            service.cleanUpCompletedJobsAndJobChunks(
                    getJobProgressManager( jpBtih ),
                    getTapeEjector( teBtih ),
                    new Object() );
            latch.countDown();
        };
        for ( int i = 0; i < 10; ++i )
        {
            SystemWorkPool.getInstance().submit( r );
        }
        assertTrue(latch.await( 10, TimeUnit.SECONDS ), "All instances shoulda completed successfully.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(Job.class).getCount(), "Should notta whacked any jobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Should notta whacked any jobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Should notta whacked any jobs.");
        assertEquals(0,  teBtih.getTotalCallCount(), "Should notta auto-ejected tapes.");
        assertEquals(0,  jpBtih.getTotalCallCount(), "Should notta flushed job progress.");
    }
    
    
    @Test
    public void testCleanUpCompletedJobsAndJobChunksDoesSoEvenUnderInteleavedCallsWhenAutoEjectConfigured()
            throws InterruptedException
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final List< UUID > canceledJobIds = new ArrayList<>();
        mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 31 * 24L * 3600 * 1000 ) );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 29 * 24L * 3600 * 1000 ) ).getId() );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 10 * 24L * 3600 * 1000 ) ).getId() );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() ) ).getId() );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() + 31 * 24L * 3600 * 1000 ) ).getId() );
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Should notta whacked any canceled jobs yet.");

        final List< UUID > completedJobIds = new ArrayList<>();
        mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 31 * 24L * 3600 * 1000 ) );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 29 * 24L * 3600 * 1000 ) ).getId() );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 10 * 24L * 3600 * 1000 ) ).getId() );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() ) ).getId() );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() + 31 * 24L * 3600 * 1000 ) ).getId() );
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Should notta whacked any completed jobs yet.");

        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        completedJobIds.add( 
                mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId() );
        completedJobIds.add( 
                mockDaoDriver.createJob( null, null, JobRequestType.GET ).getId() );
        final JobEntry chunk1a =
                mockDaoDriver.createJobWithEntry( JobRequestType.GET, b1 );
        final JobEntry chunk1b =
                mockDaoDriver.createJobEntry( chunk1a.getJobId(), b2 );
        final JobEntry chunk2a =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b2 );
        final JobEntry chunk2b =
                mockDaoDriver.createJobEntry( chunk2a.getJobId(), b3 );
        final JobEntry chunk3 = mockDaoDriver.createJobEntry( job1.getId(), b1 );
        final JobEntry chunk4 = mockDaoDriver.createJobEntry( job2.getId(), b1 );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        mockDaoDriver.updateBean( 
                storageDomain.setAutoEjectUponJobCompletion( true ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( 
                tape.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ), 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        completedJobIds.add( job3.getId() );
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Should notta whacked any canceled jobs yet.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Should notta whacked any completed jobs yet.");

        final BasicTestsInvocationHandler jpBtih = new BasicTestsInvocationHandler( null );
        final BasicTestsInvocationHandler teBtih = getTapeEjectorBtih( dbSupport );
        final CountDownLatch latch = new CountDownLatch( 10 );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        final Runnable r = () -> {
            service.cleanUpCompletedJobsAndJobChunks(
                    getJobProgressManager( jpBtih ),
                    getTapeEjector( teBtih ),
                    new Object() );
            latch.countDown();
        };
        for ( int i = 0; i < 10; ++i )
        {
            SystemWorkPool.getInstance().submit( r );
        }
        assertTrue(latch.await( 10, TimeUnit.SECONDS ), "All instances shoulda completed successfully.");
        assertTrue(0 < teBtih.getTotalCallCount(), "Shoulda auto-ejected tapes that needed to be auto-ejected.");
        assertTrue(0 < jpBtih.getTotalCallCount(), "Shoulda flushed job progress when creating any completed jobs.");

        final Set< UUID > remainingChunks = BeanUtils.toMap(
                dbSupport.getServiceManager().getRetriever( JobEntry.class ).retrieveAll().toSet() ).keySet();
        final Object expected1 = CollectionFactory.toSet(
                chunk1a.getId(), chunk1b.getId(), chunk2a.getId(), chunk2b.getId(), chunk3.getId(), chunk4.getId() );
        assertEquals(expected1, remainingChunks, "Shoulda kept only those chunks that still have entries in them.");

        final Set< UUID > remainingJobs = BeanUtils.toMap( service.retrieveAll().toSet() ).keySet();
        final Object expected = CollectionFactory.toSet( 
                job1.getId(), job2.getId(), chunk1a.getJobId(), chunk2a.getJobId() );
        assertEquals(expected, remainingJobs, "Shoulda kept only those jobs that still have chunks with entries in them.");
        assertEquals(new HashSet<UUID>( completedJobIds ), BeanUtils.toMap( dbSupport.getServiceManager().getRetriever(
                        CompletedJob.class ).retrieveAll().toSet() ).keySet(), "Shoulda created completed job entries for jobs that were completed out.");
        assertEquals(new HashSet<UUID>( canceledJobIds ), BeanUtils.toMap( dbSupport.getServiceManager().getRetriever(
                        CanceledJob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked canceled jobs that were too old to retain.");
    }
    
    
    @Test
    public void testCleanUpCompletedJobsAndJobChunksDoesSoEvenUnderInteleavedCallsWhenAutoEjectNotConfigured()
            throws InterruptedException
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final List< UUID > canceledJobIds = new ArrayList<>();
        mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 31 * 24L * 3600 * 1000 ) );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 29 * 24L * 3600 * 1000 ) ).getId() );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 10 * 24L * 3600 * 1000 ) ).getId() );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() ) ).getId() );
        canceledJobIds.add( mockDaoDriver.createCanceledJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() + 31 * 24L * 3600 * 1000 ) ).getId() );
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Should notta whacked any canceled jobs yet.");

        final List< UUID > completedJobIds = new ArrayList<>();
        mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 31 * 24L * 3600 * 1000 ) );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 29 * 24L * 3600 * 1000 ) ).getId() );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() - 10 * 24L * 3600 * 1000 ) ).getId() );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() ) ).getId() );
        completedJobIds.add( mockDaoDriver.createCompletedJob(
                null,
                null, 
                null,
                new Date( System.currentTimeMillis() + 31 * 24L * 3600 * 1000 ) ).getId() );
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Should notta whacked any completed jobs yet.");

        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 100 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        completedJobIds.add( 
                mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId() );
        completedJobIds.add( 
                mockDaoDriver.createJob( null, null, JobRequestType.GET ).getId() );
        final JobEntry chunk1a =
                mockDaoDriver.createJobWithEntry( JobRequestType.GET, b1 );
        final JobEntry chunk1b =
                mockDaoDriver.createJobEntry( chunk1a.getJobId(), b2 );
        final JobEntry chunk2a =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b2 );
        final JobEntry chunk2b =
                mockDaoDriver.createJobEntry( chunk1b.getJobId(), b3 );
        final JobEntry chunk3 = mockDaoDriver.createJobEntry( job1.getId(), b1 );
        final JobEntry chunk4 = mockDaoDriver.createJobEntry( job2.getId(), b1 );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        mockDaoDriver.updateBean( 
                storageDomain.setAutoEjectUponJobCompletion( false ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.updateBean( 
                tape.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ), 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        completedJobIds.add( job3.getId() );
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Should notta whacked any canceled jobs yet.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Should notta whacked any completed jobs yet.");

        final BasicTestsInvocationHandler jpBtih = new BasicTestsInvocationHandler( null );
        final BasicTestsInvocationHandler teBtih = getTapeEjectorBtih( dbSupport );
        final CountDownLatch latch = new CountDownLatch( 10 );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        final Runnable r = () -> {
            service.cleanUpCompletedJobsAndJobChunks(
                    getJobProgressManager( jpBtih ),
                    getTapeEjector( teBtih ),
                    new Object() );
            latch.countDown();
        };
        for ( int i = 0; i < 10; ++i )
        {
            SystemWorkPool.getInstance().submit( r );
        }
        assertTrue(latch.await( 10, TimeUnit.SECONDS ), "All instances shoulda completed successfully.");
        assertFalse(0 < teBtih.getTotalCallCount(), "Should notta auto-ejected tapes that needed to be auto-ejected.");
        assertTrue(0 < jpBtih.getTotalCallCount(), "Shoulda flushed job progress when creating any completed jobs.");

        final Set< UUID > remainingChunks = BeanUtils.toMap(
                dbSupport.getServiceManager().getRetriever( JobEntry.class ).retrieveAll().toSet() ).keySet();
        final Object expected1 = CollectionFactory.toSet(
                chunk1a.getId(), chunk1b.getId(), chunk2a.getId(), chunk2b.getId(), chunk3.getId(), chunk4.getId() );
        assertEquals(expected1, remainingChunks, "Shoulda kept only those chunks that still have entries in them.");

        final Set< UUID > remainingJobs = BeanUtils.toMap( service.retrieveAll().toSet() ).keySet();
        final Object expected = CollectionFactory.toSet( 
                job1.getId(), job2.getId(), chunk1a.getJobId(), chunk2a.getJobId() );
        assertEquals(expected, remainingJobs, "Shoulda kept only those jobs that still have chunks with entries in them.");
        assertEquals(new HashSet<UUID>( completedJobIds ), BeanUtils.toMap( dbSupport.getServiceManager().getRetriever(
                        CompletedJob.class ).retrieveAll().toSet() ).keySet(), "Shoulda created completed job entries for jobs that were completed out.");
        assertEquals(new HashSet<UUID>( canceledJobIds ), BeanUtils.toMap( dbSupport.getServiceManager().getRetriever(
                        CanceledJob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked canceled jobs that were too old to retain.");
    }
    
    
    @Test
    public void testCleanUpCompletedJobsAndJobChunksDoesSoWhenTruncation()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final List< UUID > canceledJobIds = new ArrayList<>();
        final List< UUID > completedJobIds = new ArrayList<>();
            
        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.updateBean( job1.setTruncated( true ), JobObservable.TRUNCATED );
        canceledJobIds.add( job1.getId() );
        mockDaoDriver.updateBean(
                job2.setTruncated( true ).setTruncatedDueToTimeout( true ),
                JobObservable.TRUNCATED, Job.TRUNCATED_DUE_TO_TIMEOUT );
        canceledJobIds.add( job2.getId() );
        completedJobIds.add( job3.getId() );
        
        dbSupport.getServiceManager().getService( JobService.class ).cleanUpCompletedJobsAndJobChunks(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new Object() );

        final Object expected = completedJobIds.iterator().next();
        assertEquals(expected, mockDaoDriver.attainOneAndOnly( CompletedJob.class ).getId(), "Shoulda completed out all jobs.");
        final Object actual = dbSupport.getServiceManager().getRetriever( CanceledJob.class ).getCount();
        assertEquals(canceledJobIds.size(), actual, "Shoulda completed out all jobs.");
        assertTrue(mockDaoDriver.attain( CanceledJob.class, job1.getId() ).isTruncated(), "Shoulda noted correct cancellation info.");
        assertTrue(mockDaoDriver.attain( CanceledJob.class, job2.getId() ).isTruncated(), "Shoulda noted correct cancellation info.");
        assertFalse(mockDaoDriver.attain( CanceledJob.class, job1.getId() ).isCanceledDueToTimeout(), "Shoulda noted correct cancellation info.");
        assertTrue(mockDaoDriver.attain( CanceledJob.class, job2.getId() ).isCanceledDueToTimeout(), "Shoulda noted correct cancellation info.");
    }
    
    
    @Test
    public void testCleanUpCompletedJobsAndJobChunksDueToTooManyCompletedJobsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
            
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        
        Set< CompletedJob > jobs = new HashSet<>();
        for ( int i = 0; i < JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE + 10; ++i )
        {
            final CompletedJob cj = BeanFactory.newBean( CompletedJob.class );
            BeanCopier.copy( cj, job );
            cj.setId( UUID.randomUUID() );
            cj.setDateCompleted( new Date( System.currentTimeMillis() - ( i * 1000 ) ) );
            jobs.add( cj );
        }
        DataManager transaction = dbSupport.getDataManager().startTransaction();
        try
        {
            transaction.createBeans( jobs );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        service.cleanUpCompletedJobsAndJobChunks(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new Object() );
        assertEquals(JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE + 10 + 1,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Should notta deleted any jobs.");

        jobs = new HashSet<>();
        for ( int i = 0; i < JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE * 1.2; ++i )
        {
            final CompletedJob cj = BeanFactory.newBean( CompletedJob.class );
            BeanCopier.copy( cj, job );
            cj.setId( UUID.randomUUID() );
            cj.setDateCompleted( new Date( System.currentTimeMillis() - ( i * 1000 ) ) );
            jobs.add( cj );
        }
        transaction = dbSupport.getDataManager().startTransaction();
        try
        {
            transaction.createBeans( jobs );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        final Job activeJob = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        service.cleanUpCompletedJobsAndJobChunks(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new Object() );
        assertEquals(JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE,  dbSupport.getServiceManager().getRetriever(CompletedJob.class).getCount(), "Shoulda deleted jobs.");

        dbSupport.getDataManager().createBean( activeJob );
        service.cleanUpCompletedJobsAndJobChunks(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new Object() );
    }
    
    
    @Test
    public void testCleanUpCompletedJobsAndJobChunksDueToTooManyCanceledJobsDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
            
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        
        List< CanceledJob > jobs = new ArrayList<>();
        for ( int i = 0; i < JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE + 10; ++i )
        {
            final CanceledJob cj = BeanFactory.newBean( CanceledJob.class );
            BeanCopier.copy( cj, job );
            cj.setId( UUID.randomUUID() );
            cj.setDateCanceled( new Date( System.currentTimeMillis() - ( i * 1000 ) ) );
            jobs.add( cj );
        }
        DataManager transaction = dbSupport.getDataManager().startTransaction();
        try
        {
            transaction.createBeans( new HashSet<>( jobs ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        final CanceledJob first = jobs.get( 0 );
        final CanceledJob last = jobs.get( jobs.size() - 1 );
        
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        service.cleanUpCompletedJobsAndJobChunks(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new Object() );
        assertEquals(JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE + 10,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Should notta deleted any jobs.");

        jobs = new ArrayList<>();
        for ( int i = 0; i < JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE * 1.2; ++i )
        {
            final CanceledJob cj = BeanFactory.newBean( CanceledJob.class );
            BeanCopier.copy( cj, job );
            cj.setId( UUID.randomUUID() );
            cj.setDateCanceled( new Date( System.currentTimeMillis() - ( i * 1000 ) ) );
            jobs.add( cj );
        }
        transaction = dbSupport.getDataManager().startTransaction();
        try
        {
            transaction.createBeans( new HashSet<>( jobs ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        mockDaoDriver.createJob( null, null, JobRequestType.values()[ 0 ] );
        service.cleanUpCompletedJobsAndJobChunks(
                InterfaceProxyFactory.getProxy( JobProgressManager.class, null ),
                InterfaceProxyFactory.getProxy( TapeEjector.class, null ),
                new Object() );
        assertEquals(JobObservable.MAX_COUNT_TO_RETAIN_ONCE_DONE,  dbSupport.getServiceManager().getRetriever(CanceledJob.class).getCount(), "Shoulda deleted jobs.");
        assertNull(mockDaoDriver.retrieve( last ), "Shoulda deleted oldest entry.");
        assertNotNull(
                mockDaoDriver.retrieve( first ),
                "Should notta deleted most recent entry."
                );
    }
    
    
    @Test
    public void testAutoEjectTapesDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final StorageDomain storageDomain1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomain storageDomain4 = mockDaoDriver.createStorageDomain( "sd4" );
        mockDaoDriver.updateBean( 
                storageDomain1.setAutoEjectUponJobCompletion( false ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION );
        mockDaoDriver.updateBean( 
                storageDomain2.setAutoEjectUponJobCompletion( false ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION );
        mockDaoDriver.updateBean( 
                storageDomain2.setVerifyPriorToAutoEject( BlobStoreTaskPriority.values()[ 1 ] ), 
                StorageDomain.VERIFY_PRIOR_TO_AUTO_EJECT );
        mockDaoDriver.updateBean( 
                storageDomain3.setAutoEjectUponJobCompletion( true ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION );
        mockDaoDriver.updateBean( 
                storageDomain4.setAutoEjectUponJobCompletion( true ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION );
        mockDaoDriver.updateBean( 
                storageDomain1.setAutoEjectUponJobCancellation( false ),
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION );
        mockDaoDriver.updateBean( 
                storageDomain2.setAutoEjectUponJobCancellation( true ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION );
        mockDaoDriver.updateBean( 
                storageDomain3.setAutoEjectUponJobCancellation( true ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION );
        mockDaoDriver.updateBean( 
                storageDomain4.setAutoEjectUponJobCancellation( false ), 
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION );
        
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD, 
                dataPolicy1.getId(),
                DataPersistenceRuleType.PERMANENT, 
                storageDomain1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy1.getId(),
                DataPersistenceRuleType.PERMANENT, 
                storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy1.getId(),
                DataPersistenceRuleType.PERMANENT, 
                storageDomain3.getId() );
        
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD, 
                dataPolicy2.getId(),
                DataPersistenceRuleType.PERMANENT, 
                storageDomain3.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy2.getId(),
                DataPersistenceRuleType.PERMANENT, 
                storageDomain4.getId() );
        
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.EJECTED );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain1.getId(), tape0.getPartitionId(), tape0.getType() );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tape0.getPartitionId(), tape0.getType() );
        final StorageDomainMember sdm3 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tape0.getPartitionId(), tape0.getType() );
        final StorageDomainMember sdm4 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain4.getId(), tape0.getPartitionId(), tape0.getType() );
        mockDaoDriver.updateBean( 
                tape0.setStorageDomainMemberId( sdm1.getId() ).setBucketId( null ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( sdm1.getId() ).setBucketId( null ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( sdm1.getId() ).setBucketId( bucket1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape3 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape3.setStorageDomainMemberId( sdm2.getId() ).setBucketId( null ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape4 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape4.setStorageDomainMemberId( sdm2.getId() ).setBucketId( bucket1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape5 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape5.setStorageDomainMemberId( sdm3.getId() ).setBucketId( null ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape6 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape6.setStorageDomainMemberId( sdm3.getId() ).setBucketId( bucket1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape7 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape7.setStorageDomainMemberId( sdm4.getId() ).setBucketId( null ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape8 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape8.setStorageDomainMemberId( sdm4.getId() ).setBucketId( bucket1.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        final Tape tape9 = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape9.setStorageDomainMemberId( sdm4.getId() ).setBucketId( bucket2.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.BUCKET_ID );
        
        final BasicTestsInvocationHandler btih = getTapeEjectorBtih( dbSupport );
        final JobService service = dbSupport.getServiceManager().getService( JobService.class );
        
        btih.reset();
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Tape.class ).setEjectPending( null ),
                Tape.EJECT_PENDING );
        service.autoEjectTapes(
                bucket1.getId(), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION,
                getTapeEjector( btih ) );
        assertTapesMarkedForEjection( dbSupport, btih, tape6 );
        
        btih.reset();
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Tape.class ).setEjectPending( null ),
                Tape.EJECT_PENDING );
        service.autoEjectTapes(
                bucket1.getId(), 
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION,
                getTapeEjector( btih ) );
        assertTapesMarkedForEjection( dbSupport, btih, tape4, tape6 );
        
        btih.reset();
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Tape.class ).setEjectPending( null ),
                Tape.EJECT_PENDING );
        service.autoEjectTapes(
                bucket2.getId(), 
                StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION,
                getTapeEjector( btih ) );
        assertTapesMarkedForEjection( dbSupport, btih, tape5, tape9 );
        
        btih.reset();
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Tape.class ).setEjectPending( null ),
                Tape.EJECT_PENDING );
        service.autoEjectTapes(
                bucket2.getId(), 
                StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION,
                getTapeEjector( btih ) );
        assertTapesMarkedForEjection( dbSupport, btih, tape5 );
    }
    
    
    private void assertTapesMarkedForEjection(
            final DatabaseSupport dbSupport,
            final BasicTestsInvocationHandler btih, 
            final Tape ... tapes )
    {
        assertEquals(tapes.length,  btih.getTotalCallCount(), "Shoulda called eject for each tape being ejected.");
        assertEquals(BeanUtils.toMap( CollectionFactory.toSet( tapes ) ).keySet(), BeanUtils.toMap( dbSupport.getServiceManager().getRetriever( Tape.class ).retrieveAll( 
                        Require.not( Require.beanPropertyEquals( Tape.EJECT_PENDING, null ) ) ).toSet() )
                        .keySet(), "Shoulda called eject for each tape being ejected.");

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        for ( final Tape tape : tapes )
        {
            if ( null == tape.getStorageDomainMemberId() )
            {
                continue;
            }
            final StorageDomain sd = new TapeRM( tape, dbSupport.getServiceManager() )
                    .getStorageDomainMember().getStorageDomain().unwrap();
            final Object expected = sd.getVerifyPriorToAutoEject();
            assertEquals(expected, mockDaoDriver.attain( tape ).getVerifyPending(), "Shoulda configured verify pending per storage domain policy.");
        }
    }
    
    
    private BasicTestsInvocationHandler getTapeEjectorBtih( final DatabaseSupport dbSupport )
    {
        return new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( TapeEjector.class, "ejectTape" ), ( proxy, method, args ) -> {
                    dbSupport.getDataManager().updateBeans(
                            CollectionFactory.toSet( Tape.VERIFY_PENDING ),
                            BeanFactory.newBean( Tape.class ).setVerifyPending(
                                    (BlobStoreTaskPriority)args[ 0 ] ),
                            Require.beanPropertyEquals( Identifiable.ID, args[ 1 ] ) );
                    dbSupport.getDataManager().updateBeans(
                            CollectionFactory.toSet( Tape.EJECT_PENDING ),
                            BeanFactory.newBean( Tape.class ).setEjectPending( new Date() ),
                            Require.beanPropertyEquals( Identifiable.ID, args[ 1 ] ) );
                    return null;
                },
                null ) );
    }
    
    
    private JobProgressManager getJobProgressManager( final BasicTestsInvocationHandler btih )
    {
        return InterfaceProxyFactory.getProxy( JobProgressManager.class, btih );
    }
    
    
    private TapeEjector getTapeEjector( final BasicTestsInvocationHandler btih )
    {
        return InterfaceProxyFactory.getProxy( TapeEjector.class, btih );
    }
}
