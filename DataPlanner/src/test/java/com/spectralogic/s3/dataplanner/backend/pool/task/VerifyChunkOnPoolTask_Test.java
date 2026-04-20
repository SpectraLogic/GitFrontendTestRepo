/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.testfrmwrk.PoolTaskBuilder;
import com.spectralogic.util.db.query.Require;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.PoolLockSupportImpl;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.testfrmwrk.MockPoolPersistence;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class VerifyChunkOnPoolTask_Test
{
    @Test
    public void testPrepareForStartWhenJobChunkNoLongerExistsResultsInException()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );
        mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
            chunk.setReadFromPoolId( pool.getId() );
            mockDaoDriver.updateBean( chunk, ReadFromObservable.READ_FROM_POOL_ID );
        }
        
        final VerifyChunkOnPoolTask task = 
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                job.getPriority(),
                pool.getId(),
                PersistenceType.POOL,
                new ArrayList<>(chunks))
                );
        
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.nothing() );
        task.prepareForExecutionIfPossible();
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda invalidated task."
                 );
    }
    
    @Test
    public void testPrepareForStartWhenPoolLockedResultsInNotPreparedToExecute()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );
        mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
            chunk.setReadFromPoolId( pool.getId() );
            mockDaoDriver.updateBean( chunk, ReadFromObservable.READ_FROM_POOL_ID );
        }
        
        final PoolLockSupport< PoolTask > poolLockSupport = new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ), 
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
        poolLockSupport.acquireExclusiveLock(
                pool.getId(), InterfaceProxyFactory.getProxy( PoolTask.class, null ) );
        final VerifyChunkOnPoolTask task =
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .withLockSupport(poolLockSupport)
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                                job.getPriority(),
                                pool.getId(),
                                PersistenceType.POOL,
                                new ArrayList<>(chunks))
                        );
        
        task.prepareForExecutionIfPossible();
        assertEquals(
                BlobStoreTaskState.READY,
                task.getState(),
                "Shoulda said task isn't ready to execute."
                 );
    }
    
    @Test
    public void testRunWhenJobChunkNoLongerExistsResultsInRetryRequired()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );
        mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
            chunk.setReadFromPoolId( pool.getId() );
            mockDaoDriver.updateBean( chunk, ReadFromObservable.READ_FROM_POOL_ID );
        }
        
        final VerifyChunkOnPoolTask task = 
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                job.getPriority(),
                pool.getId(),
                PersistenceType.POOL,
                new ArrayList<>(chunks))
                );
        
        task.prepareForExecutionIfPossible();
        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.nothing() );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda invalidated task."
                 );
    }
    
    @Test
    public void testRunWhenJobChunkNoLongerContainsAnyJobEntriesResultsInTaskCompletion()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Pool pool = mockDaoDriver.createPool( PoolState.NORMAL );
        mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry entry = mockDaoDriver.createJobWithEntry(blob);
        mockDaoDriver.updateBean(entry.setReadFromPoolId(pool.getId()), ReadFromObservable.READ_FROM_POOL_ID );
        
        final VerifyChunkOnPoolTask task = 
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                job.getPriority(),
                pool.getId(),
                PersistenceType.POOL,
                List.of(entry))
                );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda invalidated task."
                );
    }
    
    @Test
    public void testRunWithoutFailuresWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.updateAllBeans( 
                b1.setChecksumType( ChecksumType.CRC_32 ).setChecksum( "RWzXRg==" ), 
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        mockDaoDriver.attainAndUpdate( Blob.class, b1 );
        mockDaoDriver.attainAndUpdate( Blob.class, b2 );
        mockDaoDriver.attainAndUpdate( Blob.class, b3 );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, null );
        final Pool pool = pp.getPool();
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b3.getId() );
        pp.create( b1, b2 );
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            chunk.setReadFromPoolId( pool.getId() );
            mockDaoDriver.updateBean( chunk, ReadFromObservable.READ_FROM_POOL_ID );
        }
        
        final VerifyChunkOnPoolTask task = 
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                job.getPriority(),
                pool.getId(),
                PersistenceType.POOL,
                new ArrayList<>(chunks))
                );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda completed task."
                 );
        assertEquals(
                3,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Shoulda retained blobs that weren't verified as well as those that were successfully."
                );
        pp.shutdown();
    }
    
    @Test
    public void testRunWithFailuresRecordsBlobLoss()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.updateAllBeans( 
                b1.setChecksumType( ChecksumType.CRC_32 ).setChecksum( "RWzXRg==" ), 
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        mockDaoDriver.attainAndUpdate( Blob.class, b1 );
        mockDaoDriver.attainAndUpdate( Blob.class, b2 );
        mockDaoDriver.attainAndUpdate( Blob.class, b3 );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, null );
        final Pool pool = pp.getPool();
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b3.getId() );
        pp.create( b1, b3 );
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            chunk.setReadFromPoolId( pool.getId() );
            mockDaoDriver.updateBean( chunk, ReadFromObservable.READ_FROM_POOL_ID );
        }
        
        final VerifyChunkOnPoolTask task = 
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                job.getPriority(),
                pool.getId(),
                PersistenceType.POOL,
                new ArrayList<>(chunks))
                );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda completed task."
                 );
        assertEquals(
                3,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Shoulda retained blobs that weren't verified as well as those that failed verification."
                );
        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( SuspectBlobPool.class ).getCount(),
                "Shoulda recorded suspect blob."
                );
        pp.shutdown();
    }
    
    @Test
    public void testPerformVerifyWithFailuresThrows()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.updateAllBeans( 
                b1.setChecksumType( ChecksumType.CRC_32 ).setChecksum( "RWzXRg==" ), 
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        mockDaoDriver.attainAndUpdate( Blob.class, b1 );
        mockDaoDriver.attainAndUpdate( Blob.class, b2 );
        mockDaoDriver.attainAndUpdate( Blob.class, b3 );
        
        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, null );
        final Pool pool = pp.getPool();
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b2.getId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), b3.getId() );
        pp.create( b1, b3 );
        
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            chunk.setReadFromPoolId( pool.getId() );
            mockDaoDriver.updateBean( chunk, ReadFromObservable.READ_FROM_POOL_ID );
        }
        
        final VerifyChunkOnPoolTask task = 
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                job.getPriority(),
                pool.getId(),
                PersistenceType.POOL,
                new ArrayList<>(chunks))
                );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                task.runAsNestedTaskInsideAnotherTask( pool.getId() );
            }
        } );

        assertEquals(
                3,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Shoulda retained blobs that weren't verified as well as those that failed verification."
                 );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( SuspectBlobPool.class ).getCount(),
                "Should notta recorded suspect blob."
                );
        pp.shutdown();
    }
    
    @Test
    public void testRunAsNestedTaskInsideAnotherTaskDoesNotMarkChunkCompleted()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateAllBeans(
                b1.setChecksumType( ChecksumType.CRC_32 ).setChecksum( "RWzXRg==" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        mockDaoDriver.attainAndUpdate( Blob.class, b1 );

        final MockPoolPersistence pp = new MockPoolPersistence( dbSupport, null );
        final Pool pool = pp.getPool();
        mockDaoDriver.putBlobOnPool( pool.getId(), b1.getId() );
        pp.create( b1 );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry entry = mockDaoDriver.createJobEntry( job.getId(), b1 );
        entry.setReadFromPoolId( pool.getId() ).setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS );;
        mockDaoDriver.updateBean( entry, ReadFromObservable.READ_FROM_POOL_ID,JobEntry.BLOB_STORE_STATE );

        final VerifyChunkOnPoolTask task =
                new PoolTaskBuilder(dbSupport.getServiceManager())
                        .buildVerifyChunkOnPoolTask(new ReadDirective(
                                job.getPriority(),
                                pool.getId(),
                                PersistenceType.POOL,
                                CollectionFactory.toList(entry))
                        );

        task.runAsNestedTaskInsideAnotherTask( pool.getId() );

        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Task should have completed successfully."
                );


        pp.shutdown();
    }
    
    
    @SuppressWarnings( "unchecked" )
    private PoolLockSupport< PoolTask > getPoolLockSupport()
    {
        return InterfaceProxyFactory.getProxy( PoolLockSupport.class, null );
    }

    private static DatabaseSupport dbSupport;
    @BeforeAll
    public static void setUpDB() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void setUp() {
        dbSupport.reset();
    }
}
