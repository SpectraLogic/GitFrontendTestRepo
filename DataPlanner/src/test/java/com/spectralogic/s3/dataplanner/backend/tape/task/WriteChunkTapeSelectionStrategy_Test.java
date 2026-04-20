/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.orm.BlobLocalTargetRM;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.frmwrk.CanAllocatePersistenceTargetSupport;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class WriteChunkTapeSelectionStrategy_Test 
{
    @Test
    public void testCapacityWriteOptimizationTapeSelectedWhichIsAlreadyAssignedToBucketWhenPossible()
    {
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition partition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition.getId(), TapeType.LTO5 ); 
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ), 
                sd.setWriteOptimization( WriteOptimization.CAPACITY ) );
        
        final Tape tape1 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Tape tape2 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "b" )
                .setPartitionId( partition.getId() )
                .setState( TapeState.NORMAL );
        final Tape tape3 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "wp" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setWriteProtected( true );
        dbSupport.getDataManager().createBean( tape1 );
        dbSupport.getDataManager().createBean( tape2 );
        dbSupport.getDataManager().createBean( tape3 );
        
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final AtomicLong currentSize = new AtomicLong( 0 );
        final Object expected4 = tape1.getId();
        assertEquals(expected4, selectTape(strategy,
                        currentSize,
                        pts,
                        new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        assertEquals(100L * 1024 * 1024 * 1024,  currentSize.get(), "Shoulda gotten correct current size of bucket.");
        final Object expected3 = tape1.getId();
        assertEquals(expected3, selectTape(strategy,
                        currentSize,
                        pts,
                        new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        final Object expected2 = tape1.getId();
        assertEquals(expected2, selectTape(strategy,
                        currentSize,
                        pts,
                        new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        final Object expected1 = tape1.getId();
        assertEquals(expected1, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().setPreferredTape( tape1 ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        assertEquals(null, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().addUnavailableTape( tape1.getId() )
                                                  .setPreferredTape( tape1 ),
                        true
                ), "Shoulda throttle-prevented selection of tape not in exclusion set.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");

        final S3Object object2 = mockDaoDriver.createObject( bucket.getId(), "o2", -1 );
        final List< Blob > extraBlobs = 
                mockDaoDriver.createBlobs( object2.getId(), 5, 100L * 1024 * 1024 * 1024 );
        final Set<JobEntry> chunks2 = mockDaoDriver.createJobEntries( job.getId(), extraBlobs );
        mockDaoDriver.createPersistenceTargetsForChunks(chunks2);
        assertEquals(null, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().addUnavailableTape( tape1.getId() )
                                                  .setPreferredTape( tape1 ),
                        true
                ), "Shoulda cached write optimization data.");
        CanAllocatePersistenceTargetSupport.clearCachedBucketWriteOptimizationData();
        final Object expected = tape2.getId();
        assertEquals(expected, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().addUnavailableTape( tape1.getId() )
                                                  .setPreferredTape( tape1 ),
                        true
                ), "Shoulda selected tape not in exclusion set.");

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");
    }
    
    
    @Test
    public void testCapacityWriteOptimizationThrottlingCannotResultInStorageDomainStallFailure()
    {
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition partition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition.getId(), TapeType.LTO5 ); 
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ), 
                sd.setWriteOptimization( WriteOptimization.CAPACITY ) );
        
        final Tape tape1 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        dbSupport.getDataManager().createBean( tape1 );
        
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final AtomicLong currentSize = new AtomicLong( 0 );
        final Object expected1 = tape1.getId();
        assertEquals(expected1, selectTape(strategy,
                        currentSize,
                        pts,
                        new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        assertEquals(100L * 1024 * 1024 * 1024,  currentSize.get(), "Shoulda gotten correct current size of bucket.");
        final Object expected = tape1.getId();
        assertEquals(expected, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().setPreferredTape( tape1 ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        assertEquals(null, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().addUnavailableTape( tape1.getId() )
                                                  .setPreferredTape( tape1 ),
                        true
                ), "Shoulda throttle-prevented selection of tape not in exclusion set.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");
    }


    @Test
    public void testCapacityWriteOptimizationOnlyConsidersIndividualMediaThatIsActuallyUsable()
    {
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition partition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final TapePartition partition2 =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN + "2" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition.getId(), TapeType.values()[ 0 ] );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition2.getId(), TapeType.values()[ 0 ] );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd.setWriteOptimization( WriteOptimization.CAPACITY ) );

        final Tape tape1 = BeanFactory.newBean( Tape.class )
                .setSerialNumber( UUID.randomUUID().toString() )
                .setBarCode( UUID.randomUUID().toString() )
                .setAvailableRawCapacity(99L * 1024 * 1024 * 1024)
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Tape tape2 = BeanFactory.newBean( Tape.class )
                .setSerialNumber( UUID.randomUUID().toString() )
                .setBarCode( UUID.randomUUID().toString() )
                .setAvailableRawCapacity(99L * 1024 * 1024 * 1024)
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Tape tape3 = BeanFactory.newBean( Tape.class )
                .setSerialNumber( UUID.randomUUID().toString() )
                .setBarCode( UUID.randomUUID().toString() )
                .setAvailableRawCapacity(100L * 1024 * 1024 * 1024)
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition2.getId() )
                .setStorageDomainMemberId( sdm2.getId() );
        final Tape tape4 = BeanFactory.newBean( Tape.class )
                .setSerialNumber( UUID.randomUUID().toString() )
                .setBarCode( UUID.randomUUID().toString() )
                .setAvailableRawCapacity(1000L * 1024 * 1024 * 1024)
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( tape1 );
        dbSupport.getDataManager().createBean( tape2 );
        dbSupport.getDataManager().createBean( tape3 );
        dbSupport.getDataManager().createBean( tape4 );
        mockDaoDriver.updateBean( partition2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( object.getId(), 100, (long) 1024 * 1024 * 1024);
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries( job.getId(), blobs );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final AtomicLong currentSize = new AtomicLong( 0 );

        assertFalse(dbSupport.getServiceManager().getService( TapeService.class ).attain( tape4.getId() ).isAssignedToStorageDomain(), "Should not be assigned to storage domain yet");

        final Object expected = tape4.getId();
        assertEquals(expected, selectTape(strategy,
                currentSize,
                pts,
                new MockTapeAvailability().setTapePartitionId( tape4.getPartitionId() ),
                true
        ), "Shoulda selected tape not assigned to bucket since no single tape will fit the chunk.");
        assertTrue(dbSupport.getServiceManager().getService( TapeService.class ).attain( tape4.getId() ).isAssignedToStorageDomain(), "Should have been assigned to storage domain");
    }
    
    
    @Test
    public void testPerformanceWriteOptimizationTapeSelectedWhichIsAlreadyAssignedToBucketWhenPossible()
    {
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition partition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition.getId(), TapeType.LTO5 ); 
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ), 
                sd.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        
        final Tape tape1 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Tape tape2 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "b" )
                .setPartitionId( partition.getId() )
                .setState( TapeState.NORMAL );
        final Tape tape3 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "wp" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setWriteProtected( true );
        dbSupport.getDataManager().createBean( tape1 );
        dbSupport.getDataManager().createBean( tape2 );
        dbSupport.getDataManager().createBean( tape3 );
        
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final AtomicLong currentSize = new AtomicLong( 0 );
        final Object expected4 = tape1.getId();
        assertEquals(expected4, selectTape(strategy,
                        currentSize,
                        pts,
                        new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        assertEquals(100L * 1024 * 1024 * 1024,  currentSize.get(), "Shoulda gotten correct current size of bucket.");
        final Object expected3 = tape1.getId();
        assertEquals(expected3, selectTape(strategy,
                        currentSize,
                        pts,
                        new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        final Object expected2 = tape1.getId();
        assertEquals(expected2, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().setPreferredTape( tape1 ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        final Object expected1 = tape1.getId();
        assertEquals(expected1, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().setPreferredTape( tape1 ),
                        true
                ), "Shoulda selected tape already assigned to bucket.");
        final Object expected = tape2.getId();
        assertEquals(expected, selectTape(strategy,
                        new AtomicLong(0),
                        pts,
                        new MockTapeAvailability().addUnavailableTape( tape1.getId() )
                                                  .setPreferredTape( tape1 ),
                        true
                ), "Should notta throttle-prevented selection of tape not in exclusion set.");

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");
    }
    
    
    @Test
    public void testSelectTapeCreatesFailureWhenNoFreeTapesRemaining()
    {
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition partition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), partition.getId(), TapeType.LTO5 ); 
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ), 
                sd.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT ).getId();
        final JobEntry chunk = mockDaoDriver.createJobEntry( jobId );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final UUID selectedTape = selectTape(strategy,
                new AtomicLong(12000),
                pts,
                new MockTapeAvailability().setTapePartitionId( partition.getId() ),
                true
        );
        assertNull(selectedTape, "Should notta selected a tape since none of them had enough space.");

        final StorageDomainFailureService failureService = dbSupport
                .getServiceManager()
                .getService( StorageDomainFailureService.class );
        final StorageDomainFailure mainFailure =
                failureService.retrieve( Require.nothing() );
        assertNotNull(mainFailure, "Shoulda created a partition failure for the main partition.");
        assertNotNull(mainFailure.getErrorMessage(), "Shoulda had an error message.");
        assertEquals(StorageDomainFailureType.WRITES_STALLED_DUE_TO_NO_FREE_MEDIA_REMAINING, mainFailure.getType(), "Shoulda had the writes stalled failure type.");
    }

    @Test
    public void testSelectTapeIgnoresUnacceptableTapes()
    {
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition partition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final Tape tape1 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );

        dbSupport.getDataManager().createBean( tape1 );


        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final AtomicLong currentSize = new AtomicLong( 0 );
        final Object expected = tape1.getId();
        assertEquals(expected, selectTape(strategy,
                currentSize,
                pts,
                new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                true
        ), "Should select tape .");
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape1, TapeState.BAD );
        assertNull( selectTape(strategy,
                currentSize,
                pts,
                new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                true
        ), "Shouldnot select tape1 since state is BAD.");
        tapeService.transistState( tape1, TapeState.NORMAL );
        tapeService.update(
                tape1.setWriteProtected( true ),
                Tape.WRITE_PROTECTED );
        assertNull( selectTape(strategy,
                currentSize,
                pts,
                new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                true
        ), "Shouldnot select tape1 since tape is Write Protected.");
        tapeService.update(
                tape1.setWriteProtected( false ),
                Tape.WRITE_PROTECTED );
        assertEquals(expected, selectTape(strategy,
                currentSize,
                pts,
                new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                true
        ), "Should select tape .");
    }

    @Test
    public void testSelectTapeReturnsCorrectlyWhenBucketNotIsolated()
    {
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain sd2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final TapePartition partition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final TapePartition partition2 =
                mockDaoDriver.createTapePartition( null, "1234" );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition2.getId(), TapeType.LTO5 );

        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd2.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final Tape tape1 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() )
                .setAssignedToStorageDomain( true );

        final Tape tape2 = BeanFactory.newBean( Tape.class )
                .setAvailableRawCapacity( Long.valueOf( 500L * 1024 * 1024 * 1024 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "b" )
                .setState( TapeState.NORMAL )
                .setPartitionId( partition2.getId() )
                .setAssignedToStorageDomain( false );

        dbSupport.getDataManager().createBean( tape1 );
        dbSupport.getDataManager().createBean( tape2 );


        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId(), blob );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());
        final Set<LocalBlobDestination> ptsForSd = pts.stream()
                .filter(pt -> pt.getStorageDomainId().equals(sd.getId()))
                .collect(java.util.stream.Collectors.toSet());

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final AtomicLong currentSize = new AtomicLong( 0 );
        final Object expected = tape1.getId();
        assertEquals(expected, selectTape(strategy,
                currentSize,
                ptsForSd,
                new MockTapeAvailability().setTapePartitionId( tape1.getPartitionId() ),
                true
        ), "Should select tape .");

        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object object2 = mockDaoDriver.createObject( bucket2.getId(), "o2", 100L * 1024 * 1024 * 1024 );
        final Blob blob2 = mockDaoDriver.getBlobFor( object2.getId() );
        final Job job2 = mockDaoDriver.createJob( bucket2.getId(), null, JobRequestType.PUT );
        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job2.getId() , blob2);
        final Set<LocalBlobDestination> pts2 = mockDaoDriver.createPersistenceTargetsForChunk(chunk2.getId());
        final Set<LocalBlobDestination> pts2ForSd2 = pts2.stream()
                .filter(pt -> pt.getStorageDomainId().equals(sd2.getId()))
                .collect(java.util.stream.Collectors.toSet());
        assertEquals(tape2.getId(), selectTape(strategy,
                new AtomicLong(0),
                pts2ForSd2,
                new MockTapeAvailability().setTapePartitionId( tape2.getPartitionId() ),
                true
        ), "Should select tape .");
    }

    @Test
    public void testSelectTapeReturnsCorrectlyWhenBucketIsolated()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, "tp2" );
        final TapePartition tp3 = mockDaoDriver.createTapePartition( null, "tp3" );
        final Tape tape1 = mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        final Tape tape2 = mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL, TapeType.LTO6 );
        final Tape tape3 = mockDaoDriver.createTape( tp2.getId(), TapeState.NORMAL, TapeType.LTO5 );
        final Tape tape4 = mockDaoDriver.createTape( tp2.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.createTape( tp3.getId(), TapeState.NORMAL, TapeType.LTO6 );
        // Set distinct capacities so ascending-capacity sort is deterministic:
        // tape1 < tape2 in tp1, tape4 < tape3 in tp2
        final TapeService tapeServiceSetup = dbSupport.getServiceManager().getService( TapeService.class );
        tapeServiceSetup.update( tape1.setAvailableRawCapacity( 500L * 1024 * 1024 ), Tape.AVAILABLE_RAW_CAPACITY );
        tapeServiceSetup.update( tape2.setAvailableRawCapacity( 600L * 1024 * 1024 ), Tape.AVAILABLE_RAW_CAPACITY );
        tapeServiceSetup.update( tape3.setAvailableRawCapacity( 600L * 1024 * 1024 ), Tape.AVAILABLE_RAW_CAPACITY );
        tapeServiceSetup.update( tape4.setAvailableRawCapacity( 500L * 1024 * 1024 ), Tape.AVAILABLE_RAW_CAPACITY );

        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "3" );
        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "4" );
        final StorageDomain sd5 = mockDaoDriver.createStorageDomain( "5" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), null );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), null );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), null );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), tp1.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm3a = mockDaoDriver.addTapePartitionToStorageDomain(
                sd3.getId(), tp1.getId(), TapeType.LTO5, WritePreferenceLevel.HIGH );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd3.getId(), tp1.getId(), TapeType.LTO6 );
        final StorageDomainMember sdm4a = mockDaoDriver.addTapePartitionToStorageDomain(
                sd4.getId(), tp2.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm4b = mockDaoDriver.addTapePartitionToStorageDomain(
                sd4.getId(), tp2.getId(), TapeType.LTO6, WritePreferenceLevel.HIGH );
        mockDaoDriver.addTapePartitionToStorageDomain( sd5.getId(), null, null );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd4.getId(), tp3.getId(), TapeType.LTO6, WritePreferenceLevel.NEVER_SELECT );

        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd4.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.RETIRED,
                sd5.getId() );

        final Bucket bucket2 = mockDaoDriver.createBucket( null, "otherbucket" );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final UUID isolatedBucketId = bucket.getId();

        // === Section 1: No tapes assigned to storage domains yet ===
        assertNull(
                strategy.selectTape( sd2.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertNull(
                strategy.selectTape( sd3.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertNull(
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertEquals( tape1.getId(),
                strategy.selectTape( sd2.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Shoulda been able to select tape with dao changes.");
        assertEquals( tape1.getId(),
                strategy.selectTape( sd3.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Shoulda been able to select tape with dao changes.");
        assertNull(
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), true ),
                "Should notta been able to select tape with dao changes.");
        assertEquals( tape4.getId(),
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape4.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Shoulda been able to select tape with dao changes.");
        assertEquals( tape2.getId(),
                strategy.selectTape( sd2.getId(), isolatedBucketId, tape1.getPartitionId(), tape2.getId(), 1000, new HashSet<>(), true ).getId(),
                "Shoulda respected preferred tape id since tape type filtering is partition-based.");
        assertEquals( tape2.getId(),
                strategy.selectTape( sd3.getId(), isolatedBucketId, tape2.getPartitionId(), tape2.getId(), 1000, new HashSet<>(), true ).getId(),
                "Shoulda respected preferred tape id.");
        assertEquals( tape4.getId(),
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape4.getPartitionId(), tape2.getId(), 1000, new HashSet<>(), true ).getId(),
                "Couldn't respect preferred tape id even if we wanted to.");

        // === Section 2: tape1 assigned to sd4/sdm4a, tape4 assigned to sd4/sdm4b with bucket2 ===
        tapeService.update(
                tape1.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm4a.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        tapeService.update(
                tape4.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm4b.getId() )
                        .setBucketId( bucket2.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.BUCKET_ID );

        assertNull(
                strategy.selectTape( sd2.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertNull(
                strategy.selectTape( sd3.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertNull(
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertEquals( tape2.getId(),
                strategy.selectTape( sd2.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Current code doesn't filter by tape type, so tape2 (LTO6) is valid for sd2's tp1/LTO5 member.");
        assertEquals( tape2.getId(),
                strategy.selectTape( sd3.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Shoulda been able to select tape with dao changes.");
        assertEquals( tape3.getId(),
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape3.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Shoulda been able to select tape with dao changes.");

        // === Section 3: tape1 reassigned to sd3/sdm3a, tape4 reassigned to bucket ===
        tapeService.update(
                tape1.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm3a.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        tapeService.update(
                tape4.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm4b.getId() )
                        .setBucketId( bucket.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.BUCKET_ID );

        assertNull(
                strategy.selectTape( sd2.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertNull(
                strategy.selectTape( sd3.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "Should notta been able to select tape without dao changes.");
        assertEquals( tape4.getId(),
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape4.getPartitionId(), null, 1000, new HashSet<>(), false ).getId(),
                "Shoulda been able to select tape without dao changes.");
        assertEquals( tape2.getId(),
                strategy.selectTape( sd2.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Current code doesn't filter by tape type, so tape2 (LTO6) is valid for sd2's tp1/LTO5 member.");
        assertEquals( tape2.getId(),
                strategy.selectTape( sd3.getId(), isolatedBucketId, tape2.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Shoulda been able to select tape with dao changes.");
        assertEquals( tape3.getId(),
                strategy.selectTape( sd4.getId(), isolatedBucketId, tape3.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Shoulda been able to select tape with dao changes.");
    }


    @Test
    public void testSelectTapeGeneratesOutOfMediaFailuresUntilMediaAvailable()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, "tp2" );
        final TapePartition tp3 = mockDaoDriver.createTapePartition( null, "tp3" );

        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "3" );
        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "4" );
        final StorageDomain sd5 = mockDaoDriver.createStorageDomain( "5" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), null );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), null );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), null );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd3.getId(), tp1.getId(), TapeType.LTO5, WritePreferenceLevel.HIGH );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd3.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd4.getId(), tp2.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd4.getId(), tp2.getId(), TapeType.LTO6, WritePreferenceLevel.HIGH );
        mockDaoDriver.addTapePartitionToStorageDomain( sd5.getId(), null, null );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd4.getId(), tp3.getId(), TapeType.LTO6, WritePreferenceLevel.NEVER_SELECT );

        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd4.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.RETIRED,
                sd5.getId() );

        mockDaoDriver.createBucket( null, "otherbucket" );

        // sd3 has only tape members (both for tp1), so stalling tp1 = fully stalled in WritesStalledSupport.
        // Set PERFORMANCE optimization so CanAllocatePersistenceTargetSupport always allows allocation.
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd3.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );
        final StorageDomainFailureService failureService =
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );

        // === Section 1: No tapes available ===
        assertNull(
                strategy.selectTape( sd2.getId(), null, tp1.getId(), null, 1000, new HashSet<>(), false ),
                "Should not select tape since no tapes available." );
        assertNull(
                strategy.selectTape( sd3.getId(), null, tp1.getId(), null, 1000, new HashSet<>(), false ),
                "Should not select tape since no tapes available." );
        assertNull(
                strategy.selectTape( sd4.getId(), null, tp1.getId(), null, 1000, new HashSet<>(), false ),
                "Should not select tape since no tapes available." );
        assertEquals( 0, failureService.getCount(),
                "No failures recorded yet — inner selectTape does not track failures." );
        assertNull(
                strategy.selectTape( sd2.getId(), null, tp1.getId(), null, 1000, new HashSet<>(), true ),
                "Should not select tape since no tapes available." );
        assertNull(
                strategy.selectTape( sd3.getId(), null, tp1.getId(), null, 1000, new HashSet<>(), true ),
                "Should not select tape since no tapes available." );
        assertNull(
                strategy.selectTape( sd4.getId(), null, tp1.getId(), null, 1000, new HashSet<>(), true ),
                "Should not select tape since no tapes available." );

        // Public selectTape for sd3: triggers out-of-media failure via WritesStalledSupport
        assertNull(
                strategy.selectTape( 1000, sd3.getId(), bucket.getId(),
                        new MockTapeAvailability( tp1.getId(), UUID.randomUUID() ), true ),
                "No tapes available via public selectTape." );
        assertEquals( 1, failureService.getCount(),
                "Should have recorded out-of-media failure for sd3." );

        // === Section 2: Tapes become available ===
        final Tape tape1 = mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        final Tape tape2 = mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.createTape( tp2.getId(), TapeState.NORMAL, TapeType.LTO5 );
        final Tape tape4 = mockDaoDriver.createTape( tp2.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.createTape( tp3.getId(), TapeState.NORMAL, TapeType.LTO6 );
        // Set distinct capacities for deterministic sorting
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.update( tape1.setAvailableRawCapacity( 500L * 1024 * 1024 ), Tape.AVAILABLE_RAW_CAPACITY );
        tapeService.update( tape2.setAvailableRawCapacity( 600L * 1024 * 1024 ), Tape.AVAILABLE_RAW_CAPACITY );
        tapeService.update( tape4.setAvailableRawCapacity( 500L * 1024 * 1024 ), Tape.AVAILABLE_RAW_CAPACITY );

        // Inner selectTape: allocated=false still returns null (no tapes allocated yet)
        assertNull(
                strategy.selectTape( sd2.getId(), null, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "No allocated tapes." );
        assertNull(
                strategy.selectTape( sd3.getId(), null, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "No allocated tapes." );
        assertNull(
                strategy.selectTape( sd4.getId(), null, tape2.getPartitionId(), null, 1000, new HashSet<>(), false ),
                "No allocated tapes." );

        // Inner selectTape: unallocated=true finds tapes
        assertEquals( tape1.getId(),
                strategy.selectTape( sd2.getId(), null, tape1.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Should select tape1 (lowest capacity in tp1)." );
        assertEquals( tape1.getId(),
                strategy.selectTape( sd3.getId(), null, tape1.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Should select tape1 (lowest capacity in tp1)." );
        assertEquals( tape4.getId(),
                strategy.selectTape( sd4.getId(), null, tape4.getPartitionId(), null, 1000, new HashSet<>(), true ).getId(),
                "Should select tape4 in tp2." );

        // Preferred tape assertions
        assertEquals( tape2.getId(),
                strategy.selectTape( sd2.getId(), null, tape1.getPartitionId(), tape2.getId(), 1000, new HashSet<>(), true ).getId(),
                "Current code doesn't filter by tape type, so preferred tape2 is valid for sd2." );
        assertEquals( tape2.getId(),
                strategy.selectTape( sd3.getId(), null, tape2.getPartitionId(), tape2.getId(), 1000, new HashSet<>(), true ).getId(),
                "Should respect preferred tape2." );
        assertEquals( tape4.getId(),
                strategy.selectTape( sd4.getId(), null, tape4.getPartitionId(), tape2.getId(), 1000, new HashSet<>(), true ).getId(),
                "Can't respect preferred tape2 since it's not in sd4's partition." );

        // Public selectTape: finds tape and clears out-of-media failure
        final UUID selectedTapeId = strategy.selectTape( 1000, sd3.getId(), bucket.getId(),
                new MockTapeAvailability( tp1.getId(), UUID.randomUUID() ), true );
        assertNotNull( selectedTapeId, "Should select tape now that media is available." );
        assertEquals( 0, failureService.getCount(),
                "Out-of-media failure should be cleared now that media was allocated." );
    }


    @Test
    public void testSelectTapePrefersHighestWritePrefTapesThenTapesWithNonNullLeastAvailCapacity()
    {
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        final TapePartition dp1 = mockDaoDriver.createTapePartition( null, "dp1" );
        final TapePartition dp2 = mockDaoDriver.createTapePartition( null, "dp2" );
        final TapePartition dp3 = mockDaoDriver.createTapePartition( null, "dp3" );
        final Tape tape6 = mockDaoDriver.createTape( dp2.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean( tape6.setAvailableRawCapacity( 10L * 100L * 1024L * 1024L ),
                Tape.AVAILABLE_RAW_CAPACITY );
        mockDaoDriver.createTape( dp3.getId(), TapeState.NORMAL );
        final Tape tape1 = mockDaoDriver.createTape( dp1.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape1.setAvailableRawCapacity( null ),
                Tape.AVAILABLE_RAW_CAPACITY );
        final Tape tape2 = mockDaoDriver.createTape( dp1.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean( tape2.setAvailableRawCapacity( 100L * 100L * 1024L * 1024L ),
                Tape.AVAILABLE_RAW_CAPACITY );
        final Tape tape3 = mockDaoDriver.createTape( dp1.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean( tape3.setAvailableRawCapacity( 10L * 100L * 1024L * 1024L ),
                Tape.AVAILABLE_RAW_CAPACITY );
        final Tape tape4 = mockDaoDriver.createTape( dp2.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean(
                tape4.setAvailableRawCapacity( null ),
                Tape.AVAILABLE_RAW_CAPACITY );
        final Tape tape5 = mockDaoDriver.createTape( dp2.getId(), TapeState.NORMAL );
        mockDaoDriver.updateBean( tape5.setAvailableRawCapacity( 100L * 100L * 1024L * 1024L ),
                Tape.AVAILABLE_RAW_CAPACITY );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember storageDomainMember1 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), dp1.getId(), TapeType.LTO5, WritePreferenceLevel.NORMAL );
        final StorageDomainMember storageDomainMember2 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), dp2.getId(), TapeType.LTO5, WritePreferenceLevel.HIGH );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                storageDomain.getId() );

        // Assign all tapes to the storage domain
        for ( final Tape tape : new Tape [] { tape1, tape2, tape3, tape4, tape5, tape6 } )
        {
            final UUID sdmId;
            if ( tape.getPartitionId().equals( dp1.getId() ) )
            {
                sdmId = storageDomainMember1.getId();
            }
            else
            {
                sdmId = storageDomainMember2.getId();
            }
            mockDaoDriver.updateBean( tape.setStorageDomainMemberId( sdmId )
                                          .setAssignedToStorageDomain( true ),
                    PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        }

        final WriteChunkTapeSelectionStrategy strategy =
                new WriteChunkTapeSelectionStrategy( dbSupport.getServiceManager() );

        // dp2 tapes (allocated): tape6(~1000MB), tape5(~10000MB), tape4(null capacity)
        // Sorted by capacity ASC, null excluded: tape6(~1000MB), tape5(~10000MB)
        assertEquals( tape6.getId(),
                strategy.selectTape( storageDomain.getId(), null, tape6.getPartitionId(), null, 1000, new HashSet<>(), false ).getId(),
                "Shoulda preferred correct tape." );
        assertEquals( tape5.getId(),
                strategy.selectTape( storageDomain.getId(), null, tape5.getPartitionId(), null, 1000,
                        CollectionFactory.toSet( tape6.getId() ), false ).getId(),
                "Shoulda preferred correct tape." );

        // dp1 tapes (allocated): tape3(~1000MB), tape2(~10000MB), tape1(null capacity)
        // Sorted by capacity ASC, null excluded: tape3(~1000MB), tape2(~10000MB)
        assertEquals( tape3.getId(),
                strategy.selectTape( storageDomain.getId(), null, tape3.getPartitionId(), null, 1000,
                        CollectionFactory.toSet( tape5.getId(), tape6.getId() ), false ).getId(),
                "Shoulda preferred correct tape." );
        assertEquals( tape2.getId(),
                strategy.selectTape( storageDomain.getId(), null, tape2.getPartitionId(), null, 10000,
                        CollectionFactory.toSet( tape3.getId(), tape5.getId(), tape6.getId() ), false ).getId(),
                "Shoulda preferred correct tape." );
        assertNotNull(
                strategy.selectTape( storageDomain.getId(), null, tape2.getPartitionId(), null, 10000,
                        CollectionFactory.toSet( tape3.getId(), tape5.getId(), tape6.getId() ), false ),
                "Shoulda preferred correct tape." );

        // Only tape1 remains in dp1 (null capacity) — should return null
        assertNull(
                strategy.selectTape( storageDomain.getId(), null, tape2.getPartitionId(), null, 100L * 100L,
                        CollectionFactory.toSet( tape2.getId(), tape3.getId(), tape5.getId(), tape6.getId() ), false ),
                "Shoulda preferred correct tape." );
    }


    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    private static MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
        mockDaoDriver = new MockDaoDriver( dbSupport );
    }
    @BeforeEach
    public void initialize() {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }


    private UUID selectTape(
            final WriteChunkTapeSelectionStrategy strategy,
            final AtomicLong fullSize,
            final Collection<LocalBlobDestination> pts,
            final TapeAvailability tapeAvailability,
            final boolean allocateSelection)
    {
        if ( fullSize.get() == 0 )
        {
            long sum = 0;
            for ( final LocalBlobDestination chunk : pts )
            {
                sum += dbSupport.getServiceManager().getService( JobEntryService.class ).getSizeInBytes(
                        chunk.getEntryId() );
            }
            fullSize.set( sum );
        }
        final LocalBlobDestination pt = pts.stream().findFirst().get();
        final UUID bucketId = new BlobLocalTargetRM( pt, dbSupport.getServiceManager() ).getJobEntry().getJob().getBucket().getId();
        return strategy.selectTape(fullSize.get(), pt.getStorageDomainId(), bucketId, tapeAvailability, allocateSelection);
    }
}
