package com.spectralogic.s3.dataplanner.backend.frmwrk;



import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.persistencetarget.BlobDestinationUtils;
import java.util.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;


public class TapeWorkAggregationUtils_Test {

    @Test
    public void testNoTapeIOWorkWhenNoJobsExist() {
        boolean result = TapeWorkAggregationUtils.anyTapeIOWorkOfPriority(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH);
        assertFalse(result, "Should return false when no jobs exist");
    }

    @Test
    public void testAnyTapeIOWorkOfPriorityPut() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        boolean result = TapeWorkAggregationUtils.anyTapeIOWorkOfPriority(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH);
        assertTrue(result, "There should be be tape PUT jobs");
    }

    @Test
    public void testAnyTapeIOWorkOfPriorityGet() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape1.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.GET );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        mockDaoDriver.updateBean( entry1.setReadFromTapeId(tape1.getId()), JobEntry.READ_FROM_TAPE_ID);
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        boolean result = TapeWorkAggregationUtils.anyTapeIOWorkOfPriority(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH);
        assertTrue(result, "There should be be tape Get jobs");
    }

    @Test
    public void testAnyRegularPriorityTapeIOWorkPut() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        boolean result = TapeWorkAggregationUtils.anyRegularPriorityTapeIOWork(dbSupport.getServiceManager());
        assertTrue(result, "There should be be tape PUT jobs");
    }

    @Test
    public void testAnyRegularPriorityTapeIOWorkGet() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape1.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.GET );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        mockDaoDriver.updateBean( entry1.setReadFromTapeId(tape1.getId()), JobEntry.READ_FROM_TAPE_ID);
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        boolean result = TapeWorkAggregationUtils.anyRegularPriorityTapeIOWork(dbSupport.getServiceManager());
        assertTrue(result, "There should be be tape Get jobs");
    }

    @Test
    public void testDiscoverTapeWorkAggregatedPut() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.NORMAL), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );

        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry3 = mockDaoDriver.createJobEntry( job2.getId(), b1 );
        final JobEntry entry4 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2, entry3, entry4);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.NORMAL, null,
        null);
        assertNotNull(result, "LocalReadDirectives result should not be null");
        assertEquals(2, result.size(), "There should be 2 LocalWriteDirectives returned");
        assertEquals(BlobStoreTaskPriority.HIGH, result.get(0).getPriority(), "LocalWriteDirective should be sorted");
    }

    @Test
    public void testDiscoverTapeWorkAggregatedSortOrder() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.NORMAL), Job.PRIORITY);

        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        mockDaoDriver.updateBean( entry1.setChunkNumber(1), JobEntry.CHUNK_NUMBER);
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        mockDaoDriver.updateBean( entry2.setChunkNumber(2), JobEntry.CHUNK_NUMBER);


        final Job job3 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job3.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        TestUtil.sleep(100);

        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);

        final JobEntry entry3 = mockDaoDriver.createJobEntry( job3.getId(), b1 );
        mockDaoDriver.updateBean( entry3.setChunkNumber(3), JobEntry.CHUNK_NUMBER);
        final JobEntry entry4 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.updateBean( entry4.setChunkNumber(6), JobEntry.CHUNK_NUMBER);

        final JobEntry entry5 = mockDaoDriver.createJobEntry( job3.getId(), b1 );
        mockDaoDriver.updateBean( entry4.setChunkNumber(5), JobEntry.CHUNK_NUMBER);
        final JobEntry entry6 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.updateBean( entry6.setChunkNumber(4), JobEntry.CHUNK_NUMBER);


        List<JobEntry> entries = Arrays.asList(entry1, entry2, entry3, entry4, entry5, entry6);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.NORMAL, null,
                null);
        assertNotNull(result, "LocalReadDirectives result should not be null");
        assertEquals(2, result.size(), "There should be 3 LocalWriteDirectives returned");
        assertEquals(BlobStoreTaskPriority.HIGH, result.getFirst().getPriority(), "LocalWriteDirective should be sorted");
        assertEquals(4, result.getFirst().getEntries().size(), "LocalWriteDirective should be sorted");
        assertEquals(entry3.getId(), result.getFirst().getEntries().stream().findFirst().get().getId(), "LocalWriteDirective should be sorted");
        entries = (List<JobEntry>) result.getFirst().getEntries();
        Optional<JobEntry> last = entries.stream()
                .skip(entries.size() - 1)
                .findFirst();
        assertEquals(entry4.getId(), last.get().getId(), "LocalWriteDirective should be sorted");
    }



    @Test
    public void testDiscoverTapeWorkAggregatedPutGet() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.NORMAL), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );

        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.GET );
        S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry3 = mockDaoDriver.createJobEntry( job2.getId(), b3 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2, entry3);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.updateBean( entry3.setReadFromTapeId(tape1.getId()), JobEntry.READ_FROM_TAPE_ID);
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.NORMAL, null,
                null);
        assertNotNull(result, "LocalReadDirectives result should not be null");
        assertEquals(2, result.size(), "There should be 2 LocalWriteDirectives returned");
        assertEquals(BlobStoreTaskPriority.HIGH, result.get(0).getPriority(), "LocalWriteDirective should be sorted");
    }

    @Test
    public void testDiscoverTapeWorkAggregated_Put_MixedPriority() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.LOW), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );

        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry3 = mockDaoDriver.createJobEntry( job2.getId(), b1 );
        final JobEntry entry4 = mockDaoDriver.createJobEntry( job2.getId(), b2 );


        List<JobEntry> entries = Arrays.asList(entry1, entry2, entry3, entry4);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH, null,
                null);
        assertEquals(1, result.size(), "There should be no jobs with matching priority");
    }

    @Test
    public void testDiscoverTapeWorkAggregated_GetCache() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.GET );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        mockDaoDriver.updateBean( entry1.setReadFromTapeId(tape1.getId()), JobEntry.READ_FROM_TAPE_ID);
        mockDaoDriver.updateBean( entry2.setReadFromTapeId(tape1.getId()), JobEntry.READ_FROM_TAPE_ID);

        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.GET );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.URGENT), Job.PRIORITY);
        final JobEntry entry3 = mockDaoDriver.createJobEntry( job2.getId(), b1 );
        final JobEntry entry4 = mockDaoDriver.createJobEntry( job2.getId(), b2 );
        mockDaoDriver.updateBean( entry3.setReadFromTapeId(tape1.getId()), JobEntry.READ_FROM_TAPE_ID);
        mockDaoDriver.updateBean( entry4.setReadFromTapeId(tape1.getId()), JobEntry.READ_FROM_TAPE_ID);


        List<JobEntry> entries = Arrays.asList(entry1, entry2, entry3, entry4);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH, null,
                null);

        assertNotNull(result, "ReadIntoCacheDirective result should not be null");
        assertEquals(2, result.size(), "ReadIntoCacheDirective result should have 2 entries");
        assertEquals(BlobStoreTaskPriority.URGENT, result.get(0).getPriority(), "ReadIntoCacheDirective result should be sorted.");
    }



    @Test
    public void testDiscoverTapeWorkAggregatedPutMinSpanningEnabled() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        mockDaoDriver.updateBean(job.setMinimizeSpanningAcrossMedia(true), Job.MINIMIZE_SPANNING_ACROSS_MEDIA);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );

        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry3 = mockDaoDriver.createJobEntry( job2.getId(), b1 );
        final JobEntry entry4 = mockDaoDriver.createJobEntry( job2.getId(), b2 );

        List<JobEntry> entries = Arrays.asList(entry1, entry2, entry3, entry4);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDaoDriver.markBlobInCache(b2.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH, null,
                null);
        assertEquals(2, result.size(), "There should be no jobs with matching priority");
    }


    @Test
    public void testDiscoverTapeWorkAggregatedPutMinSpanningEnabledDiffPriority() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        mockDaoDriver.updateBean(job.setMinimizeSpanningAcrossMedia(true), Job.MINIMIZE_SPANNING_ACROSS_MEDIA);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );


        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        final JobEntry entry3 = mockDaoDriver.createJobEntry( job2.getId(), b1 );


        List<JobEntry> entries = Arrays.asList(entry1, entry3);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH, null,
                null);
        assertEquals(2, result.size(), "There should be no jobs with matching priority");
    }

    @Test
    public void testDiscoverTapeWorkAggregatedPutMinSpanningEnabledGrouping() {
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( tape2.setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        mockDaoDriver.updateBean(job.setMinimizeSpanningAcrossMedia(true), Job.MINIMIZE_SPANNING_ACROSS_MEDIA);
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );


        final Job job2 = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.updateBean(job2.setPriority(BlobStoreTaskPriority.HIGH), Job.PRIORITY);
        mockDaoDriver.updateBean(job2.setMinimizeSpanningAcrossMedia(true), Job.MINIMIZE_SPANNING_ACROSS_MEDIA);
        final JobEntry entry3 = mockDaoDriver.createJobEntry( job2.getId(), b1 );


        List<JobEntry> entries = Arrays.asList(entry1, entry3);
        mockDaoDriver.createLocalBlobDestinations(entries, Arrays.asList(rule), bucket.getId() );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());
        List<IODirective> result = TapeWorkAggregationUtils.discoverTapeWorkAggregated(dbSupport.getServiceManager(), BlobStoreTaskPriority.HIGH, null,
                null);
        assertEquals(1, result.size(), "There should be no jobs with matching priority");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    private final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
    private static final String DATA_POLICY_TAPE_DUAL_COPY_NAME = "Dual Copy on Tape";
    private Bucket bucket;
    private Blob b1;
    private Blob b2;
    private StorageDomain sd;
    private DataPolicy dp;
    private DataPersistenceRule rule;

    @BeforeEach
    public void setUp() {
        dbSupport.reset();
        dp = mockDaoDriver.createDataPolicy( DATA_POLICY_TAPE_DUAL_COPY_NAME );
        bucket = mockDaoDriver.createBucket( null, dp.getId(),"bucket1" );
        S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b2 = mockDaoDriver.getBlobFor( o2.getId() );
        sd = mockDaoDriver.createStorageDomain( "sd1" );
        rule = mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
    }

    @BeforeAll
    public static void setupDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}