package com.spectralogic.s3.dataplanner.backend.frmwrk;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;

import java.util.*;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WorkAggregationUtils_Test {

    @Test
    public void testMarkReadChunksInProgress_NoEntries() {
        // Test with empty collection
        List<JobEntry> emptyEntries = new ArrayList<>();
        int result = WorkAggregationUtils.markReadChunksInProgress(emptyEntries, dbSupport.getServiceManager());
        assertEquals(0, result);
    }

    @Test
    public void testMarkReadChunksInProgress_WithEntries() {
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        Collection<JobEntry> entries = Arrays.asList(entry1, entry2);

        int result = WorkAggregationUtils.markReadChunksInProgress(entries, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkReadChunksInProgress_WithDuplicateEntries() {
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        Collection<JobEntry> entries = Arrays.asList(entry1, entry1);

        int result = WorkAggregationUtils.markReadChunksInProgress(entries, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkReadChunksInProgress_WithReadDirective_NullDirective() {
        ReadDirective readDirective = new ReadDirective(
                BlobStoreTaskPriority.values()[0],
                UUID.randomUUID(),
                PersistenceType.AZURE,
                new ArrayList<>()
        );
        int result = WorkAggregationUtils.markReadChunksInProgress(readDirective, dbSupport.getServiceManager());
        assertEquals(0, result);
    }

    @Test
    public void testMarkReadChunksInProgress_WithReadDirective_WithEntries() {
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        ReadDirective readDirective = new ReadDirective(
                BlobStoreTaskPriority.values()[0],
                UUID.randomUUID(),
                PersistenceType.AZURE,
                entries
        );
        int result = WorkAggregationUtils.markReadChunksInProgress(readDirective, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkWriteChunksInProgress_WithWriteDirective_NullDirective() {
        LocalWriteDirective wd = new LocalWriteDirective(
                new HashSet<>(),
                sd,
                BlobStoreTaskPriority.values()[0],
                new ArrayList<>(),
                0,
                bucket);
        int result = WorkAggregationUtils.markWriteChunksInProgress(wd, dbSupport.getServiceManager());
        assertEquals(0, result);

    }

    @Test
    public void testMarkWriteChunksInProgress_WithWriteDirective_WithEntries() {
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        final Set<LocalBlobDestination> destinations = mockDaoDriver.createPersistenceTargetsForChunks(entries);
        LocalWriteDirective wd = new LocalWriteDirective(
                destinations,
                sd,
                BlobStoreTaskPriority.values()[0],
                entries,
                b1.getLength() + b2.getLength(),
                bucket);
        int result = WorkAggregationUtils.markWriteChunksInProgress(wd, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkWriteChunksInProgress_WithJobEntries_NoEntries() {
        Collection<JobEntry> emptyEntries = Collections.emptyList();
        int result = WorkAggregationUtils.markWriteChunksInProgress(emptyEntries, dbSupport.getServiceManager());
        assertEquals(0, result);
    }

    @Test
    public void testMarkWriteChunksInProgress_WithJobEntries_WithEntries() {
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);

        int result = WorkAggregationUtils.markWriteChunksInProgress(entries, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkLocalDestinationsInProgress_NoDestinations() {
        Collection<LocalBlobDestination> emptyDestinations = Collections.emptyList();
        int result = WorkAggregationUtils.markLocalDestinationsInProgress(emptyDestinations, dbSupport.getServiceManager());
        assertEquals(0, result);
    }

    @Test
    public void testMarkLocalDestinationsInProgress_WithDestinations() {
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        final Set<LocalBlobDestination> destinations = mockDaoDriver.createPersistenceTargetsForChunks(entries);

        int result = WorkAggregationUtils.markLocalDestinationsInProgress(destinations, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkS3DestinationsInProgress_WithDestinations() {
        final S3Target target = mockDaoDriver.createS3Target( "target1" );
        mockDaoDriver.createS3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        final Set<S3BlobDestination> destinations = mockDaoDriver.createReplicationTargetsForChunks(S3DataReplicationRule.class, S3BlobDestination.class, entries);

        int result = WorkAggregationUtils.markS3DestinationsInProgress(destinations, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkAzureDestinationsInProgress_NoDestinations() {
        Collection<AzureBlobDestination> emptyDestinations = Collections.emptyList();
        int result = WorkAggregationUtils.markAzureDestinationsInProgress(emptyDestinations, dbSupport.getServiceManager());
        assertEquals(0, result);
    }

    @Test
    public void testMarkAzureDestinationsInProgress_WithDestinations() {
        final AzureTarget target = mockDaoDriver.createAzureTarget( "target1" );
        mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        final Set<AzureBlobDestination> destinations = mockDaoDriver.createReplicationTargetsForChunks(AzureDataReplicationRule.class, AzureBlobDestination.class, entries);

        int result = WorkAggregationUtils.markAzureDestinationsInProgress(destinations, dbSupport.getServiceManager());
        assertEquals(2, result);
    }

    @Test
    public void testMarkDs3DestinationsInProgress_NoDestinations() {
        Collection<Ds3BlobDestination> emptyDestinations = Collections.emptyList();
        int result = WorkAggregationUtils.markDs3DestinationsInProgress(emptyDestinations, dbSupport.getServiceManager());

        assertEquals(0, result);
    }

    @Test
    public void testMarkDs3DestinationsInProgress_WithDestinations() {
        final Ds3Target target = mockDaoDriver.createDs3Target( "target1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        List<JobEntry> entries = Arrays.asList(entry1, entry2);
        final Set<Ds3BlobDestination> destinations = mockDaoDriver.createReplicationTargetsForChunks(Ds3DataReplicationRule.class, Ds3BlobDestination.class, entries);
        int result = WorkAggregationUtils.markDs3DestinationsInProgress(destinations, dbSupport.getServiceManager());

        assertEquals(2, result);
    }

    @Test
    public void blobIsInCache() {
        final MockDiskManager mockDiskManager = new MockDiskManager( dbSupport.getServiceManager() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry entry1 = mockDaoDriver.createJobEntry( job.getId(), b1 );
        final JobEntry entry2 = mockDaoDriver.createJobEntry( job.getId(), b2 );
        mockDaoDriver.markBlobInCache(b1.getId());
        mockDiskManager.blobLoadedToCache(b1.getId());

        WhereClause whereClause = WorkAggregationUtils.blobIsOnDisk();
        LocalJobEntryWork results = dbSupport.getServiceManager().getRetriever(LocalJobEntryWork.class).retrieve(whereClause);
        assertNotNull(whereClause);
        assertNotNull(results);
    }

    @Test
    public void testMarkS3DestinationsInProgress_NoDestinations() {
        Collection<S3BlobDestination> emptyDestinations = Collections.emptyList();
        int result = WorkAggregationUtils.markS3DestinationsInProgress(emptyDestinations, dbSupport.getServiceManager());

        assertEquals(0, result);

    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    private MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
    private static final String DATA_POLICY_TAPE_DUAL_COPY_NAME = "Dual Copy on Tape";
    private Bucket bucket;
    private Blob b1;
    private Blob b2;
    private StorageDomain sd;
    private DataPolicy dp;

    @BeforeEach
    public void setUp() {
        dp = mockDaoDriver.createDataPolicy( DATA_POLICY_TAPE_DUAL_COPY_NAME );
        bucket = mockDaoDriver.createBucket( null, dp.getId(),"bucket1" );
        S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b2 = mockDaoDriver.getBlobFor( o2.getId() );
        sd = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createDataPersistenceRule(
                dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
    }

    @BeforeAll
    public static void initDB() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
