package com.spectralogic.integrations.performance;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


@Tag("performance")
public class IOMPersistenceRulePerfTest {
    private final static Logger LOG = Logger.getLogger(IOMPersistenceRulePerfTest.class);

    private static final String BUCKET_NAME = "perf-iom-persistence";
    private static final int OBJECT_COUNT = 50;
    private static final long OBJECT_SIZE = 5 * 1024L; // 5 KB

    private Ds3Client client;
    private UUID newPersistenceRuleId;

    private void updateUserDataPolicy() throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        assertTrue(singleCopyTapeDP.isPresent());
        client.modifyUserSpectraS3(
                new ModifyUserSpectraS3Request(authId)
                        .withDefaultDataPolicyId(singleCopyTapeDP.get().getId()));
    }

    private void addSecondCopyPersistenceRule() throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        assertTrue(singleCopyTapeDP.isPresent());

        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeSecondSD = storageDomains.stream()
                .filter(sd -> sd.getName().equals(STORAGE_DOMAIN_TAPE_DUAL_COPY_NAME)).findFirst();
        assertTrue(tapeSecondSD.isPresent());

        // Check if rule already exists
        DataPersistenceRule existing = findSecondCopyRule(singleCopyTapeDP.get().getId(), tapeSecondSD.get().getId());
        if (existing != null) {
            newPersistenceRuleId = existing.getId();
            return;
        }

        PutDataPersistenceRuleSpectraS3Response resp = client.putDataPersistenceRuleSpectraS3(
                new PutDataPersistenceRuleSpectraS3Request(
                        singleCopyTapeDP.get().getId(),
                        DataIsolationLevel.STANDARD,
                        tapeSecondSD.get().getId(),
                        DataPersistenceRuleType.PERMANENT));
        newPersistenceRuleId = resp.getDataPersistenceRuleResult().getId();
    }

    private DataPersistenceRule findSecondCopyRule(UUID dataPolicyId, UUID storageDomainId) throws IOException {
        GetDataPersistenceRulesSpectraS3Response rules =
                client.getDataPersistenceRulesSpectraS3(new GetDataPersistenceRulesSpectraS3Request());
        return rules.getDataPersistenceRuleListResult().getDataPersistenceRules().stream()
                .filter(rule -> rule.getStorageDomainId().equals(storageDomainId)
                        && rule.getDataPolicyId().equals(dataPolicyId))
                .findFirst().orElse(null);
    }

    private void cleanupPersistenceRule() throws IOException {
        if (newPersistenceRuleId != null) {
            try {
                client.deleteDataPersistenceRuleSpectraS3(
                        new DeleteDataPersistenceRuleSpectraS3Request(newPersistenceRuleId));
            } catch (IOException e) {
                LOG.warn("Failed to delete persistence rule: " + e.getMessage());
            }
            newPersistenceRuleId = null;
        }
    }

    private Path createTempDirWithObjects(int count, long sizeInBytes) throws IOException {
        final Path tempDir = Files.createTempDirectory("perf-iom-test");
        tempDir.toFile().deleteOnExit();
        for (int i = 0; i < count; i++) {
            final File file = new File(tempDir.toFile(), "object_" + i + ".dat");
            file.deleteOnExit();
            try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(sizeInBytes);
            }
        }
        return tempDir;
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupPersistenceRule();
            cleanupBuckets(client, BUCKET_NAME);
            clearAllJobs(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    public void testPutObjectsThenAddPersistenceRuleAndTrackIOMCompletion() throws IOException {
        LOG.info("Starting test: IOMPersistenceRulePerfTest - " + OBJECT_COUNT + " objects of " + OBJECT_SIZE + " bytes");
        final long testStartTime = System.currentTimeMillis();

        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
        updateUserDataPolicy();
        helper.ensureBucketExists(BUCKET_NAME);

        // --- Phase 1: PUT objects ---
        final long putStartTime = System.currentTimeMillis();
        LOG.info("Phase 1: Creating and putting " + OBJECT_COUNT + " objects (" + OBJECT_SIZE + " bytes each)...");

        final Path tempDir = createTempDirWithObjects(OBJECT_COUNT, OBJECT_SIZE);
        final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(tempDir);

        final Ds3ClientHelpers.Job putJob = helper.startWriteJob(BUCKET_NAME, objects);
        putJob.transfer(new FileObjectPutter(tempDir));

        LOG.info("PUT job created: " + putJob.getJobId());

        // Wait for PUT job to complete
        final GetActiveJobsSpectraS3Request activeJobsRequest = new GetActiveJobsSpectraS3Request();
        while (!client.getActiveJobsSpectraS3(activeJobsRequest)
                .getActiveJobListResult().getActiveJobs().isEmpty()) {
            TestUtil.sleep(5000);
        }

        final long putEndTime = System.currentTimeMillis();
        final long putDuration = putEndTime - putStartTime;
        LOG.info("Phase 1 complete: PUT of " + OBJECT_COUNT + " objects took " + putDuration + " ms ("
                + (putDuration / 1000) + " seconds)");

        // Verify PUT completed
        final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(BUCKET_NAME));
        assertEquals(OBJECT_COUNT, bucketResponse.getListBucketResult().getObjects().size(),
                "All " + OBJECT_COUNT + " objects should be in the bucket");

        final int blobsOnTape = getBlobCountOnTape(client, BUCKET_NAME);
        LOG.info("Blobs on tape after PUT: " + blobsOnTape);
        assertEquals(OBJECT_COUNT, blobsOnTape, "All blobs should be persisted to tape");

        // --- Phase 2: Add persistence rule to trigger IOM ---
        final long iomStartTime = System.currentTimeMillis();
        LOG.info("Phase 2: Adding second copy persistence rule to trigger IOM...");

        final int completedJobsBeforeIOM = getCompletedJobs(client).size();
        addSecondCopyPersistenceRule();
        LOG.info("Second copy persistence rule added: " + newPersistenceRuleId);

        // Wait for IOM jobs to be created
        LOG.info("Waiting for IOM jobs to be created...");
        while (client.getActiveJobsSpectraS3(activeJobsRequest)
                .getActiveJobListResult().getActiveJobs().isEmpty()) {
            if (getCompletedJobs(client).size() > completedJobsBeforeIOM + 1) {
                break;
            }
            TestUtil.sleep(1000);
        }
        LOG.info("IOM jobs detected.");

        // Wait for IOM jobs to complete
        LOG.info("Waiting for IOM jobs to complete...");
        while (!client.getActiveJobsSpectraS3(activeJobsRequest)
                .getActiveJobListResult().getActiveJobs().isEmpty()) {
            List<ActiveJob> activeJobs = client.getActiveJobsSpectraS3(activeJobsRequest)
                    .getActiveJobListResult().getActiveJobs();
            LOG.info("Active IOM jobs: " + activeJobs.size());
            TestUtil.sleep(5000);
        }

        final long iomEndTime = System.currentTimeMillis();
        final long iomDuration = iomEndTime - iomStartTime;
        LOG.info("Phase 2 complete: IOM jobs took " + iomDuration + " ms ("
                + (iomDuration / 1000) + " seconds)");

        // Verify IOM jobs completed
        List<CompletedJob> completedJobs = getCompletedJobs(client);
        boolean hasIOMGetJob = completedJobs.stream()
                .anyMatch(job -> job.getName() != null && job.getName().contains("IOM GET Job"));
        boolean hasIOMPutJob = completedJobs.stream()
                .anyMatch(job -> job.getName() != null && job.getName().contains("IOM PUT Job"));
        assertTrue(hasIOMGetJob, "There should be at least one completed IOM GET Job");
        assertTrue(hasIOMPutJob, "There should be at least one completed IOM PUT Job");

        LOG.info("Completed jobs:");
        for (CompletedJob job : completedJobs) {
            LOG.info("  - " + job.getName() + " (ID: " + job.getId() + ")");
        }

        // --- Summary ---
        final long totalDuration = iomEndTime - testStartTime;
        LOG.info("=== Performance Summary ===");
        LOG.info("Object count:    " + OBJECT_COUNT);
        LOG.info("Object size:     " + OBJECT_SIZE + " bytes (5 KB)");
        LOG.info("PUT duration:    " + putDuration + " ms (" + (putDuration / 1000) + " seconds)");
        LOG.info("IOM duration:    " + iomDuration + " ms (" + (iomDuration / 1000) + " seconds)");
        LOG.info("Total duration:  " + totalDuration + " ms (" + (totalDuration / 1000) + " seconds)");
        LOG.info("===========================");
    }
}
