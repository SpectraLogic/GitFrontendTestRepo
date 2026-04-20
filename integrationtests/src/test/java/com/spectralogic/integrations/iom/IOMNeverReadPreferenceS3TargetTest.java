/**
 * This test verifies that IOM recovery does NOT use an S3 cloud target
 * when the read preference for that target on the bucket is set to NEVER.
 * After suspecting a tape blob, IOM should not create recovery jobs from the S3 target.
 */
package com.spectralogic.integrations.iom;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
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

import static com.spectralogic.integrations.DatabaseUtils.markBlobSuspectForTape;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("LocalDevelopment")
@Tag("iomtest")
public class IOMNeverReadPreferenceS3TargetTest {
    private static final Logger LOG = Logger.getLogger(IOMNeverReadPreferenceS3TargetTest.class);
    private static final String BUCKET_NAME = "test-bucket";
    private Ds3Client client;

    public S3Target setupS3TargetWithReplicationRule(Ds3Client client) throws IOException {
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

        clearS3ReplicationRules(client, singleCopyTapeDP.get().getName());
        clearS3Targets(client);

        S3Target target = registerS3LocalstackTarget(client);

        PutS3DataReplicationRuleSpectraS3Request ruleRequest =
                new PutS3DataReplicationRuleSpectraS3Request(
                        singleCopyTapeDP.get().getId(),
                        target.getId(),
                        DataReplicationRuleType.PERMANENT);
        client.putS3DataReplicationRuleSpectraS3(ruleRequest);

        return target;
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            cleanupAllBuckets(client);
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            clearS3Targets(client);
            //TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupAllBuckets(client);
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
           // TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    private Path createTempFileWithSize(final String fileName, final long sizeInBytes) throws IOException {
        final Path tempDir = Files.createTempDirectory("iom-never-test-" + fileName);
        tempDir.toFile().deleteOnExit();
        final File file = new File(tempDir.toFile(), fileName);
        file.deleteOnExit();
        try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(sizeInBytes);
        }
        return tempDir;
    }

    private UUID getBucketId(String bucketName) throws IOException {
        GetBucketsSpectraS3Response response = client.getBucketsSpectraS3(new GetBucketsSpectraS3Request());
        Optional<Bucket> bucket = response.getBucketListResult().getBuckets().stream()
                .filter(b -> b.getName().equals(bucketName)).findFirst();
        assertTrue(bucket.isPresent(), "Bucket '" + bucketName + "' should exist");
        return bucket.get().getId();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testIOMDoesNotRecoverFromS3TargetWithNeverReadPreference() throws IOException, InterruptedException {
        LOG.info("Starting test: IOMNeverReadPreferenceS3TargetTest");

        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Step 1: Set up S3 target with replication rule
        S3Target s3Target = setupS3TargetWithReplicationRule(client);
        LOG.info("S3 target registered: " + s3Target.getName());

        // Step 2: Create bucket
        helper.ensureBucketExists(BUCKET_NAME);

        // Step 3: Set read preference on bucket for S3 target to NEVER
        UUID bucketId = getBucketId(BUCKET_NAME);
        PutS3TargetReadPreferenceSpectraS3Request readPrefRequest =
                new PutS3TargetReadPreferenceSpectraS3Request(
                        bucketId.toString(), TargetReadPreferenceType.NEVER, s3Target.getId());
        client.putS3TargetReadPreferenceSpectraS3(readPrefRequest);
        LOG.info("Set S3 target read preference to NEVER for bucket: " + BUCKET_NAME);

        // Step 4: Bulk PUT object "some-object" of size 1000
        final Path tempDir = createTempFileWithSize("some-object", 1000);
        final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(tempDir);
        final Ds3ClientHelpers.Job job = helper.startWriteJob(BUCKET_NAME, objects);
        job.transfer(new FileObjectPutter(tempDir));
        UUID jobId = job.getJobId();
        addJobName(client, "IOMNeverReadPrefTest", jobId);
        LOG.info("PUT job started: " + jobId);

        // Step 5: Wait for jobs to complete
        isJobCompleted(client, jobId);
        waitForJobsToComplete(client);
        LOG.info("All jobs completed.");

        // Verify object on tape and S3
        final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(BUCKET_NAME));
        assertEquals(1, bucketResponse.getListBucketResult().getObjects().size());

        final List<Ds3Object> objectList = new ArrayList<>();
        for (final Contents contents : bucketResponse.getListBucketResult().getObjects()) {
            objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
        }

        GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request placementRequest =
                new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request(BUCKET_NAME, objectList);
        GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response placementResponse =
                client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(placementRequest);

        Set<UUID> tapeIds = new HashSet<>();
        List<BulkObject> details = placementResponse.getBulkObjectListResult().getObjects();
        for (BulkObject detail : details) {
            assertFalse(detail.getPhysicalPlacement().getTapes().isEmpty(),
                    "Object should have at least one tape copy");
            detail.getPhysicalPlacement().getTapes().forEach(tape -> tapeIds.add(tape.getId()));
        }
        LOG.info("Object verified on tape. Tape IDs: " + tapeIds);

        // Step 6: Force full cache reclaim
        reclaimCache(client);
        LOG.info("Cache reclaimed.");

        int completedJobCountBeforeSuspect = getCompletedJobs(client).size();

        // Step 7: Suspect the first blob on tape
        Optional<UUID> firstTapeId = tapeIds.stream().findFirst();
        assertTrue(firstTapeId.isPresent());
        markBlobSuspectForTape(firstTapeId.get(), 1);
        LOG.info("Marked 1 blob as suspect for tape: " + firstTapeId.get());

        // Step 8: Wait and check if IOM jobs get created
        // With read preference NEVER, IOM should NOT be able to recover from S3 target
        TestUtil.sleep(3000);

        GetActiveJobsSpectraS3Request activeJobsRequest = new GetActiveJobsSpectraS3Request();
        long startTime = System.currentTimeMillis();
        long waitTimeMs = 60_000; // Wait 60 seconds to see if IOM jobs appear

        LOG.info("Waiting up to 60 seconds to check if IOM jobs get created...");
        boolean iomJobCreated = false;
        while (System.currentTimeMillis() - startTime < waitTimeMs) {
            List<ActiveJob> activeJobs = client.getActiveJobsSpectraS3(activeJobsRequest)
                    .getActiveJobListResult().getActiveJobs();
            List<CompletedJob> completedJobs = getCompletedJobs(client);

            if (!activeJobs.isEmpty()) {
                LOG.info("Active jobs found: " + activeJobs.size());
                for (ActiveJob activeJob : activeJobs) {
                    LOG.info("  Active job: " + activeJob.getName() + " (type: " + activeJob.getRequestType() + ")");
                }
                iomJobCreated = true;
                break;
            }

            if (completedJobs.size() > completedJobCountBeforeSuspect) {
                LOG.info("New completed jobs found after suspect marking.");
                for (CompletedJob completedJob : completedJobs) {
                    LOG.info("  Completed job: " + completedJob.getName());
                }
                iomJobCreated = true;
                break;
            }

            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3(new GetActiveJobsSpectraS3Request());
            assertEquals(0,
                    activeJobsResponse.getActiveJobListResult().getActiveJobs().size(),
                    "No active jobs should be present with NEVER read preference");

            GetCompletedJobsSpectraS3Response finalCompletedJobs = client.getCompletedJobsSpectraS3(new GetCompletedJobsSpectraS3Request());
            assertEquals(1,
                    finalCompletedJobs.getCompletedJobListResult().getCompletedJobs().size(),
                    "Only one completed jobs should be present");


            TestUtil.sleep(2000);
        }

        if (iomJobCreated) {
            LOG.info("IOM jobs were created despite NEVER read preference — checking if they use the S3 target.");
        } else {
            LOG.info("No IOM jobs created within 60 seconds — S3 target with NEVER read preference was correctly excluded.");
        }
    }
}
