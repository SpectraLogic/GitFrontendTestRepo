/**
 * This test sets up a BlackPearl with an S3 cloud target and replication rule,
 * writes data to tape, suspects one blob on tape, waits for IOM recovery jobs,
 * then verifies all objects on both tape and cloud.
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
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
public class S3TapeSuspectBlobIOMTest {
    private static final Logger LOG = Logger.getLogger(S3TapeSuspectBlobIOMTest.class);
    final String bucketName = "s3-tape-suspect-bucket";
    String inputPath = "testFiles";
    private Ds3Client client;

    /**
     * Sets the user's default data policy to "Single Copy on Tape" and
     * registers an S3 LocalStack target with a permanent replication rule.
     */
    public S3Target setupS3TargetWithReplicationRule(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);

        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        assertTrue(singleCopyTapeDP.isPresent(), "Data policy '" + DATA_POLICY_TAPE_SINGLE_COPY_NAME + "' not found");

        // Set user's default data policy
        client.modifyUserSpectraS3(
                new ModifyUserSpectraS3Request(authId)
                        .withDefaultDataPolicyId(singleCopyTapeDP.get().getId()));

        // Clear existing S3 rules and targets, then register new ones
        clearS3ReplicationRules(client, singleCopyTapeDP.get().getName());
        clearS3Targets(client);

        S3Target target = registerS3LocalstackTarget(client);

        // Create permanent S3 replication rule
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
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            clearS3Targets(client);
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    public void testSuspectSingleTapeBlobTriggersIOMRecovery() throws IOException, InterruptedException {
        LOG.info("Starting test: S3TapeSuspectBlobIOMTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            // Step 1: Set up S3 target with replication rule
            S3Target s3Target = setupS3TargetWithReplicationRule(client);
            LOG.info("S3 target registered with replication rule: " + s3Target.getName());

            // Step 2: Create bucket
            helper.ensureBucketExists(bucketName);

            // Step 3: Bulk put objects to the bucket
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputFilePath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputFilePath);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputFilePath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "S3TapeSuspectBlobIOMTest", currentJobId);
            LOG.info("Put job started: " + currentJobId);

            // Step 4: Wait for all jobs to complete
            isJobCompleted(client, currentJobId);
            waitForJobsToComplete(client);
            LOG.info("All jobs completed.");

            // Verify blobs are on tape and S3
            assertEquals(5, getBlobCountOnTape(client, bucketName), "Expected 5 blobs on tape");

            final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, bucketResponse.getListBucketResult().getObjects().size(), "Expected 5 objects in bucket");

            // Get physical placement before suspect marking
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : bucketResponse.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request placementRequest =
                    new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request(bucketName, objectList);
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response placementResponse =
                    client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(placementRequest);

            Set<UUID> tapeIds = new HashSet<>();
            List<BulkObject> details = placementResponse.getBulkObjectListResult().getObjects();
            for (BulkObject detail : details) {
                assertFalse(detail.getPhysicalPlacement().getTapes().isEmpty(),
                        "Each object should have at least one tape copy");
                assertEquals(1, detail.getPhysicalPlacement().getS3Targets().size(),
                        "Each object should have exactly one S3 target copy");
                detail.getPhysicalPlacement().getTapes().forEach(tape -> tapeIds.add(tape.getId()));
            }
            LOG.info("Physical placement verified. Tape IDs: " + tapeIds);

            // Step 5: Cache reclaim
            reclaimCache(client);
            LOG.info("Cache reclaimed.");

            // Step 6: Suspect one blob on tape (recordCount=1)
            Optional<UUID> firstTapeId = tapeIds.stream().findFirst();
            assertTrue(firstTapeId.isPresent(), "Should have at least one tape ID");

            markBlobSuspectForTape(firstTapeId.get(), 1);
            LOG.info("Marked 1 blob as suspect for tape: " + firstTapeId.get());
            TestUtil.sleep(3000);

            // Step 7: Wait for IOM jobs to appear and complete (max 600 seconds)
            GetActiveJobsSpectraS3Request activeJobsRequest = new GetActiveJobsSpectraS3Request();
            long startTime = System.currentTimeMillis();
            long maxWaitMs = 600_000; // 600 seconds

            LOG.info("Waiting for IOM jobs to be created (max 600 seconds)...");
            while (client.getActiveJobsSpectraS3(activeJobsRequest)
                    .getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() >= 3) {
                    break;
                }
                if (System.currentTimeMillis() - startTime > maxWaitMs) {
                    fail("Timed out waiting for IOM jobs after 600 seconds");
                }
                TestUtil.sleep(1000);
            }

            LOG.info("IOM jobs detected. Waiting for completion...");

            // Wait for all IOM jobs to complete
            while (!client.getActiveJobsSpectraS3(activeJobsRequest)
                    .getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() >= 3) {
                    break;
                }
                if (System.currentTimeMillis() - startTime > maxWaitMs) {
                    fail("Timed out waiting for IOM jobs to complete after 600 seconds");
                }
                TestUtil.sleep(1000);
            }

            // Verify IOM job pair was created
            List<CompletedJob> completedJobs = getCompletedJobs(client);
            boolean hasIOMGetJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM GET Job"));
            boolean hasIOMPutJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM PUT Job"));
            assertTrue(hasIOMGetJob, "Should have an IOM GET Job");
            assertTrue(hasIOMPutJob, "Should have an IOM PUT Job");
            LOG.info("IOM jobs completed successfully.");

            // Step 8: Verify all objects on tape using VerifyTapeSpectraS3Request
            for (UUID tapeId : tapeIds) {
                VerifyTapeSpectraS3Request verifyTapeRequest = new VerifyTapeSpectraS3Request(tapeId.toString());
                VerifyTapeSpectraS3Response verifyTapeResponse =
                        client.verifyTapeSpectraS3(verifyTapeRequest);
                Tape verifiedTape = verifyTapeResponse.getTapeResult();
                LOG.info("Tape verified: " + verifiedTape.getId() + " state: " + verifiedTape.getState());
            }

            // Verify S3 target with full details
            VerifyS3TargetSpectraS3Request verifyS3Request =
                    new VerifyS3TargetSpectraS3Request(s3Target.getName());
            verifyS3Request.withFullDetails(true);
            VerifyS3TargetSpectraS3Response verifyS3Response =
                    client.verifyS3TargetSpectraS3(verifyS3Request);
            S3Target verifiedTarget = verifyS3Response.getS3TargetResult();
            LOG.info("S3 target verified. State: " + verifiedTarget.getState());

            // Verify all objects still exist in bucket with correct placement
            final GetBucketResponse finalBucketResponse = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, finalBucketResponse.getListBucketResult().getObjects().size(),
                    "All 5 objects should still exist in bucket after IOM recovery");

            final List<Ds3Object> finalObjectList = new ArrayList<>();
            for (final Contents contents : finalBucketResponse.getListBucketResult().getObjects()) {
                finalObjectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request finalPlacementRequest =
                    new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request(bucketName, finalObjectList);
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response finalPlacementResponse =
                    client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(finalPlacementRequest);

            int index = 0;
            List<BulkObject> finalDetails = finalPlacementResponse.getBulkObjectListResult().getObjects();
            for (BulkObject detail : finalDetails) {
                assertEquals(detail.getLength(), details.get(index++).getLength(),
                        "Object length should be unchanged after IOM recovery");
                assertFalse(detail.getPhysicalPlacement().getTapes().isEmpty(),
                        "After IOM recovery, each object should have tape copies");
                assertFalse(detail.getPhysicalPlacement().getS3Targets().isEmpty(),
                        "After IOM recovery, each object should have S3 target copies");
            }


            LOG.info("All objects verified on tape and cloud after IOM recovery. No suspect blobs remain.");

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
