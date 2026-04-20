/**
 * This test sets up a BlackPearl with an S3 cloud target, writes data to tape,
 * corrupts the data on the S3 cloud, marks the first tape blob as suspect,
 * verifies the S3 target with full details, then waits for IOM job pairs
 * to recover the data, and finally verifies all objects in the bucket.
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
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;


import static com.spectralogic.integrations.CloudUtils.createLocalStackClient;
import static com.spectralogic.integrations.CloudUtils.reduceSizeS3Object;
import static com.spectralogic.integrations.DatabaseUtils.markBlobSuspectForTape;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/*
  Test steps:
  1. Set up BlackPearl with S3 cloud target
 */
@Tag("LocalDevelopment")
@Tag("iomtest")
public class S3CloudCorruptionIOMSuspectTest {
    private static final Logger LOG = Logger.getLogger(S3CloudCorruptionIOMSuspectTest.class);
    final String bucketName = "test-bucket";
    String inputPath = "testFiles";
    private Ds3Client client;

    public S3Target setupS3CloudTarget(Ds3Client client) throws IOException {
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

    private UUID getObjectId (List<BulkObject> details, String key) {
        for (BulkObject detail : details) {
            if (key.contains(detail.getId().toString())) {
                return detail.getId();
            }
        }
        return null;
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    public void testS3CloudCorruptionWithTapeSuspectIOM() throws IOException, InterruptedException {
        LOG.info("Starting test: S3CloudCorruptionIOMSuspectTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            // Step 1: Set up BlackPearl with S3 cloud target
            S3Target s3Target = setupS3CloudTarget(client);
            LOG.info("S3 cloud target registered: " + s3Target.getName());

            // Step 2: Create bucket with data policy "Single Copy on Tape"
            helper.ensureBucketExists(bucketName);

            // Step 3: Create a Put job (startWrite) for the bucket
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputFilePath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputFilePath);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputFilePath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "S3CloudCorruptionIOMSuspectTest", currentJobId);
            LOG.info("Put job started: " + currentJobId);

            // Step 4: Wait for the put job to complete
            isJobCompleted(client, currentJobId);
            waitForJobsToComplete(client);
            LOG.info("Put job completed. All jobs finished.");

            // Verify blobs are on tape
            assertEquals(5, getBlobCountOnTape(client, bucketName), "Expected 5 blobs on tape");

            // Get bucket contents and verify object count
            final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, bucketResponse.getListBucketResult().getObjects().size(), "Expected 5 objects in bucket");

            // Build object list for placement verification
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : bucketResponse.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            // Verify physical placement: each object should be on tape and S3
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

            // Step 5: Corrupt the data on the S3 cloud
            S3Client localStackClient = createLocalStackClient();
            assertNotNull(localStackClient, "LocalStack S3 client should not be null");

            // Find the S3 bucket name used by the target
            GetS3TargetsSpectraS3Response s3TargetsResponse =
                    client.getS3TargetsSpectraS3(new GetS3TargetsSpectraS3Request());
            List<S3Target> s3Targets = s3TargetsResponse.getS3TargetListResult().getS3Targets();
            assertFalse(s3Targets.isEmpty(), "Should have at least one S3 target");


            // Corrupt an object on the S3 cloud target
            String key = reduceSizeS3Object(localStackClient, bucketName);
            LOG.info("S3 object corrupted on cloud target.");
            UUID blobId = getObjectId(details, key);


            Optional<UUID> firstTapeId = tapeIds.stream().findFirst();
            assertTrue(firstTapeId.isPresent(), "Should have at least one tape ID");

            try {
                markBlobSuspectForTape(firstTapeId.get(),blobId);
                LOG.info("Marked blobs as suspect for tape: " + firstTapeId.get());
                System.gc();
                TestUtil.sleep(3000);
            } catch (Exception e) {
                LOG.warn("Error marking blob as suspect, but continuing: " + e.getMessage(), e);
                System.gc();
                TestUtil.sleep(1000);
            }

            // Step 7: Verify S3 target with full details
            VerifyS3TargetSpectraS3Request verifyRequest =
                    new VerifyS3TargetSpectraS3Request(s3Target.getName());
            verifyRequest.withFullDetails(true);
            VerifyS3TargetSpectraS3Response verifyResponse =
                    client.verifyS3TargetSpectraS3(verifyRequest);
            S3Target verifiedTarget = verifyResponse.getS3TargetResult();
            LOG.info("S3 target verified. State: " + verifiedTarget.getState());

            // Step 8: Wait a maximum of 600 seconds for IOM job pair to appear
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

            LOG.info("IOM Jobs created. Waiting for them to complete...");

            // Step 9: Wait for all jobs to complete
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

            // Verify IOM GET and PUT job pair were created
            List<CompletedJob> completedJobs = getCompletedJobs(client);
            boolean hasIOMGetJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM GET Job"));
            boolean hasIOMPutJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM PUT Job"));
            assertTrue(hasIOMGetJob, "Should have an IOM GET Job");
            assertTrue(hasIOMPutJob, "Should have an IOM PUT Job");
            LOG.info("IOM jobs completed successfully.");

            // Step 10: Verify all objects in bucket
            final GetBucketResponse finalBucketResponse = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, finalBucketResponse.getListBucketResult().getObjects().size(),
                    "All 5 objects should still exist in bucket after IOM recovery");

            final List<Ds3Object> finalObjectList = new ArrayList<>();
            for (final Contents contents : finalBucketResponse.getListBucketResult().getObjects()) {
                finalObjectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            // Verify physical placement is intact after recovery
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
            }
            LOG.info("All objects verified in bucket after IOM recovery.");

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
