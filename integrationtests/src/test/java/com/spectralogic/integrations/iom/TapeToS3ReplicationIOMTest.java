/**
 * Put 50 objects to a tape-only data policy, delete 25 of them directly from the database, then
 * register an S3 target and add a PERMANENT S3 replication rule. IOM should migrate the remaining
 * 25 tape-resident blobs to the new S3 target.
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.deleteObjectsFromBucket;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("LocalDevelopment")
@Tag("iomtest")
public class TapeToS3ReplicationIOMTest {
    private static final Logger LOG = Logger.getLogger(TapeToS3ReplicationIOMTest.class);
    final String bucketName = "tape-to-s3-replication-bucket";
    private static final int TOTAL_OBJECTS = 50;
    private static final int DELETE_COUNT = 25;
    private static final int REMAINING_OBJECTS = TOTAL_OBJECTS - DELETE_COUNT;
    private Ds3Client client;
    private Path generatedFilesDir;

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
        if (generatedFilesDir != null) {
            deleteDirectoryRecursively(generatedFilesDir);
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    public void testAddingS3ReplicationRuleTriggersIOMForRemainingObjects() throws IOException, InterruptedException {
        LOG.info("Starting test: TapeToS3ReplicationIOMTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            // Step 1: Set the user's default data policy to "Single Copy on Tape" — no S3 yet.
            final GetDataPoliciesSpectraS3Response responsePolicy =
                    client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
            List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
            Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                    .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
            assertTrue(singleCopyTapeDP.isPresent(), "Data policy '" + DATA_POLICY_TAPE_SINGLE_COPY_NAME + "' not found");

            final UUID dataPolicyId = singleCopyTapeDP.get().getId();
            client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicyId));

            // Step 2: Create bucket.
            helper.ensureBucketExists(bucketName);

            // Step 3: Generate 50 files and PUT them.
            generatedFilesDir = createGeneratedFiles(TOTAL_OBJECTS);
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(generatedFilesDir);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            UUID currentJobId = job.getJobId();
            addJobName(client, "TapeToS3ReplicationIOMTest", currentJobId);

            job.transfer(new FileObjectPutter(generatedFilesDir));

            isJobCompleted(client, currentJobId);
            waitForJobsToComplete(client);
            LOG.info("PUT job completed for " + TOTAL_OBJECTS + " objects.");

            assertEquals(TOTAL_OBJECTS, getBlobCountOnTape(client, bucketName),
                    "Expected " + TOTAL_OBJECTS + " blobs on tape after PUT");

            reclaimCache(client);
            // Step 4: Delete 25 objects directly from the database.
            deleteObjectsFromBucket(bucketName, DELETE_COUNT);
            LOG.info("Deleted " + DELETE_COUNT + " objects from database.");

            final GetBucketResponse afterDeleteResponse = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(REMAINING_OBJECTS, afterDeleteResponse.getListBucketResult().getObjects().size(),
                    "Expected " + REMAINING_OBJECTS + " objects remaining in bucket");

            // Step 5: Register S3 target and add PERMANENT replication rule.
            S3Target s3Target = registerS3LocalstackTarget(client);
            PutS3DataReplicationRuleSpectraS3Request ruleRequest =
                    new PutS3DataReplicationRuleSpectraS3Request(
                            dataPolicyId, s3Target.getId(), DataReplicationRuleType.PERMANENT);
            client.putS3DataReplicationRuleSpectraS3(ruleRequest);
            LOG.info("Registered S3 target and added PERMANENT replication rule.");

            // Step 6: Wait for IOM jobs to appear and complete.
            final GetActiveJobsSpectraS3Request activeJobsRequest = new GetActiveJobsSpectraS3Request();
            final long startTime = System.currentTimeMillis();
            final long maxWaitMs = 600_000;

            LOG.info("Waiting for IOM jobs to be created (max 600 seconds)...");
            while (client.getActiveJobsSpectraS3(activeJobsRequest)
                    .getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (hasIOMCompletedJobs(client)) {
                    break;
                }
                if (System.currentTimeMillis() - startTime > maxWaitMs) {
                    fail("Timed out waiting for IOM jobs to be created after 600 seconds");
                }
                TestUtil.sleep(1000);
            }
            LOG.info("IOM jobs detected. Waiting for completion...");

            while (!client.getActiveJobsSpectraS3(activeJobsRequest)
                    .getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (System.currentTimeMillis() - startTime > maxWaitMs) {
                    fail("Timed out waiting for IOM jobs to complete after 600 seconds");
                }
                TestUtil.sleep(1000);
            }

            // Step 7: Verify IOM GET + PUT job pair was created for the remaining 25 blobs.
            List<CompletedJob> completedJobs = getCompletedJobs(client);
            boolean hasIOMGetJob = completedJobs.stream()
                    .anyMatch(j -> j.getName().contains("IOM GET Job"));
            boolean hasIOMPutJob = completedJobs.stream()
                    .anyMatch(j -> j.getName().contains("IOM PUT Job"));
            assertTrue(hasIOMGetJob, "Should have an IOM GET Job for the remaining " + REMAINING_OBJECTS + " blobs");
            assertTrue(hasIOMPutJob, "Should have an IOM PUT Job replicating to the new S3 target");

            // Step 8: Verify the 25 remaining blobs now have placements on BOTH tape and the new S3 target.
            final GetBucketResponse finalBucketResponse = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(REMAINING_OBJECTS, finalBucketResponse.getListBucketResult().getObjects().size(),
                    REMAINING_OBJECTS + " objects should still exist after IOM replication");

            final List<Ds3Object> finalObjectList = new ArrayList<>();
            for (final Contents contents : finalBucketResponse.getListBucketResult().getObjects()) {
                finalObjectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request finalPlacementRequest =
                    new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request(bucketName, finalObjectList);
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response finalPlacementResponse =
                    client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(finalPlacementRequest);

            List<BulkObject> finalDetails = finalPlacementResponse.getBulkObjectListResult().getObjects();
            assertEquals(REMAINING_OBJECTS, finalDetails.size(),
                    "Placement response should cover " + REMAINING_OBJECTS + " blobs");
            for (BulkObject detail : finalDetails) {
                assertFalse(detail.getPhysicalPlacement().getTapes().isEmpty(),
                        "Each remaining blob should still have a tape placement");
                assertFalse(detail.getPhysicalPlacement().getS3Targets().isEmpty(),
                        "Each remaining blob should have a new S3 target placement after IOM replication");
            }

            LOG.info("IOM replication of " + REMAINING_OBJECTS + " blobs to new S3 target verified.");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean hasIOMCompletedJobs(Ds3Client client) throws IOException {
        return getCompletedJobs(client).stream()
                .anyMatch(j -> j.getName().contains("IOM GET Job") || j.getName().contains("IOM PUT Job"));
    }

    private static Path createGeneratedFiles(int count) throws IOException {
        Path dir = Files.createTempDirectory("tape-to-s3-replication-test-");
        for (int i = 0; i < count; i++) {
            Path file = dir.resolve(String.format("obj-%03d.txt", i));
            Files.write(file, ("generated test content " + i).getBytes());
        }
        return dir;
    }

    private static void deleteDirectoryRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                    }
                });
    }
}
