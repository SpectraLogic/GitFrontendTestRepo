package com.spectralogic.integrations.s3nativetarget.datacorruption;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.CloudUtils.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.addJobName;
import static com.spectralogic.integrations.Ds3ApiHelpers.reclaimCache;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.*;

/*
    Test steps:
    1. Set up BlackPearl with an S3 native cloud target.
    2. Create a bucket with the data policy "Single Copy on Tape" and complete a PUT job.
    3. Clear cache and quiesce tape partitions.
    4. Corrupt an object on the S3 cloud target with the same size as the original object.
    5. Attempt reading from the cloud bucket.
    6. Read should fail. Corrupted blob should be marked suspect and IOM jobs should be created.

 */

public class GetFromNativeS3SameSizeCorruptionTest {
    private static final Logger LOG = Logger.getLogger(GetFromNativeS3SameSizeCorruptionTest.class);
    final String bucketName = "test-bucket";
    String inputPath = "testFiles";
    private Ds3Client client;
    S3Client localStackClient = createLocalStackClient();


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

        S3Target target = registerS3NativeLocalstackTarget(client);

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
            deleteAllBuckets(localStackClient);
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            clearS3Targets(client);
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            deleteAllBuckets(localStackClient);
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            TestUtils.cleanSetUp(client);
            clearS3Targets(client);
            getTapesReady(client);
            client.close();
        }
    }



    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    public void testS3CloudCorruption() throws IOException, InterruptedException {
        LOG.info("Starting test: GetFromNativeS3SameSizeCorruptionTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            S3Target s3Target = setupS3CloudTarget(client);
            LOG.info("S3 cloud target registered: " + s3Target.getName());

            helper.ensureBucketExists(bucketName);

            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputFilePath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputFilePath);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputFilePath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "GetFromNativeS3SameSizeCorruptionTest", currentJobId);
            LOG.info("Put job started: " + currentJobId);

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

            assertNotNull(localStackClient, "LocalStack S3 client should not be null");

            // Find the S3 bucket name used by the target
            GetS3TargetsSpectraS3Response s3TargetsResponse =
                    client.getS3TargetsSpectraS3(new GetS3TargetsSpectraS3Request());
            List<S3Target> s3Targets = s3TargetsResponse.getS3TargetListResult().getS3Targets();
            assertFalse(s3Targets.isEmpty(), "Should have at least one S3 target");

            LOG.info("Clear cache and quiesce tape partitions.");
            quiescePartitions(client);
            reclaimCache(client);

            // Corrupt an object on the S3 cloud target
            corruptS3ObjectSameSize(localStackClient, bucketName);
            LOG.info("S3 object corrupted on cloud target.");



            String outputPath = "testFilesOutput";
            final URL resourcesUrl = getClass().getClassLoader().getResource("");
            assert resourcesUrl != null;


            final Path resourcesPath = Paths.get(resourcesUrl.toURI());
            final Path outputPathFiles = resourcesPath.resolve(outputPath);


            //Output will be created in the build/classes
            try {
                Files.createDirectories(outputPathFiles);
                System.out.println("Created output directory: " + outputPathFiles.toAbsolutePath());
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + e.getMessage());
                throw new RuntimeException("Directory setup failed.", e);
            }
            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
            UUID readJobId = readJob.getJobId();

            addJobName(client, "ReadS3Native", readJobId);
            try{
                java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try {
                        readJob.transfer(new FileObjectGetter(outputPathFiles));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).get(2, TimeUnit.MINUTES);
            } catch (Exception e) {
                System.out.println("Error executing read" + e);
                GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
                GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3(request);
                assertEquals(3, activeJobsResponse.getActiveJobListResult().getActiveJobs().size());
            }

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
