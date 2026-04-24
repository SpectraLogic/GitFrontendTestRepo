package com.spectralogic.integrations.s3target.datacorruption;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.CloudUtils;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.markS3BlobSuspect;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_S3_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

// Mark all S3 blobs suspect in the DB. With single-copy data policy there is no
// surviving source, so IOM must NOT create recovery jobs. Only the failed read should
// remain active.
@Tag("LocalDevelopment")
@Tag("iomtest")
public class S3MarkSuspectNoIOMTest {
    private Ds3Client client;
    final String bucketName = "gets3bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( S3MarkSuspectNoIOMTest.class );
    S3Target target;
    S3Client s3Client = CloudUtils.createLocalStackClient();
    UUID dataPolicyId;

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertFalse(dataPolicies.isEmpty());

        Optional<DataPolicy> singleCopyS3DP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_S3_SINGLE_COPY_NAME)).findFirst();

        if (!singleCopyS3DP.isPresent()) {
            DataPolicy dp = createDataPolicy(client, DATA_POLICY_S3_SINGLE_COPY_NAME);
            dataPolicyId = dp.getId();
        } else {
            dataPolicyId = singleCopyS3DP.get().getId();
        }

        target = registerS3LocalstackTarget(client);
        PutS3DataReplicationRuleSpectraS3Request putS3DataReplicationRuleSpectraS3Request =
                new PutS3DataReplicationRuleSpectraS3Request(dataPolicyId, target.getId(), DataReplicationRuleType.PERMANENT);
        client.putS3DataReplicationRuleSpectraS3(putS3DataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicyId));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        CloudUtils.deleteAllBuckets(s3Client);
        client = TestUtils.setTestParams();
        if (client != null) {
            cleanupBuckets(client, bucketName);
            clearS3ReplicationRules(client, DATA_POLICY_S3_SINGLE_COPY_NAME);
            clearS3Targets(client);
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupBuckets(client, bucketName);
            clearS3ReplicationRules(client, DATA_POLICY_S3_SINGLE_COPY_NAME);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 25, unit = TimeUnit.MINUTES)
    public void testMarkSuspectNoIOM() throws IOException, InterruptedException {
        LOG.info("Starting test : S3MarkSuspectNoIOMTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            updateUserDataPolicy(client);

            helper.ensureBucketExists(bucketName);
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            UUID currentJobId = job.getJobId();
            // Set the job name while the job is still active. Doing this after transfer can race
            // with DataPlanner's completed-job cleanup and 404.
            addJobName(client, "S3MarkSuspectNoIOMTest", currentJobId);

            job.transfer(new FileObjectPutter(inputPath));

            isJobCompleted(client, currentJobId);

            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            // Reclaim cache BEFORE marking suspect. Once the only durable copy is suspect, the
            // reclaimer conservatively refuses to evict, so cache would never empty.
            reclaimCache(client);

            // Mark S3 blobs as suspect directly in the database.
            markS3BlobSuspect();

            final URL resourcesUrl = getClass().getClassLoader().getResource("");
            assert resourcesUrl != null;

            String outputPath = "testFilesOutput";
            final Path resourcesPath = Paths.get(resourcesUrl.toURI());
            final Path outputPathFiles = resourcesPath.resolve(outputPath);

            try {
                Files.createDirectories(outputPathFiles);
            } catch (IOException e) {
                throw new RuntimeException("Directory setup failed.", e);
            }

            // Cache is empty and all S3 placements are suspect, so the strategy must refuse to
            // create the GET job.

            try {
                final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
                addJobName(client, "ReadS3MarkSuspectNoIOMTest", readJob.getJobId());
                CompletableFuture.runAsync(() -> {
                    try {
                        readJob.transfer(new FileObjectGetter(outputPathFiles));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get(2, TimeUnit.MINUTES);
                Assertions.fail("Read should have been refused because all S3 blobs are suspect");
            } catch (Exception e) {
                LOG.info("Expected failure while starting/running read: " + e);
            }

            // Give the IOM driver a few cycles to potentially enqueue recovery jobs.
            TestUtil.sleep(5000);

            GetSuspectBlobS3TargetsSpectraS3Request getSuspectBlobS3TargetsSpectraS3Request = new GetSuspectBlobS3TargetsSpectraS3Request();
            GetSuspectBlobS3TargetsSpectraS3Response getSuspectBlobS3TargetsSpectraS3Response = client.getSuspectBlobS3TargetsSpectraS3(getSuspectBlobS3TargetsSpectraS3Request);
            List<SuspectBlobS3Target> suspectBlobS3Targets = getSuspectBlobS3TargetsSpectraS3Response.getSuspectBlobS3TargetListResult().getSuspectBlobS3Targets();
            assertNotNull(suspectBlobS3Targets, "Suspect blobs exist");

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3(request);
            // Read job creation was refused (no readable source) and IOM must not enqueue recovery
            // jobs either, so no jobs should remain active.
            assertEquals(0, activeJobsResponse.getActiveJobListResult().getActiveJobs().size(),
                    "IOM must not create recovery jobs when the only source is suspect");

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
