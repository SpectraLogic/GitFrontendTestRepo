package com.spectralogic.integrations.s3target;

import com.spectralogic.ds3client.Ds3Client;
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

// Mark a single S3 blob suspect in the DB. With single-copy data policy the one suspect blob has
// no surviving source, so IOM must NOT create recovery jobs for it. The remaining four blobs
// still have a valid S3 source.
@Tag("LocalDevelopment")
@Tag("iomtest")
public class S3SingleBlobSuspectNoIOMTest {
    private Ds3Client client;
    final String bucketName = "gets3bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger(S3SingleBlobSuspectNoIOMTest.class);
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
    public void testSingleSuspectBlobNoIOM() throws IOException, InterruptedException {
        LOG.info("Starting test : S3SingleBlobSuspectNoIOMTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            updateUserDataPolicy(client);

            helper.ensureBucketExists(bucketName);
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputFilePath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputFilePath);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            UUID currentJobId = job.getJobId();
            addJobName(client, "S3SingleBlobSuspectNoIOMTest", currentJobId);

            job.transfer(new FileObjectPutter(inputFilePath));

            isJobCompleted(client, currentJobId);

            // Reclaim cache BEFORE marking suspect. Once the only durable copy is suspect, the
            // reclaimer refuses to evict, so cache would never empty.
            reclaimCache(client);

            // Mark a single S3 blob as suspect.
            markS3BlobSuspect(1);

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

            // Reading all blobs is expected to fail because the one suspect blob has no source.
            // The other four blobs are still readable on S3, so only the suspect one is
            // unrecoverable.
            try {
                final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
                addJobName(client, "ReadS3SingleBlobSuspectNoIOMTest", readJob.getJobId());
                CompletableFuture.runAsync(() -> {
                    try {
                        readJob.transfer(new FileObjectGetter(outputPathFiles));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }).get(2, TimeUnit.MINUTES);
                Assertions.fail("Read should have been refused because one S3 blob is suspect with no other source");
            } catch (Exception e) {
                LOG.info("Expected failure while starting/running read: " + e);
            }

            // Give the IOM driver a few cycles to potentially enqueue recovery jobs.
            TestUtil.sleep(5000);

            GetSuspectBlobS3TargetsSpectraS3Request getSuspectBlobS3TargetsSpectraS3Request = new GetSuspectBlobS3TargetsSpectraS3Request();
            GetSuspectBlobS3TargetsSpectraS3Response getSuspectBlobS3TargetsSpectraS3Response = client.getSuspectBlobS3TargetsSpectraS3(getSuspectBlobS3TargetsSpectraS3Request);
            List<SuspectBlobS3Target> suspectBlobS3Targets = getSuspectBlobS3TargetsSpectraS3Response.getSuspectBlobS3TargetListResult().getSuspectBlobS3Targets();
            assertNotNull(suspectBlobS3Targets, "Suspect blobs exist");
            assertEquals(1, suspectBlobS3Targets.size(), "Exactly one S3 blob was marked suspect");

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3(request);
            // Read job creation was refused (one blob has no source) and IOM must not enqueue
            // recovery jobs for the suspect blob either.
            assertEquals(0, activeJobsResponse.getActiveJobListResult().getActiveJobs().size(),
                    "IOM must not create recovery jobs for a suspect blob with no other source");

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
