package com.spectralogic.integrations.jobs;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.integrations.TestUtils;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static org.junit.jupiter.api.Assertions.*;

@Tag("LocalDevelopment")
public class BulkGetPreInitiatedTest {
    private final String bucketName = "bulk_get_pre_initiated_bucket";
    private final static Logger LOG = Logger.getLogger(BulkGetPreInitiatedTest.class);
    private Ds3Client client;

    private void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        modifyUser(client, singleCopyTapeDP.get());
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
            // Always revert allowNewJobRequests back to true
            ModifyDataPathBackendSpectraS3Request revertRequest = new ModifyDataPathBackendSpectraS3Request();
            revertRequest.withAllowNewJobRequests(true);
            client.modifyDataPathBackendSpectraS3(revertRequest);

            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testPreInitiatedBulkGetCompletesAfterBlockingNewJobs() throws IOException, InterruptedException {
        LOG.info("Starting test : BulkGetPreInitiatedTest - testPreInitiatedBulkGetCompletesAfterBlockingNewJobs");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
        updateUserDataPolicy(client);

        // Create a bucket and put an object
        helper.ensureBucketExists(bucketName);

        final Path tempFilePath = Files.createTempFile("ds3test", "upload.txt");
        final Path tempFilePathOutput = Files.createTempFile("ds3test", "output.txt");
        tempFilePath.toFile().deleteOnExit();
        tempFilePathOutput.toFile().deleteOnExit();

        final byte[] content = "0123456789\n".getBytes();
        final long objectSize = content.length;
        Files.write(tempFilePath, content, StandardOpenOption.CREATE);

        try (FileChannel readChannel = FileChannel.open(tempFilePath, StandardOpenOption.READ)) {
            PutObjectRequest putRequest = new PutObjectRequest(bucketName, "TestObject", readChannel, objectSize);
            client.putObject(putRequest);
        }

        // Wait for the PUT job to complete
        waitForJobsToComplete(client);

        // Initiate a bulk GET job BEFORE disabling new job requests
        final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
        assertNotNull(readJob, "Read job should be created successfully");
        assertNotNull(readJob.getJobId(), "Read job should have a valid job ID");
        LOG.info("Bulk GET job initiated with jobId: " + readJob.getJobId());

        // Now disable new job requests
        ModifyDataPathBackendSpectraS3Request modifyRequest = new ModifyDataPathBackendSpectraS3Request();
        modifyRequest.withAllowNewJobRequests(false);
        client.modifyDataPathBackendSpectraS3(modifyRequest);
        LOG.info("Disabled allowNewJobRequests");

        // Transfer data for the pre-initiated GET job — should succeed
        FileChannel outputChannel = FileChannel.open(
                tempFilePathOutput,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, "TestObject", outputChannel);
        getObjectRequest.withOffset(0);
        getObjectRequest.withJob(readJob.getJobId());
        client.getObject(getObjectRequest);

        // Verify the retrieved data matches the original
        assertEquals(-1, Files.mismatch(tempFilePathOutput, tempFilePath), "Retrieved file should match the uploaded file");
        LOG.info("Pre-initiated bulk GET job completed successfully with data transfer after blocking new jobs");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testNewBulkGetFailsWhileExistingGetInProgressAndJobCreationBlocked() throws IOException, InterruptedException {
        LOG.info("Starting test : BulkGetPreInitiatedTest - testNewBulkGetFailsWhileExistingGetInProgressAndJobCreationBlocked");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
        updateUserDataPolicy(client);

        // Create a bucket and put an object
        helper.ensureBucketExists(bucketName);

        final Path tempFilePath = Files.createTempFile("ds3test", "upload.txt");
        tempFilePath.toFile().deleteOnExit();

        final byte[] content = "0123456789\n".getBytes();
        final long objectSize = content.length;
        Files.write(tempFilePath, content, StandardOpenOption.CREATE);

        try (FileChannel readChannel = FileChannel.open(tempFilePath, StandardOpenOption.READ)) {
            PutObjectRequest putRequest = new PutObjectRequest(bucketName, "TestObject", readChannel, objectSize);
            client.putObject(putRequest);
        }

        // Wait for the PUT job to complete — object is now in cache
        waitForJobsToComplete(client);

        // Initiate a bulk GET job — this one will remain in progress (no data transfer yet)
        final Ds3ClientHelpers.Job existingReadJob = helper.startReadAllJob(bucketName);
        assertNotNull(existingReadJob, "First read job should be created successfully");
        assertNotNull(existingReadJob.getJobId(), "First read job should have a valid job ID");
        LOG.info("First bulk GET job initiated (in progress) with jobId: " + existingReadJob.getJobId());

        // Disable new job requests while the first GET job is still in progress
        ModifyDataPathBackendSpectraS3Request modifyRequest = new ModifyDataPathBackendSpectraS3Request();
        modifyRequest.withAllowNewJobRequests(false);
        client.modifyDataPathBackendSpectraS3(modifyRequest);
        LOG.info("Disabled allowNewJobRequests while existing GET job is still in progress");

        // Attempt to initiate a new bulk GET job — should fail with 403
        try {
            helper.startReadAllJob(bucketName);
            fail("Expected an exception when starting a new GET job with allowNewJobRequests=false and existing GET in progress");
        } catch (final Exception e) {
            LOG.info("New bulk GET job correctly rejected: " + e.getMessage());
            assertTrue(e.getMessage().contains("403"),
                    "Expected a 403 response, but got: " + e.getMessage());
        }
    }
}
