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
public class GetJobBlockedTest {
    private final String bucketName = "get_job_blocked_bucket";
    private final static Logger LOG = Logger.getLogger(GetJobBlockedTest.class);
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
            // Revert allowNewJobRequests back to true
            ModifyDataPathBackendSpectraS3Request revertRequest = new ModifyDataPathBackendSpectraS3Request();
            revertRequest.withAllowNewJobRequests(true);
            client.modifyDataPathBackendSpectraS3(revertRequest);

            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testGetJobFailsWhenNewJobRequestsDisabled() throws IOException, InterruptedException {
        LOG.info("Starting test : GetJobBlockedTest - testGetJobFailsWhenNewJobRequestsDisabled");
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

        // Wait for the PUT job to complete
        waitForJobsToComplete(client);

        // Disable new job requests on the data path backend
        ModifyDataPathBackendSpectraS3Request modifyRequest = new ModifyDataPathBackendSpectraS3Request();
        modifyRequest.withAllowNewJobRequests(false);
        client.modifyDataPathBackendSpectraS3(modifyRequest);

        // Attempt a GET job on the bucket — should fail with 403
        try {
            helper.startReadAllJob(bucketName);
            fail("Expected an exception when starting a GET job with allowNewJobRequests=false");
        } catch (final Exception e) {
            LOG.info("GET job correctly rejected: " + e.getMessage());
            assertTrue(e.getMessage().contains("403"),
                    "Expected a 403 response, but got: " + e.getMessage());
        }

        // Revert allowNewJobRequests back to true
        ModifyDataPathBackendSpectraS3Request revertRequest = new ModifyDataPathBackendSpectraS3Request();
        revertRequest.withAllowNewJobRequests(true);
        client.modifyDataPathBackendSpectraS3(revertRequest);

        // Attempt a GET job again — should now succeed
        final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
        assertNotNull(readJob, "GET job should succeed after re-enabling allowNewJobRequests");
        assertNotNull(readJob.getJobId(), "GET job should have a valid job ID");
        LOG.info("GET job succeeded after reverting allowNewJobRequests=true, jobId: " + readJob.getJobId());
    }
}
