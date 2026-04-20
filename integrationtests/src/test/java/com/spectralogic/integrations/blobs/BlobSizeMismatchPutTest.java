package com.spectralogic.integrations.blobs;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


public class BlobSizeMismatchPutTest {
    private final static Logger LOG = Logger.getLogger(BlobSizeMismatchPutTest.class);

    private static final String BUCKET_NAME = "blob-size-mismatch";

    private Ds3Client client;

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
            cleanupBuckets(client, BUCKET_NAME);
            clearAllJobs(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testFailToPutBlobWithMismatchedSize() throws IOException {
        LOG.info("Starting test: BlobSizeMismatchPutTest - testFailToPutBlobWithMismatchedSize");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        updateUserDataPolicy();

        // Create bucket
        helper.ensureBucketExists(BUCKET_NAME);

        // Initiate bulk PUT with object "some_object" of size 1000
        final List<Ds3Object> objects = new ArrayList<>();
        objects.add(new Ds3Object("some_object", 1000));

        final PutBulkJobSpectraS3Response bulkResponse =
                client.putBulkJobSpectraS3(new PutBulkJobSpectraS3Request(BUCKET_NAME, objects));
        final MasterObjectList masterObjectList = bulkResponse.getMasterObjectList();
        final UUID jobId = masterObjectList.getJobId();
        LOG.info("Bulk PUT job initiated with declared size 1000: " + jobId);

        // Allocate chunks
        for (final com.spectralogic.ds3client.models.Objects chunk : masterObjectList.getObjects()) {
            client.allocateJobChunkSpectraS3(new AllocateJobChunkSpectraS3Request(chunk.getChunkId()));
        }

        // Attempt to PUT "some_object" with only 500 bytes — should fail with 409
        final byte[] mismatchedData = new byte[500];
        final ByteArrayInputStream stream = new ByteArrayInputStream(mismatchedData);

        final PutObjectRequest putRequest = new PutObjectRequest(
                BUCKET_NAME, "some_object", jobId, 0, 500, stream);

        try {
            client.putObject(putRequest);
            fail("Expected a 409 error for size mismatch but PUT succeeded");
        } catch (final FailedRequestException e) {
            LOG.info("Correctly failed with status code: " + e.getStatusCode()
                    + ", message: " + e.getMessage());
            assertEquals(409, e.getStatusCode(),
                    "Expected 409 for size mismatch but got " + e.getStatusCode());

            final String responseBody = e.getResponseString();
            assertTrue(responseBody.contains("Expected blob size"),
                    "Response should contain 'Expected blob size' but was: " + responseBody);
            assertTrue(responseBody.contains("was 1000B, but file was 500B"),
                    "Response should contain 'was 1000B, but file was 500B' but was: " + responseBody);
        }

        LOG.info("Blob PUT with mismatched size correctly returned 409 with expected error message.");
    }
}
