package com.spectralogic.integrations.blobs;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.revertBlobSize;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


public class TooManyBlobsPutTest {
    private final static Logger LOG = Logger.getLogger(TooManyBlobsPutTest.class);

    private static final String DATA_POLICY_NAME = "test-data-policy";
    private static final String BUCKET_NAME = "test-bucket";

    private Ds3Client client;

    private void cleanupDataPolicies() throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        for (DataPolicy dataPolicy : dataPolicies) {
            if (dataPolicy.getName().equals(DATA_POLICY_NAME)) {
                clearPersistenceRules(client, dataPolicy.getId());
                client.deleteDataPolicySpectraS3(new DeleteDataPolicySpectraS3Request(dataPolicy.getId()));
            } else {
                revertBlobSize(dataPolicy.getName());
            }
        }
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            cleanupAllBuckets(client);
            cleanupDataPolicies();
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupAllBuckets(client);
            clearAllJobs(client);
            cleanupDataPolicies();
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testFailToCreatePutJobWithTooManyBlobs() throws IOException {
        LOG.info("Starting test: TooManyBlobsPutTest - testFailToCreatePutJobWithTooManyBlobs");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Create data policy with blobbingEnabled=true, defaultBlobSize=1000
        final PutDataPolicySpectraS3Request createDpRequest = new PutDataPolicySpectraS3Request(DATA_POLICY_NAME);
        createDpRequest.withBlobbingEnabled(true);
        createDpRequest.withDefaultBlobSize(1000L);
        final PutDataPolicySpectraS3Response dpResponse = client.putDataPolicySpectraS3(createDpRequest);
        final DataPolicy dp = dpResponse.getDataPolicyResult();

        // Create persistence rule using tape
        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp.getId(), DataIsolationLevel.STANDARD, storageDomainId, DataPersistenceRuleType.PERMANENT));

        // Create bucket with the data policy
        helper.ensureBucketExists(BUCKET_NAME, dp.getId());

        // Build 10001 objects each of size 500,000,000
        // Each object: ceil(500000000 / 1000) = 500000 blobs
        // Total: 10001 * 500000 = 5,000,500,000 blobs — exceeds 500,000 max
        final List<Ds3Object> objects = new ArrayList<>();
        for (int i = 0; i < 10001; i++) {
            objects.add(new Ds3Object("test" + i, 500000000L));
        }

        LOG.info("Initiating bulk PUT with 10001 objects of size 500000000 (would require 5000500000 blobs)");

        try {
            client.putBulkJobSpectraS3(new PutBulkJobSpectraS3Request(BUCKET_NAME, objects));
            fail("Expected a 400 error for exceeding maximum blob count but bulk PUT succeeded");
        } catch (final FailedRequestException e) {
            LOG.info("Correctly failed with status code: " + e.getStatusCode()
                    + ", message: " + e.getMessage());
            assertEquals(400, e.getStatusCode(),
                    "Expected 400 for too many blobs but got " + e.getStatusCode());

            final String responseBody = e.getResponseString();
            assertTrue(responseBody.contains("5000500000 blobs were needed for 10001 objects, but 500000 is the maximum"),
                    "Response should contain expected blob count error message but was: " + responseBody);
        }

        LOG.info("Bulk PUT with too many blobs correctly returned 400.");
    }
}
