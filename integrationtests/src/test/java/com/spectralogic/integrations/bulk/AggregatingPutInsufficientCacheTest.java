package com.spectralogic.integrations.bulk;

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


public class AggregatingPutInsufficientCacheTest {
    private final static Logger LOG = Logger.getLogger(AggregatingPutInsufficientCacheTest.class);

    private static final String DATA_POLICY_NAME = "test-data-policy";
    private static final String BUCKET_NAME = "bulk-put";

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
            cleanupDataPolicies();
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupBuckets(client, BUCKET_NAME);
            clearAllJobs(client);
            cleanupDataPolicies();
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testFailToCreateLargeAggregatingPutWithInsufficientCache() throws IOException {
        LOG.info("Starting test: AggregatingPutInsufficientCacheTest - testFailToCreateLargeAggregatingPutWithInsufficientCache");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Create data policy with blobbing enabled
        final PutDataPolicySpectraS3Request createDpRequest = new PutDataPolicySpectraS3Request(DATA_POLICY_NAME);
        createDpRequest.withBlobbingEnabled(true);
        final PutDataPolicySpectraS3Response dpResponse = client.putDataPolicySpectraS3(createDpRequest);
        final DataPolicy dp = dpResponse.getDataPolicyResult();

        // Create persistence rule using tape
        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp.getId(), DataIsolationLevel.STANDARD, storageDomainId, DataPersistenceRuleType.PERMANENT));

        // Create bucket with the data policy
        helper.ensureBucketExists(BUCKET_NAME, dp.getId());

        // Initiate aggregating bulk PUT with a 50 TB object
        final List<Ds3Object> objects = new ArrayList<>();
        objects.add(new Ds3Object("DummyObject_1.txt", 50000000000000L));

        LOG.info("Initiating aggregating bulk PUT with 50 TB object");

        final PutBulkJobSpectraS3Request request =
                new PutBulkJobSpectraS3Request(BUCKET_NAME, objects).withAggregating(true);

        try {
            client.putBulkJobSpectraS3(request);
            fail("Expected a 503 error for insufficient cache but bulk PUT succeeded");
        } catch (final FailedRequestException e) {
            LOG.info("Correctly failed with status code: " + e.getStatusCode()
                    + ", message: " + e.getMessage());
            assertEquals(503, e.getStatusCode(),
                    "Expected 503 for insufficient cache but got " + e.getStatusCode());

            final String responseBody = e.getResponseString();
            assertTrue(responseBody.contains("PUT jobs that are aggregating must be entirely pre-allocated"),
                    "Response should contain pre-allocation error message but was: " + responseBody);
            assertTrue(responseBody.contains("Cannot pre-allocate this 46566 GiB job at this time"),
                    "Response should contain job size detail but was: " + responseBody);
            assertTrue(responseBody.contains("Please ensure you have adequate cache space to entirely pre-allocate this job"),
                    "Response should contain cache space guidance but was: " + responseBody);
        }

        LOG.info("Aggregating bulk PUT with insufficient cache correctly returned 503.");
    }
}
