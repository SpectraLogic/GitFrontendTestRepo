package com.spectralogic.integrations.blobs;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
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

@Tag("LargeCache")
public class MaxBlobsPutTest {
    private final static Logger LOG = Logger.getLogger(MaxBlobsPutTest.class);

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
    @Timeout(value = 125, unit = TimeUnit.MINUTES)
    public void testSuccessfullyCreatePutJobWith500000Blobs() throws IOException {
        LOG.info("Starting test: MaxBlobsPutTest - testSuccessfullyCreatePutJobWith500000Blobs");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Create data policy with blobbingEnabled=true, defaultBlobSize=2
        final PutDataPolicySpectraS3Request createDpRequest = new PutDataPolicySpectraS3Request(DATA_POLICY_NAME);
        createDpRequest.withBlobbingEnabled(true);
        createDpRequest.withDefaultBlobSize(2L);
        final PutDataPolicySpectraS3Response dpResponse = client.putDataPolicySpectraS3(createDpRequest);
        final DataPolicy dp = dpResponse.getDataPolicyResult();

        // Create persistence rule using tape
        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp.getId(), DataIsolationLevel.STANDARD, storageDomainId, DataPersistenceRuleType.PERMANENT));

        // Create bucket with the data policy
        helper.ensureBucketExists(BUCKET_NAME, dp.getId());

        // Build 5000 objects each of size 200
        // Each object: ceil(200 / 2) = 100 blobs
        // Total: 5000 * 100 = 500,000 blobs — exactly the maximum allowed
        final List<Ds3Object> objects = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            objects.add(new Ds3Object("test" + i, 200L));
        }

        LOG.info("Initiating bulk PUT with 5000 objects of size 200 (requires exactly 500000 blobs)");

        // Should succeed with 200 — exactly at the 500,000 blob limit
        final PutBulkJobSpectraS3Response bulkResponse =
                client.putBulkJobSpectraS3(new PutBulkJobSpectraS3Request(BUCKET_NAME, objects));
        assertNotNull(bulkResponse.getMasterObjectList(),
                "Bulk PUT should return a valid MasterObjectList");
        assertNotNull(bulkResponse.getMasterObjectList().getJobId(),
                "Bulk PUT should return a valid job ID");

        UUID jobId = bulkResponse.getMasterObjectList().getJobId();
        LOG.info("Bulk PUT job successfully created: " + bulkResponse.getMasterObjectList().getJobId());

        LOG.info("Cancelling first job set: " + bulkResponse.getMasterObjectList().getJobId());
        CancelActiveJobSpectraS3Request cancelActiveJobSpectraS3Request = new CancelActiveJobSpectraS3Request(jobId);
        client.cancelActiveJobSpectraS3(cancelActiveJobSpectraS3Request);
        LOG.info("Waiting for cancellation cleanup of job: " + jobId);
        waitForCancellation(client, jobId);
        LOG.info("Cancellation cleanup complete for job: " + jobId);

        final List<Ds3Object> objectsSet2 = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            objectsSet2.add(new Ds3Object("testSet2" + i, 200L));
        }

        LOG.info("Initiating bulk PUT with 5000 objectsSet2 of size 200 (requires exactly 500000 blobs)");

        // Should succeed with 200 — exactly at the 500,000 blob limit
        final PutBulkJobSpectraS3Response bulkResponse2 =
                client.putBulkJobSpectraS3(new PutBulkJobSpectraS3Request(BUCKET_NAME, objectsSet2));
        assertNotNull(bulkResponse2.getMasterObjectList(),
                "Bulk PUT should return a valid MasterObjectList");
        assertNotNull(bulkResponse2.getMasterObjectList().getJobId(),
                "Bulk PUT should return a valid job ID");

        UUID jobId2 = bulkResponse2.getMasterObjectList().getJobId();
        LOG.info("Bulk PUT job successfully created: " + bulkResponse2.getMasterObjectList().getJobId());





        LOG.info("Cancelling second job set: " + bulkResponse2.getMasterObjectList().getJobId());
        CancelActiveJobSpectraS3Request cancelActiveJobSpectraS3Request2 = new CancelActiveJobSpectraS3Request(jobId2);
        client.cancelActiveJobSpectraS3(cancelActiveJobSpectraS3Request2);
        LOG.info("Waiting for cancellation cleanup of job: " + jobId2);
        waitForCancellation(client, jobId2);
        LOG.info("Cancellation cleanup complete for job: " + jobId2);



    }
}
