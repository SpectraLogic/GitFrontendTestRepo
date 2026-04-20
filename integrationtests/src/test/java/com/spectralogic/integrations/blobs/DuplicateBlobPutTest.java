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


public class DuplicateBlobPutTest {
    private final static Logger LOG = Logger.getLogger(DuplicateBlobPutTest.class);

    private static final String DATA_POLICY_NAME = "first-data-policy";
    private static final String BUCKET_NAME = "first-bucket";

    private Ds3Client client;
    private UUID dataPolicyId;

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

    private void cleanupDataPolicies() throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        for ( DataPolicy dataPolicy: dataPolicies) {
            if (dataPolicy.getName().equals(DATA_POLICY_NAME) ) {
                clearPersistenceRules(client, dataPolicy.getId());
                client.deleteDataPolicySpectraS3(new DeleteDataPolicySpectraS3Request(dataPolicy.getId()));

            } else {
                revertBlobSize(dataPolicy.getName());
            }
        }
    }
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testFailToPutBlobWhileFirstAttemptInTransit() throws IOException {
        LOG.info("Starting test: DuplicateBlobPutTest - testFailToPutBlobWhileFirstAttemptInTransit");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Create data policy with blobbingEnabled=true, defaultBlobSize=10000000000 (10 GB)
        final PutDataPolicySpectraS3Response createResponse =
                client.putDataPolicySpectraS3(new PutDataPolicySpectraS3Request(DATA_POLICY_NAME));
        final DataPolicy dp = createResponse.getDataPolicyResult();
        dataPolicyId = dp.getId();

        final ModifyDataPolicySpectraS3Request modifyRequest = new ModifyDataPolicySpectraS3Request(dp.getId());
        modifyRequest.withBlobbingEnabled(true);
        modifyRequest.withDefaultBlobSize(10000000000L);
        client.modifyDataPolicySpectraS3(modifyRequest);

        // Create persistence rule using tape
        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp.getId(), DataIsolationLevel.STANDARD, storageDomainId, DataPersistenceRuleType.PERMANENT));

        // Create bucket with the data policy
        helper.ensureBucketExists(BUCKET_NAME, dp.getId());

        // Define the objects for bulk put
        final List<Ds3Object> objects = new ArrayList<>();
        objects.add(new Ds3Object("my_object", 100000000000L));
        objects.add(new Ds3Object("cucumber-file.txt", 100000000000L));

        // First bulk put initiation — should succeed
        final Ds3ClientHelpers.Job firstJob = helper.startWriteJob(BUCKET_NAME, objects);
        LOG.info("First bulk PUT job initiated successfully: " + firstJob.getJobId());

        // Second bulk put initiation with the same objects — should fail
        // because the blobs are still in transit from the first job
        try {
            helper.startWriteJob(BUCKET_NAME, objects);
            fail("Expected an error when initiating a second bulk PUT while the first is still in transit");
        } catch (final FailedRequestException e) {
            LOG.info("Second bulk PUT correctly failed with status code: " + e.getStatusCode()
                    + ", error: " + e.getMessage());
        }
    }
}
