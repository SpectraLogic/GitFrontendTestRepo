package com.spectralogic.integrations.bulk;

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


public class AggregatingDifferentStorageDomainTest {
    private final static Logger LOG = Logger.getLogger(AggregatingDifferentStorageDomainTest.class);

    private static final String DATA_POLICY_1_NAME = "test-dp-tape-single";
    private static final String DATA_POLICY_2_NAME = "test-dp-tape-dual";
    private static final String BUCKET_NAME_1 = "storage-domain-agg-bucket-1";
    private static final String BUCKET_NAME_2 = "storage-domain-agg-bucket-2";

    private Ds3Client client;

    private void cleanupDataPolicies() throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        for (DataPolicy dataPolicy : dataPolicies) {
            if (dataPolicy.getName().equals(DATA_POLICY_1_NAME) ||
                    dataPolicy.getName().equals(DATA_POLICY_2_NAME)) {
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
            cleanupBuckets(client, BUCKET_NAME_1);
            cleanupBuckets(client, BUCKET_NAME_2);
            cleanupDataPolicies();
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupBuckets(client, BUCKET_NAME_1);
            cleanupBuckets(client, BUCKET_NAME_2);
            clearAllJobs(client);
            cleanupDataPolicies();
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testFailToAggregateJobsWithDifferentStorageDomains() throws IOException {
        LOG.info("Starting test: AggregatingDifferentStorageDomainTest - testFailToAggregateJobsWithDifferentStorageDomains");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Create data policy 1 with "Tape First Copy" storage domain
        final PutDataPolicySpectraS3Response dp1Response =
                client.putDataPolicySpectraS3(new PutDataPolicySpectraS3Request(DATA_POLICY_1_NAME));
        final DataPolicy dp1 = dp1Response.getDataPolicyResult();
        final UUID storageDomain1Id = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp1.getId(), DataIsolationLevel.STANDARD, storageDomain1Id, DataPersistenceRuleType.PERMANENT));

        // Create data policy 2 with "Tape Second Copy" storage domain
        final PutDataPolicySpectraS3Response dp2Response =
                client.putDataPolicySpectraS3(new PutDataPolicySpectraS3Request(DATA_POLICY_2_NAME));
        final DataPolicy dp2 = dp2Response.getDataPolicyResult();
        final UUID storageDomain2Id = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_DUAL_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp2.getId(), DataIsolationLevel.STANDARD, storageDomain2Id, DataPersistenceRuleType.PERMANENT));

        // Create buckets with different data policies (different storage domains)
        helper.ensureBucketExists(BUCKET_NAME_1, dp1.getId());
        helper.ensureBucketExists(BUCKET_NAME_2, dp2.getId());

        // First aggregating bulk PUT into bucket 1 (storage domain: Tape First Copy)
        final List<Ds3Object> firstObjects = new ArrayList<>();
        firstObjects.add(new Ds3Object("DummyObject_1.txt", 1000));
        firstObjects.add(new Ds3Object("DummyObject_2.txt", 1000));

        final PutBulkJobSpectraS3Request firstRequest =
                new PutBulkJobSpectraS3Request(BUCKET_NAME_1, firstObjects)
                        .withAggregating(true);
        final PutBulkJobSpectraS3Response firstResponse = client.putBulkJobSpectraS3(firstRequest);
        final UUID firstJobId = firstResponse.getMasterObjectList().getJobId();
        LOG.info("First aggregating PUT job (storage domain: Tape First Copy) created: " + firstJobId);

        // Second aggregating bulk PUT into bucket 2 (storage domain: Tape Second Copy)
        // Should NOT aggregate — different storage domain
        final List<Ds3Object> secondObjects = new ArrayList<>();
        secondObjects.add(new Ds3Object("DummyObject_3.txt", 1000));
        secondObjects.add(new Ds3Object("DummyObject_4.txt", 1000));

        final PutBulkJobSpectraS3Request secondRequest =
                new PutBulkJobSpectraS3Request(BUCKET_NAME_2, secondObjects)
                        .withAggregating(true);
        final PutBulkJobSpectraS3Response secondResponse = client.putBulkJobSpectraS3(secondRequest);
        final UUID secondJobId = secondResponse.getMasterObjectList().getJobId();
        LOG.info("Second aggregating PUT job (storage domain: Tape Second Copy) created: " + secondJobId);

        assertNotEquals(firstJobId, secondJobId,
                "Aggregating PUT jobs with different storage domains should NOT return the same job ID");

        LOG.info("Verified that different storage domains prevent job aggregation.");
    }
}
