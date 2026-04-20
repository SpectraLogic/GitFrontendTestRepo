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


@Tag("LargeCache")
public class LargePutJobTest {
    private final static Logger LOG = Logger.getLogger(LargePutJobTest.class);

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

    private DataPolicy createDataPolicyWithBlobbing() throws IOException {
        final PutDataPolicySpectraS3Request createDpRequest = new PutDataPolicySpectraS3Request(DATA_POLICY_NAME);
        createDpRequest.withBlobbingEnabled(true);
        final PutDataPolicySpectraS3Response dpResponse = client.putDataPolicySpectraS3(createDpRequest);
        final DataPolicy dp = dpResponse.getDataPolicyResult();

        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp.getId(), DataIsolationLevel.STANDARD, storageDomainId, DataPersistenceRuleType.PERMANENT));

        return dp;
    }


    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testSuccessfullyCreate50TBAggregatingPutJob() throws IOException {
        LOG.info("Starting test: LargePutJobTest - testSuccessfullyCreate50TBAggregatingPutJob");
        reclaimCache(client);
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        final DataPolicy dp = createDataPolicyWithBlobbing();
        helper.ensureBucketExists(BUCKET_NAME, dp.getId());

        // Initiate aggregating bulk PUT with a 50 TB object
        final List<Ds3Object> objects = new ArrayList<>();
        objects.add(new Ds3Object("DummyObject_1.txt", 50000000000000L));

        LOG.info("Initiating aggregating bulk PUT with 50 TB object");

        final PutBulkJobSpectraS3Request request =
                new PutBulkJobSpectraS3Request(BUCKET_NAME, objects).withAggregating(true);
        final PutBulkJobSpectraS3Response response = client.putBulkJobSpectraS3(request);

        assertNotNull(response.getMasterObjectList(),
                "Bulk PUT should return a valid MasterObjectList");
        assertNotNull(response.getMasterObjectList().getJobId(),
                "Bulk PUT should return a valid job ID");

        LOG.info("50 TB aggregating bulk PUT job successfully created: "
                + response.getMasterObjectList().getJobId());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testSuccessfullyCreate49TBNonAggregatingPutJob() throws IOException {
        LOG.info("Starting test: LargePutJobTest - testSuccessfullyCreate49TBNonAggregatingPutJob");
        reclaimCache(client);
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        final DataPolicy dp = createDataPolicyWithBlobbing();
        helper.ensureBucketExists(BUCKET_NAME, dp.getId());

        // Initiate non-aggregating bulk PUT with a 49 TB object
        final List<Ds3Object> objects = new ArrayList<>();
        objects.add(new Ds3Object("DummyObject_1.txt", 49000000000000L));

        LOG.info("Initiating non-aggregating bulk PUT with 49 TB object");

        final PutBulkJobSpectraS3Request request =
                new PutBulkJobSpectraS3Request(BUCKET_NAME, objects);
        final PutBulkJobSpectraS3Response response = client.putBulkJobSpectraS3(request);

        assertNotNull(response.getMasterObjectList(),
                "Bulk PUT should return a valid MasterObjectList");
        assertNotNull(response.getMasterObjectList().getJobId(),
                "Bulk PUT should return a valid job ID");

        LOG.info("49 TB non-aggregating bulk PUT job successfully created: "
                + response.getMasterObjectList().getJobId());
    }
}
