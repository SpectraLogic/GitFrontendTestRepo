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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


public class NonAggregatingPutJobTest {
    private final static Logger LOG = Logger.getLogger(NonAggregatingPutJobTest.class);

    private static final String BUCKET_NAME = "bulk-put";

    private Ds3Client client;

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

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testFailToAggregateWhenAggregatingNotEnabled() throws IOException {
        LOG.info("Starting test: NonAggregatingPutJobTest - testFailToAggregateWhenAggregatingNotEnabled");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Set user's default data policy so bucket creation doesn't fail
        updateUserDataPolicy();

        // Create bucket
        helper.ensureBucketExists(BUCKET_NAME);

        // First bulk PUT without aggregating
        final List<Ds3Object> firstObjects = new ArrayList<>();
        firstObjects.add(new Ds3Object("DummyObject_1.txt", 1000));
        firstObjects.add(new Ds3Object("DummyObject_2.txt", 1000));
        firstObjects.add(new Ds3Object("DummyObject_3.txt", 1000));

        final PutBulkJobSpectraS3Request firstRequest =
                new PutBulkJobSpectraS3Request(BUCKET_NAME, firstObjects);
        final PutBulkJobSpectraS3Response firstResponse = client.putBulkJobSpectraS3(firstRequest);
        final UUID firstJobId = firstResponse.getMasterObjectList().getJobId();
        LOG.info("First bulk PUT job (non-aggregating) created: " + firstJobId);

        // Second bulk PUT with aggregating enabled
        final List<Ds3Object> secondObjects = new ArrayList<>();
        secondObjects.add(new Ds3Object("DummyObject_4.txt", 1000));
        secondObjects.add(new Ds3Object("DummyObject_5.txt", 1000));
        secondObjects.add(new Ds3Object("DummyObject_6.txt", 1000));

        final PutBulkJobSpectraS3Request secondRequest =
                new PutBulkJobSpectraS3Request(BUCKET_NAME, secondObjects).withAggregating(true);
        final PutBulkJobSpectraS3Response secondResponse = client.putBulkJobSpectraS3(secondRequest);
        final UUID secondJobId = secondResponse.getMasterObjectList().getJobId();
        LOG.info("Second bulk PUT job (aggregating) created: " + secondJobId);

        assertNotEquals(firstJobId, secondJobId,
                "A non-aggregating job should NOT be aggregated with a subsequent aggregating job");

        LOG.info("Verified that non-aggregating and aggregating PUT jobs have different job IDs.");
    }
}
