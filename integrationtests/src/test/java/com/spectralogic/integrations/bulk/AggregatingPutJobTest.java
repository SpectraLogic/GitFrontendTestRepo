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


public class AggregatingPutJobTest {
    private final static Logger LOG = Logger.getLogger(AggregatingPutJobTest.class);

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
    public void testSuccessfullyAggregateTwoPutJobs() throws IOException {
        LOG.info("Starting test: AggregatingPutJobTest - testSuccessfullyAggregateTwoPutJobs");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Set user's default data policy so bucket creation doesn't fail
        updateUserDataPolicy();

        // Create bucket
        helper.ensureBucketExists(BUCKET_NAME);

        // First aggregating bulk PUT with 3 objects
        final List<Ds3Object> firstObjects = new ArrayList<>();
        firstObjects.add(new Ds3Object("DummyObject_1.txt", 1000));
        firstObjects.add(new Ds3Object("DummyObject_2.txt", 1000));
        firstObjects.add(new Ds3Object("DummyObject_3.txt", 1000));

        final PutBulkJobSpectraS3Request firstRequest =
                new PutBulkJobSpectraS3Request(BUCKET_NAME, firstObjects).withAggregating(true);
        final PutBulkJobSpectraS3Response firstResponse = client.putBulkJobSpectraS3(firstRequest);
        final UUID firstJobId = firstResponse.getMasterObjectList().getJobId();
        LOG.info("First aggregating bulk PUT job created: " + firstJobId);

        // Second aggregating bulk PUT with 3 more objects — should return the same job ID
        final List<Ds3Object> secondObjects = new ArrayList<>();
        secondObjects.add(new Ds3Object("DummyObject_4.txt", 1000));
        secondObjects.add(new Ds3Object("DummyObject_5.txt", 1000));
        secondObjects.add(new Ds3Object("DummyObject_6.txt", 1000));

        final PutBulkJobSpectraS3Request secondRequest =
                new PutBulkJobSpectraS3Request(BUCKET_NAME, secondObjects).withAggregating(true);
        final PutBulkJobSpectraS3Response secondResponse = client.putBulkJobSpectraS3(secondRequest);
        final UUID secondJobId = secondResponse.getMasterObjectList().getJobId();
        LOG.info("Second aggregating bulk PUT job returned: " + secondJobId);

        assertEquals(firstJobId, secondJobId,
                "Both aggregating bulk PUT requests should return the same job ID");

        // Ask which chunks are ready for processing
        final GetJobChunksReadyForClientProcessingSpectraS3Response chunksResponse =
                client.getJobChunksReadyForClientProcessingSpectraS3(
                        new GetJobChunksReadyForClientProcessingSpectraS3Request(firstJobId));

        final List<com.spectralogic.ds3client.models.Objects> chunks =
                chunksResponse.getMasterObjectListResult().getObjects();

        int totalObjects = 0;
        for (final com.spectralogic.ds3client.models.Objects chunk : chunks) {
            totalObjects += chunk.getObjects().size();
        }

        LOG.info("Chunks ready: " + chunks.size() + ", total objects across chunks: " + totalObjects);
        assertEquals(6, totalObjects,
                "The aggregated job should contain 6 objects across all chunks");

        LOG.info("Successfully verified two aggregated PUT jobs with 6 total objects.");
    }
}
