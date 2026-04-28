package com.spectralogic.integrations.future;

import com.google.common.collect.Lists;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;

public class BucketIsolatedAllocationLogThrottleTest {
    private final static Logger LOG = Logger.getLogger(BucketIsolatedAllocationLogThrottleTest.class);

    private static final String DATA_POLICY_NAME = "bucket_isolated_log_throttle_dp";
    private static final String BUCKET_PREFIX = "isolated_log_throttle_bucket_";
    private static final String INPUT_FILES_DIR = "testFiles";
    private static final int NUM_BUCKETS = 25;
    private static final int PUT_JOBS_PER_BUCKET = 5;

    private Ds3Client client;

    private void cleanupTestBuckets() throws IOException {
        for (int i = 0; i < NUM_BUCKETS; i++) {
            cleanupBuckets(client, BUCKET_PREFIX + i);
        }
    }

    private void cleanupTestDataPolicy() throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        final List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        for (final DataPolicy dataPolicy : dataPolicies) {
            if (dataPolicy.getName().equals(DATA_POLICY_NAME)) {
                clearPersistenceRules(client, dataPolicy.getId());
                client.deleteDataPolicySpectraS3(new DeleteDataPolicySpectraS3Request(dataPolicy.getId()));
            }
        }
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            cleanupTestBuckets();
            cleanupTestDataPolicy();
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupTestBuckets();
            clearAllJobs(client);
            cleanupTestDataPolicy();
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    public void testBucketIsolatedAllocationTriggersLogCondition() throws IOException, URISyntaxException {
        LOG.info("Starting test: BucketIsolatedAllocationLogThrottleTest");

        final PutDataPolicySpectraS3Response dpResponse = client.putDataPolicySpectraS3(
                new PutDataPolicySpectraS3Request(DATA_POLICY_NAME));
        final DataPolicy dp = dpResponse.getDataPolicyResult();

        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp.getId(),
                DataIsolationLevel.BUCKET_ISOLATED,
                storageDomainId,
                DataPersistenceRuleType.PERMANENT));

        modifyUser(client, dp);

        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        final URL testFilesUrl = getClass().getClassLoader().getResource(INPUT_FILES_DIR);
        if (testFilesUrl == null) {
            throw new RuntimeException("Could not find " + INPUT_FILES_DIR + " directory in resources.");
        }
        final Path inputPath = Paths.get(testFilesUrl.toURI());
        final List<Ds3Object> sourceObjects = Lists.newArrayList(helper.listObjectsForDirectory(inputPath));
        Assertions.assertTrue(sourceObjects.size() >= PUT_JOBS_PER_BUCKET,
                "Expected at least " + PUT_JOBS_PER_BUCKET + " files in " + INPUT_FILES_DIR
                        + " but found " + sourceObjects.size());

        for (int b = 0; b < NUM_BUCKETS; b++) {
            final String bucketName = BUCKET_PREFIX + b;
            helper.ensureBucketExists(bucketName, dp.getId());

            for (int j = 0; j < PUT_JOBS_PER_BUCKET; j++) {
                final Ds3Object sourceObject = sourceObjects.get(j);
                final Ds3ClientHelpers.Job job = helper.startWriteJob(
                        bucketName, Collections.singletonList(sourceObject));
                job.transfer(new FileObjectPutter(inputPath));
                addJobName(client, "log_throttle_b" + b + "_" + sourceObject.getName(), job.getJobId());
            }
        }

        LOG.info("Created " + NUM_BUCKETS + " buckets and "
                + (NUM_BUCKETS * PUT_JOBS_PER_BUCKET)
                + " PUT jobs (with data) against bucket-isolated data policy " + DATA_POLICY_NAME);

        // Give the DataPlanner time to repeatedly evaluate pending allocations across all
        // (bucket, storage_domain) keys. The TapeTaskStarter wakes once per minute, so we
        // sleep long enough to span several cycles. With the throttle in
        // CanAllocatePersistenceTargetSupport, the "Media can be allocated to storage domain"
        // log line should appear at most once per 2 minutes per (bucket, storage_domain)
        // key unless the pending or available byte counts change.
        TestUtil.sleep(5 * 60 * 1000);
    }
}
