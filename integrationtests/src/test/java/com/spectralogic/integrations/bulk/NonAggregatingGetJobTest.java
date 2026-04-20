package com.spectralogic.integrations.bulk;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


public class NonAggregatingGetJobTest {
    private final static Logger LOG = Logger.getLogger(NonAggregatingGetJobTest.class);

    private static final String BUCKET_NAME = "bulk-get-1";

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

    private Path createTempDirWithFiles(final String[] fileNames, final long sizeInBytes) throws IOException {
        final Path tempDir = Files.createTempDirectory("non-agg-get-test");
        tempDir.toFile().deleteOnExit();
        for (final String fileName : fileNames) {
            final File file = new File(tempDir.toFile(), fileName);
            file.deleteOnExit();
            try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(sizeInBytes);
            }
        }
        return tempDir;
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testFailToAggregateGetJobsWhenFirstIsNotAggregating() throws IOException, InterruptedException {
        LOG.info("Starting test: NonAggregatingGetJobTest - testFailToAggregateGetJobsWhenFirstIsNotAggregating");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Set user's default data policy so bucket creation doesn't fail
        updateUserDataPolicy();

        // Create bucket and PUT 6 objects
        helper.ensureBucketExists(BUCKET_NAME);

        final String[] objectNames = {
                "DummyObject_1.txt", "DummyObject_2.txt", "DummyObject_3.txt",
                "DummyObject_4.txt", "DummyObject_5.txt", "DummyObject_6.txt"
        };
        final Path tempDir = createTempDirWithFiles(objectNames, 1000);
        final Iterable<Ds3Object> putObjects = helper.listObjectsForDirectory(tempDir);
        final Ds3ClientHelpers.Job putJob = helper.startWriteJob(BUCKET_NAME, putObjects);
        putJob.transfer(new FileObjectPutter(tempDir));
        final UUID putJobId = putJob.getJobId();
        LOG.info("Bulk PUT job initiated: " + putJobId);

        isJobCompleted(client, putJobId);
        waitForJobsToComplete(client);
        LOG.info("All PUT jobs completed.");

        // Verify bucket has 6 objects
        final com.spectralogic.ds3client.commands.GetBucketResponse bucketResponse =
                client.getBucket(new com.spectralogic.ds3client.commands.GetBucketRequest(BUCKET_NAME));
        assertEquals(6, bucketResponse.getListBucketResult().getObjects().size(),
                "Bucket should have 6 objects");
        LOG.info("Bucket verified: 6 objects present.");

        // First bulk GET without aggregating — objects 1-3
        final List<Ds3Object> firstGetObjects = new ArrayList<>();
        firstGetObjects.add(new Ds3Object("DummyObject_1.txt", 1000));
        firstGetObjects.add(new Ds3Object("DummyObject_2.txt", 1000));
        firstGetObjects.add(new Ds3Object("DummyObject_3.txt", 1000));

        final GetBulkJobSpectraS3Request firstGetRequest =
                new GetBulkJobSpectraS3Request(BUCKET_NAME, firstGetObjects);
        final GetBulkJobSpectraS3Response firstGetResponse = client.getBulkJobSpectraS3(firstGetRequest);
        final UUID firstGetJobId = firstGetResponse.getMasterObjectList().getJobId();
        LOG.info("First bulk GET job (non-aggregating) created: " + firstGetJobId);

        // Second bulk GET with aggregating — objects 4-6
        final List<Ds3Object> secondGetObjects = new ArrayList<>();
        secondGetObjects.add(new Ds3Object("DummyObject_4.txt", 1000));
        secondGetObjects.add(new Ds3Object("DummyObject_5.txt", 1000));
        secondGetObjects.add(new Ds3Object("DummyObject_6.txt", 1000));

        final GetBulkJobSpectraS3Request secondGetRequest =
                new GetBulkJobSpectraS3Request(BUCKET_NAME, secondGetObjects).withAggregating(true);
        final GetBulkJobSpectraS3Response secondGetResponse = client.getBulkJobSpectraS3(secondGetRequest);
        final UUID secondGetJobId = secondGetResponse.getMasterObjectList().getJobId();
        LOG.info("Second bulk GET job (aggregating) created: " + secondGetJobId);

        assertNotEquals(firstGetJobId, secondGetJobId,
                "A non-aggregating GET job should NOT be aggregated with a subsequent aggregating GET job");

        LOG.info("Verified that non-aggregating and aggregating GET jobs have different job IDs.");
    }
}
