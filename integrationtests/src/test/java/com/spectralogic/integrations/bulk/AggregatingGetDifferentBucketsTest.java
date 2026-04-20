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


public class AggregatingGetDifferentBucketsTest {
    private final static Logger LOG = Logger.getLogger(AggregatingGetDifferentBucketsTest.class);

    private static final String BUCKET_NAME_1 = "bulk-get-1";
    private static final String BUCKET_NAME_2 = "bulk-get-2";

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
            cleanupBuckets(client, BUCKET_NAME_1);
            cleanupBuckets(client, BUCKET_NAME_2);
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
        final Path tempDir = Files.createTempDirectory("agg-get-diff-bucket-test");
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

    private void putObjectsToBucket(Ds3ClientHelpers helper, String bucketName,
                                    String[] objectNames, long size) throws IOException, InterruptedException {
        final Path tempDir = createTempDirWithFiles(objectNames, size);
        final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(tempDir);
        final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
        job.transfer(new FileObjectPutter(tempDir));
        final UUID jobId = job.getJobId();
        LOG.info("Bulk PUT job for bucket " + bucketName + ": " + jobId);
        isJobCompleted(client, jobId);
        waitForJobsToComplete(client);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testFailToAggregateTwoGetJobsFromDifferentBuckets() throws IOException, InterruptedException {
        LOG.info("Starting test: AggregatingGetDifferentBucketsTest - testFailToAggregateTwoGetJobsFromDifferentBuckets");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Set user's default data policy so bucket creation doesn't fail
        updateUserDataPolicy();

        final String[] objectNames = {
                "DummyObject_1.txt", "DummyObject_2.txt", "DummyObject_3.txt",
                "DummyObject_4.txt", "DummyObject_5.txt", "DummyObject_6.txt"
        };

        // Create bucket 1 and PUT 6 objects
        helper.ensureBucketExists(BUCKET_NAME_1);
        putObjectsToBucket(helper, BUCKET_NAME_1, objectNames, 1000);

        // Verify bucket 1 has 6 objects
        final com.spectralogic.ds3client.commands.GetBucketResponse bucket1Response =
                client.getBucket(new com.spectralogic.ds3client.commands.GetBucketRequest(BUCKET_NAME_1));
        assertEquals(6, bucket1Response.getListBucketResult().getObjects().size(),
                "Bucket bulk-get-1 should have 6 objects");
        LOG.info("Bucket bulk-get-1 verified: 6 objects present.");

        // Create bucket 2 and PUT 6 objects
        helper.ensureBucketExists(BUCKET_NAME_2);
        putObjectsToBucket(helper, BUCKET_NAME_2, objectNames, 1000);

        // Verify bucket 2 has 6 objects
        final com.spectralogic.ds3client.commands.GetBucketResponse bucket2Response =
                client.getBucket(new com.spectralogic.ds3client.commands.GetBucketRequest(BUCKET_NAME_2));
        assertEquals(6, bucket2Response.getListBucketResult().getObjects().size(),
                "Bucket bulk-get-2 should have 6 objects");
        LOG.info("Bucket bulk-get-2 verified: 6 objects present.");

        // Aggregating bulk GET from bucket 1 with objects 1-3
        final List<Ds3Object> firstGetObjects = new ArrayList<>();
        firstGetObjects.add(new Ds3Object("DummyObject_1.txt", 1000));
        firstGetObjects.add(new Ds3Object("DummyObject_2.txt", 1000));
        firstGetObjects.add(new Ds3Object("DummyObject_3.txt", 1000));

        final GetBulkJobSpectraS3Request firstGetRequest =
                new GetBulkJobSpectraS3Request(BUCKET_NAME_1, firstGetObjects).withAggregating(true);
        final GetBulkJobSpectraS3Response firstGetResponse = client.getBulkJobSpectraS3(firstGetRequest);
        final UUID firstGetJobId = firstGetResponse.getMasterObjectList().getJobId();
        LOG.info("First aggregating bulk GET job (bucket-1) created: " + firstGetJobId);

        // Aggregating bulk GET from bucket 2 with objects 4-6 — should NOT return the same job ID
        final List<Ds3Object> secondGetObjects = new ArrayList<>();
        secondGetObjects.add(new Ds3Object("DummyObject_4.txt", 1000));
        secondGetObjects.add(new Ds3Object("DummyObject_5.txt", 1000));
        secondGetObjects.add(new Ds3Object("DummyObject_6.txt", 1000));

        final GetBulkJobSpectraS3Request secondGetRequest =
                new GetBulkJobSpectraS3Request(BUCKET_NAME_2, secondGetObjects).withAggregating(true);
        final GetBulkJobSpectraS3Response secondGetResponse = client.getBulkJobSpectraS3(secondGetRequest);
        final UUID secondGetJobId = secondGetResponse.getMasterObjectList().getJobId();
        LOG.info("Second aggregating bulk GET job (bucket-2) created: " + secondGetJobId);

        assertNotEquals(firstGetJobId, secondGetJobId,
                "Aggregating bulk GET requests to different buckets should NOT return the same job ID");

        LOG.info("Verified that aggregating GET jobs from different buckets have different job IDs.");
    }
}
