package com.spectralogic.integrations.blobs;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
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

import static com.spectralogic.integrations.DatabaseUtils.revertBlobSize;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("LargeCache")
public class BlobCacheAllocationTest {
    private final static Logger LOG = Logger.getLogger(BlobCacheAllocationTest.class);

    private static final String FIRST_DATA_POLICY_NAME = "first-data-policy";
    private static final String SECOND_DATA_POLICY_NAME = "second-data-policy";
    private static final String BIG_BLOB_DATA_POLICY_NAME = "big-blob-dp";

    private static final String FIRST_BUCKET_NAME = "first-bucket";
    private static final String SECOND_BUCKET_NAME = "second-bucket";
    private static final String BIG_BLOB_BUCKET_NAME = "big-blob-bucket";

    private Ds3Client client;
    private final List<UUID> createdDataPolicyIds = new ArrayList<>();

    private void cleanupDataPolicies() throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        for ( DataPolicy dataPolicy: dataPolicies) {
            if (dataPolicy.getName().equals(FIRST_DATA_POLICY_NAME) ||
                    dataPolicy.getName().equals(SECOND_DATA_POLICY_NAME)||
                    dataPolicy.getName().equals(BIG_BLOB_DATA_POLICY_NAME)) {
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
            cleanupBuckets(client, FIRST_BUCKET_NAME);
            cleanupBuckets(client, SECOND_BUCKET_NAME);
            cleanupBuckets(client, BIG_BLOB_BUCKET_NAME);
            cleanupDataPolicies();
            TestUtils.cleanSetUp(client);
            getTapesReady(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupBuckets(client, FIRST_BUCKET_NAME);
            cleanupBuckets(client, SECOND_BUCKET_NAME);
            cleanupBuckets(client, BIG_BLOB_BUCKET_NAME);
            cleanupDataPolicies();
            clearAllJobs(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    private DataPolicy createDataPolicyWithBlobSize(final String name, final long blobSize) throws IOException {
        final PutDataPolicySpectraS3Request createRequest = new PutDataPolicySpectraS3Request(name);
        createRequest.withBlobbingEnabled(true);
        createRequest.withDefaultBlobSize(blobSize);
        final PutDataPolicySpectraS3Response createResponse = client.putDataPolicySpectraS3(createRequest);
        final DataPolicy dp = createResponse.getDataPolicyResult();
        createdDataPolicyIds.add(dp.getId());
        return dp;
    }


    private void createTapePersistenceRule(final UUID dataPolicyId) throws IOException {
        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        final PutDataPersistenceRuleSpectraS3Request request = new PutDataPersistenceRuleSpectraS3Request(
                dataPolicyId, DataIsolationLevel.STANDARD, storageDomainId, DataPersistenceRuleType.PERMANENT);
        client.putDataPersistenceRuleSpectraS3(request);
    }


    private Path createTempFileWithSize(final String fileName, final long sizeInBytes) throws IOException {
        final Path tempDir = Files.createTempDirectory("blob-test-" + fileName);
        tempDir.toFile().deleteOnExit();
        final File file = new File(tempDir.toFile(), fileName);
        file.deleteOnExit();
        try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(sizeInBytes);
        }
        return tempDir;
    }


    private UUID bulkPutObject(final Ds3ClientHelpers helper, final String bucketName,
                               final String objectName, final long objectSize) throws IOException {
        final Path tempDir = createTempFileWithSize(objectName, objectSize);
        final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(tempDir);
        final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
        job.transfer(new FileObjectPutter(tempDir));
        return job.getJobId();
    }

    public static void getBlobCount(Ds3Client client ) throws IOException {
        GetTapesSpectraS3Request getTapesSpectraS3Request = new GetTapesSpectraS3Request();
        GetTapesSpectraS3Response getTapesSpectraS3Response = client.getTapesSpectraS3( getTapesSpectraS3Request);
        int blobCount1 = 0;
        int blobCount2 = 0;
        int blobCount3 = 0;
        for (Tape tape : getTapesSpectraS3Response.getTapeListResult().getTapes()) {
            UUID tapeId = tape.getId();
            GetBlobsOnTapeSpectraS3Request getBlobsOnTapeSpectraS3Request = new GetBlobsOnTapeSpectraS3Request(tapeId);
            GetBlobsOnTapeSpectraS3Response getBlobsOnTapeSpectraS3Response = client.getBlobsOnTapeSpectraS3(getBlobsOnTapeSpectraS3Request);
            if (!getBlobsOnTapeSpectraS3Response.getBulkObjectListResult().getObjects().isEmpty()) {
                List<BulkObject> objects = getBlobsOnTapeSpectraS3Response.getBulkObjectListResult().getObjects();
                blobCount1 += Math.toIntExact(objects.stream().filter(obj -> obj.getName().equals("first-object")).count());
                blobCount2 += Math.toIntExact(objects.stream().filter(obj -> obj.getName().equals("second-object")).count());
                blobCount3 += Math.toIntExact(objects.stream().filter(obj -> obj.getName().equals("big-blob-object")).count());

            }

        }
        assertEquals(10, blobCount1);
        assertEquals(3, blobCount2);
        assertEquals(11, blobCount3);

    }


    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testPutSeveralObjectsAndVerifyBlobbing() throws IOException, InterruptedException {
        LOG.info("Starting test: BlobCacheAllocationTest - testPutSeveralObjectsAndVerifyBlobbing");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Create data policies with different blob sizes
        // first-data-policy: blobSize=1000
        //   -> first-object (10000 bytes) = ceil(10000/1000) = 10 blobs
        // second-data-policy: blobSize=2000
        //   -> second-object (4001 bytes) = ceil(4001/2000) = 3 blobs
        // big-blob-dp: blobSize=999999
        //   -> big-blob-object (10000000 bytes) = ceil(10000000/999999) = 11 blobs
        // Total: 10 + 3 + 11 = 24 blobs
        final DataPolicy firstDP = createDataPolicyWithBlobSize(FIRST_DATA_POLICY_NAME, 1000L);
        createTapePersistenceRule(firstDP.getId());

        final DataPolicy secondDP = createDataPolicyWithBlobSize(SECOND_DATA_POLICY_NAME, 2000L);
        createTapePersistenceRule(secondDP.getId());

        final DataPolicy bigBlobDP = createDataPolicyWithBlobSize(BIG_BLOB_DATA_POLICY_NAME, 999999L);
        createTapePersistenceRule(bigBlobDP.getId());

        // Create buckets with their respective data policies
        helper.ensureBucketExists(FIRST_BUCKET_NAME, firstDP.getId());
        helper.ensureBucketExists(SECOND_BUCKET_NAME, secondDP.getId());
        helper.ensureBucketExists(BIG_BLOB_BUCKET_NAME, bigBlobDP.getId());

        // Force full cache reclaim
        reclaimCache(client);

        // Bulk PUT objects
        bulkPutObject(helper, FIRST_BUCKET_NAME, "first-object", 10000);
        bulkPutObject(helper, SECOND_BUCKET_NAME, "second-object", 4001);
        bulkPutObject(helper, BIG_BLOB_BUCKET_NAME, "big-blob-object", 10000000);

        // Wait for all PUT jobs to complete
        waitForJobsToComplete(client);
        LOG.info("All PUT jobs completed.");

        // Verify objects exist with correct sizes
        GetBucketResponse firstBucketResponse = client.getBucket(new GetBucketRequest(FIRST_BUCKET_NAME));
        assertEquals(1, firstBucketResponse.getListBucketResult().getObjects().size());
        assertEquals(10000, firstBucketResponse.getListBucketResult().getObjects().get(0).getSize());

        final GetBucketResponse secondBucketResponse = client.getBucket(new GetBucketRequest(SECOND_BUCKET_NAME));
        assertEquals(1, secondBucketResponse.getListBucketResult().getObjects().size());
        assertEquals(4001, secondBucketResponse.getListBucketResult().getObjects().get(0).getSize());

        final GetBucketResponse bigBlobBucketResponse = client.getBucket(new GetBucketRequest(BIG_BLOB_BUCKET_NAME));
        assertEquals(1, bigBlobBucketResponse.getListBucketResult().getObjects().size());
        assertEquals(10000000, bigBlobBucketResponse.getListBucketResult().getObjects().get(0).getSize());

        getBlobCount(client);

    }
}
