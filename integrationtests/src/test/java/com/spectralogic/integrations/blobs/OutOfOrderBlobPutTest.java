package com.spectralogic.integrations.blobs;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.revertBlobSize;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


public class OutOfOrderBlobPutTest {
    private final static Logger LOG = Logger.getLogger(OutOfOrderBlobPutTest.class);

    private static final String DATA_POLICY_NAME = "first-data-policy";
    private static final String BUCKET_NAME = "first-bucket";

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


    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testPutBlobsOutOfOrder() throws IOException, InterruptedException {
        LOG.info("Starting test: OutOfOrderBlobPutTest - testPutBlobsOutOfOrder");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Create data policy with blobbingEnabled=true, defaultBlobSize=1000
        final PutDataPolicySpectraS3Request createDpRequest = new PutDataPolicySpectraS3Request(DATA_POLICY_NAME);
        createDpRequest.withBlobbingEnabled(true);
        createDpRequest.withDefaultBlobSize(1000L);
        final PutDataPolicySpectraS3Response dpResponse = client.putDataPolicySpectraS3(createDpRequest);
        final DataPolicy dp = dpResponse.getDataPolicyResult();

        // Create persistence rule using tape
        final UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
        client.putDataPersistenceRuleSpectraS3(new PutDataPersistenceRuleSpectraS3Request(
                dp.getId(), DataIsolationLevel.STANDARD, storageDomainId, DataPersistenceRuleType.PERMANENT));

        // Create bucket with the data policy
        helper.ensureBucketExists(BUCKET_NAME, dp.getId());

        // Create a 10000-byte array to use as object data
        final byte[] objectData = new byte[10000];
        new Random().nextBytes(objectData);

        // Initiate bulk PUT job — object "my_object" of size 10000
        // With blobSize=1000, this creates 10 blobs (offsets 0,1000,2000,...,9000)
        final List<Ds3Object> objects = new ArrayList<>();
        objects.add(new Ds3Object("my_object", 10000));

        final PutBulkJobSpectraS3Response bulkResponse =
                client.putBulkJobSpectraS3(new PutBulkJobSpectraS3Request(BUCKET_NAME, objects));
        final MasterObjectList masterObjectList = bulkResponse.getMasterObjectList();
        final UUID jobId = masterObjectList.getJobId();
        LOG.info("Bulk PUT job initiated: " + jobId);

        // Collect all blobs across all chunks, then shuffle for random order
        final List<BulkObject> allBlobs = new ArrayList<>();
        for (final com.spectralogic.ds3client.models.Objects chunk : masterObjectList.getObjects()) {
            // Allocate the chunk
            final AllocateJobChunkSpectraS3Response allocateResponse =
                    client.allocateJobChunkSpectraS3(new AllocateJobChunkSpectraS3Request(chunk.getChunkId()));
            while (allocateResponse.getStatus() == AllocateJobChunkSpectraS3Response.Status.RETRYLATER) {
                TestUtil.sleep((int) (allocateResponse.getRetryAfterSeconds() * 1000L));
            }
            allBlobs.addAll(chunk.getObjects());
        }

        // Shuffle to randomize the order
        Collections.shuffle(allBlobs);
        LOG.info("Sending " + allBlobs.size() + " blobs in random order");

        // PUT each blob out of order — all should succeed with 200
        for (final BulkObject blob : allBlobs) {
            LOG.info("Putting blob: name=" + blob.getName()
                    + ", offset=" + blob.getOffset()
                    + ", length=" + blob.getLength());

            final ByteArrayInputStream blobStream = new ByteArrayInputStream(
                    objectData, (int) blob.getOffset(), (int) blob.getLength());

            final PutObjectRequest putRequest = new PutObjectRequest(
                    BUCKET_NAME, blob.getName(), jobId, blob.getOffset(), blob.getLength(), blobStream);
            // If the PUT fails, putObject throws FailedRequestException — no exception means 200 OK
            client.putObject(putRequest);
            LOG.info("Successfully PUT blob at offset " + blob.getOffset());
        }

        LOG.info("All blobs PUT successfully out of order.");

        // Wait for the job to complete
        waitForJobsToComplete(client);

        // Verify the object exists with the correct size
        final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(BUCKET_NAME));
        assertEquals(1, bucketResponse.getListBucketResult().getObjects().size());
        assertEquals(10000, bucketResponse.getListBucketResult().getObjects().get(0).getSize());
    }
}
