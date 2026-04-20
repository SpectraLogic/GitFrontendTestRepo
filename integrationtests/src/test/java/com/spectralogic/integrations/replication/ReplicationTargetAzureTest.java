package com.spectralogic.integrations.replication;


import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.BlockListType;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;

import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;

@Order(7)
public class ReplicationTargetAzureTest {
    private final static Logger LOG = Logger.getLogger( ReplicationTargetAzureTest.class );
    final String bucketName = "azure-bucket";
    String inputPath = "testFiles";
    private Ds3Client client;
    BlobContainerClient containerClient;

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        clearAzureReplicationRules(client, singleCopyTapeDP.get().getName()  );
        clearAzureTargets(client);
        AzureTarget target = registerAzuriteTarget(client);
        PutAzureDataReplicationRuleSpectraS3Request putAzureDataReplicationRuleSpectraS3Request = new PutAzureDataReplicationRuleSpectraS3Request(singleCopyTapeDP.get().getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        client.putAzureDataReplicationRuleSpectraS3(putAzureDataReplicationRuleSpectraS3Request);
        modifyUser(client, singleCopyTapeDP.get());

    }
    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            cleanupAllBuckets(client);
            clearAzureReplicationRules(client);
            clearAzureTargets(client);
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearAzureReplicationRules(client);
            cleanupAllBuckets(client);
            clearAzureTargets(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    public void getBlobBlockCommitment(String blobName) {
        System.out.println("\n--- Block Status for: " + blobName + " ---");
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        if (!blobClient.exists()) {
            System.out.println("ERROR: Blob not found.");
            return;
        }

        try {
            // FIX: Simplified the listBlocks call to only include the required BlockListType argument,
            // resolving the "No candidates found" compilation error.
            // This is the Java SDK equivalent of the 'az storage blob get-block-list --block-list-type all' command.
            BlockList blockList = blobClient.getBlockBlobClient().listBlocks(BlockListType.ALL);

            long committedCount = blockList.getCommittedBlocks().size();
            long uncommittedCount = blockList.getUncommittedBlocks().size();
            long totalCommittedSize = blockList.getCommittedBlocks().stream().mapToLong(Block::getSizeLong).sum();
            long totalUncommittedSize = blockList.getUncommittedBlocks().stream().mapToLong(Block::getSizeLong).sum();

            System.out.println("Status: SUCCESS (Block list retrieved successfully)");
            System.out.printf("Blob Type: %s%n", blobClient.getProperties().getBlobType());
            System.out.printf("Total Size: %,d bytes%n", blobClient.getBlockBlobClient().getProperties().getCommittedBlockCount());
            System.out.println("----------------------------------------");
            System.out.printf("Committed Blocks: %,d (Total Size: %,d bytes)%n", committedCount, totalCommittedSize);
            System.out.printf("Uncommitted Blocks: %,d (Total Size: %,d bytes)%n", uncommittedCount, totalUncommittedSize);

        } catch (Exception e) {
            // This usually happens if the blob is not a BlockBlob or due to a connection error.
            System.err.println("Failed to retrieve block list for " + blobName + ". Error: " + e.getMessage());
        }
    }
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testPutJobToAzureTarget() throws IOException, InterruptedException {
        LOG.info("Starting test: ReplicationTargetAzureTest" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);

            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName);

            // Get the testFiles folder as a resource
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);


            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputPath));
            UUID jobId = job.getJobId();
            addJobName(client, "ReplicationTargetAzureTest", jobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

            Optional<UUID> filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;


            while (!filteredId.isEmpty()) {
                activeJobsResponse = client.getActiveJobsSpectraS3( request );
                filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;

                TestUtil.sleep(1000);
            }

            System.out.println("ReplicationTargetAzureTest: Completed PUT: ReplicationTargetAzureTest" );
            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(CompletedJob::getId) ;
            assertNotNull(completedId.get());
            assertEquals(5, getBlobCountOnTape(client, bucketName));

            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, response.getListBucketResult().getObjects().size());

            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            List<BulkObject> details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
            for (BulkObject detail: details) {
                assertEquals(1, detail.getPhysicalPlacement().getTapes().size() );
                assertEquals(1, detail.getPhysicalPlacement().getAzureTargets().size() );
            }
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(AZURITE_CONNECTION_STRING)
                    .buildClient();
            containerClient = blobServiceClient.getBlobContainerClient(bucketName);
            containerClient.listBlobs().forEach(blobItem -> {
                String name = blobItem.getName();
                Long size = blobItem.getProperties() != null ? blobItem.getProperties().getContentLength() : 0L;
                System.out.printf("%-60s %,15d%n", name, size);
                getBlobBlockCommitment(name);
            });

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}

