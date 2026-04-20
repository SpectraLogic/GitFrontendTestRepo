package com.spectralogic.integrations.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.CloudUtils;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.CloudUtils.deleteAllContainers;
import static com.spectralogic.integrations.DatabaseUtils.markAzureBlobSuspect;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.addJobName;
import static com.spectralogic.integrations.Ds3ApiHelpers.reclaimCache;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


// Manually mark azure blob as suspect and make sure no IOM jobs are created.
@Tag("LocalDevelopment")
public class AzureSuspectBlobIOMJobTest {
    private Ds3Client client;
    final String bucketName = "gets3bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( AzureSuspectBlobIOMJobTest.class );
    AzureTarget target;
    BlobServiceClient blobServiceClient = CloudUtils.createAzuriteClient();
    BlobContainerClient containerClient;
    UUID dataPolicyId;


    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertFalse(dataPolicies.isEmpty());

        Optional<DataPolicy> singleCopyAzureDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_AZURE_SINGLE_COPY_NAME)).findFirst();

        if (!singleCopyAzureDP.isPresent()) {
            DataPolicy dp = createDataPolicy(client, DATA_POLICY_S3_SINGLE_COPY_NAME);
            dataPolicyId = dp.getId();
        } else {
            dataPolicyId = singleCopyAzureDP.get().getId();
        }



        target = registerAzuriteTarget(client);
        PutAzureDataReplicationRuleSpectraS3Request putAzureDataReplicationRuleSpectraS3Request = new PutAzureDataReplicationRuleSpectraS3Request(dataPolicyId, target.getId(), DataReplicationRuleType.PERMANENT);
        client.putAzureDataReplicationRuleSpectraS3(putAzureDataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicyId ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        deleteAllContainers(blobServiceClient);
        if (!blobServiceClient.getBlobContainerClient(bucketName).exists()) {
            containerClient = blobServiceClient.getBlobContainerClient(bucketName);
        } else {
            containerClient = blobServiceClient.createBlobContainer(bucketName);
        }

        client  = TestUtils.setTestParams();
        if (client != null) {
            cleanupBuckets(client, bucketName);
            clearAzureReplicationRules(client, DATA_POLICY_AZURE_SINGLE_COPY_NAME );
            clearAzureTargets(client);

            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupBuckets(client, bucketName);
            clearAzureReplicationRules(client, DATA_POLICY_AZURE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 25, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : AzureTargetSuspectBlobTest" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            updateUserDataPolicy(client);
            toggleIOM(client, true);

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

            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "AzureTargetSuspectBlobTest", currentJobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

            isJobCompleted(client, currentJobId);


            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));


            // We now need to generate the list of Ds3Objects that we want to get from DS3.
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            markAzureBlobSuspect();

            final URL resourcesUrl = getClass().getClassLoader().getResource("");
            assert resourcesUrl != null;

            String outputPath = "testFilesOutput";
            final Path resourcesPath = Paths.get(resourcesUrl.toURI());
            final Path outputPathFiles = resourcesPath.resolve(outputPath);


            //Output will be created in the build/classes
            try {
                Files.createDirectories(outputPathFiles);
                System.out.println("Created output directory: " + outputPathFiles.toAbsolutePath());
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + e.getMessage());
                throw new RuntimeException("Directory setup failed.", e);
            }

            reclaimCache(client);
            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
            UUID readJobId = readJob.getJobId();
            addJobName(client, "ReadAzuriteTest", readJobId);
            try{
                java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try {
                        readJob.transfer(new FileObjectGetter(outputPathFiles));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).get(2, TimeUnit.MINUTES);
            } catch (Exception e) {
                System.out.println("Error executing read"+ e);
                GetSuspectBlobAzureTargetsSpectraS3Request suspectBlobAzureTargetsSpectraS3Request = new GetSuspectBlobAzureTargetsSpectraS3Request();
                GetSuspectBlobAzureTargetsSpectraS3Response suspectBlobAzureTargetsSpectraS3Response = client.getSuspectBlobAzureTargetsSpectraS3(suspectBlobAzureTargetsSpectraS3Request);
                List<SuspectBlobAzureTarget> suspectBlobAzureTargets = suspectBlobAzureTargetsSpectraS3Response.getSuspectBlobAzureTargetListResult().getSuspectBlobAzureTargets();
                assertNotNull(suspectBlobAzureTargets, "Suspect blobs  created");

                request = new GetActiveJobsSpectraS3Request();
                //Wait for IOM jobs to be created.
                long startTime = System.currentTimeMillis();
                long maxWaitMs = 600_000; // 600 seconds

                LOG.info("Waiting for IOM jobs to be created (max 600 seconds)...");
                while (client.getActiveJobsSpectraS3(request)
                        .getActiveJobListResult().getActiveJobs().size() < 2) {
                    if (getCompletedJobs(client).size() >= 3) {
                        break;
                    }
                    if (System.currentTimeMillis() - startTime > maxWaitMs) {
                        fail("Timed out waiting for IOM jobs after 600 seconds");
                    }
                    TestUtil.sleep(1000);
                }
                activeJobsResponse = client.getActiveJobsSpectraS3( request );
                assertEquals(3, activeJobsResponse.getActiveJobListResult().getActiveJobs().size());
                
            }

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
