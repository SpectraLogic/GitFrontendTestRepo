package com.spectralogic.integrations.s3target.datacorruption;

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
import software.amazon.awssdk.services.s3.S3Client;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.addJobName;
import static com.spectralogic.integrations.Ds3ApiHelpers.getCompletedJobs;
import static com.spectralogic.integrations.Ds3ApiHelpers.reclaimCache;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_S3_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/*
    Test steps:
    1. Set up BlackPearl with an S3 cloud target.
    2. Create a bucket with the data policy "Single Copy on S3". Complete a PUT job.
    3. Delete one object from the cloud bucket.
    4. Clear cache.
    5. Attempt reading from the cloud bucket.
    6. Read should fail. Corrupted blob should be marked suspect and IOM jobs should be created.

 */
public class S3TargetSuspectBlobTest {
    private Ds3Client client;
    final String bucketName = "gets3bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( S3TargetSuspectBlobTest.class );
    S3Target target;
    UUID dataPolicyId;
    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertFalse(dataPolicies.isEmpty());

        Optional<DataPolicy> singleCopyS3DP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_S3_SINGLE_COPY_NAME)).findFirst();

        if (!singleCopyS3DP.isPresent()) {
            DataPolicy dp = createDataPolicy(client, DATA_POLICY_S3_SINGLE_COPY_NAME);
            dataPolicyId = dp.getId();
        } else {
            dataPolicyId = singleCopyS3DP.get().getId();
        }

        target = registerS3LocalstackTarget(client);
        PutS3DataReplicationRuleSpectraS3Request putS3DataReplicationRuleSpectraS3Request = new PutS3DataReplicationRuleSpectraS3Request(dataPolicyId, target.getId(), DataReplicationRuleType.PERMANENT);
        client.putS3DataReplicationRuleSpectraS3(putS3DataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicyId ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client  = TestUtils.setTestParams();
        cleanupBuckets(client, bucketName);
        clearS3ReplicationRules(client, DATA_POLICY_S3_SINGLE_COPY_NAME );
        clearS3Targets(client);
        TestUtils.cleanSetUp(client);
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupBuckets(client, bucketName);
            clearS3ReplicationRules(client, DATA_POLICY_S3_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 25, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : S3TargetSuspectBlobTest" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            updateUserDataPolicy(client);

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
            addJobName(client, "S3TargetSuspectBlobTest", currentJobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

            Optional<UUID> filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(ActiveJob::getId) ;


            while (!filteredId.isEmpty()) {
                activeJobsResponse = client.getActiveJobsSpectraS3( request );
                filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .filter(model -> model.getId().equals(currentJobId)).findFirst().map(ActiveJob::getId) ;
                TestUtil.sleep(1000);
            }

            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(CompletedJob::getId) ;
            Assertions.assertNotNull(completedId.get());

            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));


            // We now need to generate the list of Ds3Objects that we want to get from DS3.
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            S3Client s3Client = CloudUtils.createLocalStackClient();
            CloudUtils.deleteObject(s3Client, bucketName);

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
            addJobName(client, "ReadS3TargetSuspectBlobTest", readJobId);
            try{
                CompletableFuture.runAsync(() -> {
                    try {
                        readJob.transfer(new FileObjectGetter(outputPathFiles));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).get(2, TimeUnit.MINUTES);
            } catch (Exception e) {
                System.out.println("Error executing read"+ e);
                GetSuspectBlobS3TargetsSpectraS3Request getSuspectBlobS3TargetsSpectraS3Request = new GetSuspectBlobS3TargetsSpectraS3Request();
                GetSuspectBlobS3TargetsSpectraS3Response getSuspectBlobS3TargetsSpectraS3Response = client.getSuspectBlobS3TargetsSpectraS3(getSuspectBlobS3TargetsSpectraS3Request);
                List<SuspectBlobS3Target> suspectBlobS3Targets = getSuspectBlobS3TargetsSpectraS3Response.getSuspectBlobS3TargetListResult().getSuspectBlobS3Targets();
                assertNotNull(suspectBlobS3Targets, "Suspect blobs not created");
                try {
                    request = new GetActiveJobsSpectraS3Request();
                    activeJobsResponse = client.getActiveJobsSpectraS3( request );
                    assertEquals(3, activeJobsResponse.getActiveJobListResult().getActiveJobs().size());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

            }

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
