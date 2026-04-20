package com.spectralogic.integrations.s3target;


import com.spectralogic.ds3client.Ds3Client;

import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.commands.spectrads3.notifications.PutObjectCachedNotificationRegistrationSpectraS3Request;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.quiescePartition;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static com.spectralogic.integrations.TestUtils.cleanupSimulator;
import static org.junit.jupiter.api.Assertions.assertEquals;

/*
    Test steps:
    1. Set up BlackPearl with an S3 cloud target.
    2. Create a bucket with the data policy "Single Copy on Tape". Complete a PUT job.
    3. Clear cache. Quiesce the partitions.
    4. Attempt reading data.
    5. Read should succeed from S3 target.

 */
// This test is to read data from S3 target after data is missing on tape due to partition failure.

public class ReadDataS3Test {
    private Ds3Client client;
    final String bucketName = "gets3bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( ReadDataS3Test.class );

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        clearS3ReplicationRules(client, singleCopyTapeDP.get().getName()  );
        clearS3Targets(client);
        S3Target target = registerS3LocalstackTarget(client);
        PutS3DataReplicationRuleSpectraS3Request putS3DataReplicationRuleSpectraS3Request = new PutS3DataReplicationRuleSpectraS3Request(singleCopyTapeDP.get().getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        client.putS3DataReplicationRuleSpectraS3(putS3DataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client  = TestUtils.setTestParams();
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            cleanupBuckets(client, bucketName);
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            getTapesReady(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 25, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : GetPartitionFailureTest" );
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
            PutObjectCachedNotificationRegistrationSpectraS3Request notificationRequest =
                    new PutObjectCachedNotificationRegistrationSpectraS3Request("GetTapeTestNotification");
            //client.putObjectCachedNotificationRegistrationSpectraS3(notificationRequest);
            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);

            job.transfer(new FileObjectPutter(inputPath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "GetPartitionFailureTest", currentJobId);

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
            Assertions.assertEquals(5, getBlobCountOnTape(client, bucketName));

            // We now need to generate the list of Ds3Objects that we want to get from DS3.
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            List<BulkObject> details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
            for (BulkObject detail: details) {
                assertEquals(1, detail.getPhysicalPlacement().getTapes().size() );
                assertEquals(1, detail.getPhysicalPlacement().getS3Targets().size() );
            }
            String outputPath = "testFilesOutput";
            final URL resourcesUrl = getClass().getClassLoader().getResource("");
            assert resourcesUrl != null;


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
            quiescePartitions(client);
           // quiesceTape();
           // cleanupSimulator();
            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
            UUID readJobId = readJob.getJobId();

            addJobName(client, "ReadTapeTest", readJobId);
            readJob.transfer(new FileObjectGetter(outputPathFiles));


            activeJobsResponse = client.getActiveJobsSpectraS3( request );
            Optional<UUID> readId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(readJobId)).findFirst().map(ActiveJob::getId) ;


            while (readId.isPresent()) {
                activeJobsResponse = client.getActiveJobsSpectraS3( request );
                readId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .filter(model -> model.getId().equals(readJobId)).findFirst().map(ActiveJob::getId) ;
                TestUtil.sleep(3000);
            }

            LOG.info("Read job completed." +  readJobId);


        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
