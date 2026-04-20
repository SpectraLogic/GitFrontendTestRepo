package com.spectralogic.integrations.replication.readpreferences;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Credentials;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearDs3ReplicationRules;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearDs3Targets;
import static com.spectralogic.integrations.TestConstants.*;
import static com.spectralogic.integrations.TestUtils.creds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PreferenceAfterNearlinePoolTest {
    final String bucketName = "single_copy_bucket";
    String inputPath = "testFiles";

    private final static Logger LOG = Logger.getLogger( PreferenceAfterNearlinePoolTest.class );
    public Ds3Client client;
    public Ds3Client remote_client;
    Credentials remote_creds = new Credentials(authId, secretKey);

    public void updateUserDataPolicy(Ds3Client client) throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();


        Ds3Target target = registerDockerDs3Target(client,authId, secretKey, TargetReadPreferenceType.AFTER_NEARLINE_POOL);
        PutDs3DataReplicationRuleSpectraS3Request putDs3DataReplicationRuleSpectraS3Request = new PutDs3DataReplicationRuleSpectraS3Request(singleCopyTapeDP.get().getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        putDs3DataReplicationRuleSpectraS3Request.withReplicateDeletes(false);
        client.putDs3DataReplicationRuleSpectraS3(putDs3DataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));

    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException {
        creds = new Credentials(authId, secretKey);
        client = Ds3ClientBuilder.create("http://localhost:8081", creds).build();
        remote_client = Ds3ClientBuilder.create("http://localhost:8082", remote_creds).build();
        if (client != null) {
            cleanupAllBuckets(client);
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            clearDs3Targets(client);
        }
        if (remote_client != null) {
            cleanupAllBuckets(remote_client);
            clearDs3ReplicationRules(remote_client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            clearDs3Targets(remote_client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
        if (remote_client != null) {
            cleanupAllBuckets(remote_client);
            clearDs3ReplicationRules(remote_client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(remote_client);
            remote_client.close();
        }
    }


    @Test
    public void testPutJobToDs3target() throws IOException, InterruptedException {
        LOG.info("Starting test: PreferenceAfterNearlinePoolTest using docker" );
        try {

            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            updateUserDataPolicy(client);
            updateUserDataPolicyRemote();
            reclaimCache(client);
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
            addJobName(client, "PreferenceAfterNearlinePoolTest", jobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();

            GetActiveJobsSpectraS3Response remoteActiveJobsResponse = remote_client.getActiveJobsSpectraS3( request );
            if (remoteActiveJobsResponse.getActiveJobListResult().getActiveJobs().size() > 0) {
                System.out.println("PreferenceAfterNearlinePoolTest: Waiting for remote target to finish job: " + remoteActiveJobsResponse.getActiveJobListResult().getActiveJobs().get(0).getId());
                TestUtil.sleep(1000);
            }
            System.out.println("Ds3ReplicationTapeTest: remote target to job finished: " + remoteActiveJobsResponse.getActiveJobListResult().getActiveJobs().get(0).getId());
            assertEquals(5, getBlobCountOnTape(remote_client, bucketName));
            System.out.println("PreferenceAfterNearlinePoolTest: Blob count passed:");
            if (remote_client != null) {
                final GetBucketResponse response = remote_client.getBucket(new GetBucketRequest(bucketName));
                assertEquals(5, response.getListBucketResult().getObjects().size());
                final List<Ds3Object> objectList = new ArrayList<>();
                for (final Contents contents : response.getListBucketResult().getObjects()) {
                    objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
                }
                GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
                GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = remote_client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
                List<BulkObject> details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
                for (BulkObject detail: details) {
                    assertEquals(1, detail.getPhysicalPlacement().getTapes().size() );
                }
                System.out.println("PreferenceAfterNearlinePoolTest:GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request passed:");
            }

            System.out.println("Ds3ReplicationTapeTestt:Local system checks:");

            isJobCompleted(client, jobId);


            System.out.println("PreferenceAfterNearlinePoolTest: Completed PUT" );
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
                assertEquals(1, detail.getPhysicalPlacement().getDs3Targets().size() );
            }
            //Trying Get

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

            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
            UUID readJobId = readJob.getJobId();

            addJobName(client, "ReadTapeTest", readJobId);
            readJob.transfer(new FileObjectGetter(outputPathFiles));

            isJobCompleted(client, readJobId);

            waitForJobsToComplete(remote_client);

            GetCompletedJobsSpectraS3Request getCompletedJobs = new GetCompletedJobsSpectraS3Request();
            List<CompletedJob> completedJobs = remote_client.getCompletedJobsSpectraS3(getCompletedJobs).getCompletedJobListResult().getCompletedJobs();
            assertEquals(2,completedJobs.size());


            LOG.info("Read job completed." +  readJobId);

        } catch (final Exception e) {
            LOG.error("Test Ds3ReplicationTapeTest failed", e);
            Assertions.fail( "Test Ds3ReplicationTapeTest failed: " + e.getMessage());
        }

    }

    private void updateUserDataPolicyRemote() throws IOException {
        //Remote client
        try {
            getTapesReady(remote_client);
            cleanupAllBuckets(remote_client);
            final GetDataPoliciesSpectraS3Response responsePolicy =
                    remote_client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
            List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
            Assertions.assertTrue(dataPolicies.size() > 1);
            Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                    .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();

            remote_client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
