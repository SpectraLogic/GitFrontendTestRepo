package com.spectralogic.integrations.replication;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.spectralogic.integrations.DatabaseUtils.createUser;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

//This test runs on a real BP.
public class ReplicationTest {
    final String bucketName = "single_copy_bucket";
    String inputPath = "testFiles";
    String authId = "QWRtaW5pc3RyYXRvcg==";
    String secretKey = "cFmchHNy";
    private final static Logger LOG = Logger.getLogger( ReplicationTest.class );
    private Ds3Client client;

    public void updateUserDataPolicy(Ds3Client client) throws IOException, SQLException {
        createUser(authId, secretKey);
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();

        clearDs3ReplicationRules(client, singleCopyTapeDP.get().getName()  );
        clearDs3Targets(client);
        //4u-13 credentials

        Ds3Target target = registerDs3Target(client,authId, secretKey);
        PutDs3DataReplicationRuleSpectraS3Request putDs3DataReplicationRuleSpectraS3Request = new PutDs3DataReplicationRuleSpectraS3Request(singleCopyTapeDP.get().getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        client.putDs3DataReplicationRuleSpectraS3(putDs3DataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));

    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            cleanupAllBuckets(client);
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupAllBuckets(client);
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

   // @Test
    public void testPutJobToDs3target() throws IOException, InterruptedException {
        LOG.info("Starting test: ReplicationTest to Ds3" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            updateUserDataPolicy(client);
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
            addJobName(client, "ReplicationTargetDs3Test", jobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

            Optional<UUID> filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;


            while (filteredId.isPresent()) {
                activeJobsResponse = client.getActiveJobsSpectraS3( request );
                filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .filter(model -> model.getId().equals(jobId)).findFirst().map(ActiveJob::getId) ;
                TestUtil.sleep(1000);


            }
            System.out.println("ReplicationTargetS3Test: Completed PUT: ReplicationTargetS3Test" );
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
        } catch (final Exception e) {
            LOG.error("Test ReplicationTargetS3Test failed", e);
            Assertions.fail( "Test ReplicationTargetS3Test failed: " + e.getMessage());
        }

    }
}
