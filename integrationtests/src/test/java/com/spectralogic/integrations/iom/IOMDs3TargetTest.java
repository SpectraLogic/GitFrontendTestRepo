package com.spectralogic.integrations.iom;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Credentials;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.getCompletedJobs;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("LocalDevelopment")
@Tag("iomtest")
public class IOMDs3TargetTest {
    private final static Logger LOG = Logger.getLogger( IOMDs3TargetTest.class );
    final String bucketName = "iom-test-bucket";
    String inputPath = "testFiles";
    private Ds3Client client;
    public Ds3Client remote_client;
    Credentials remote_creds = new Credentials(authId, secretKey);

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));
    }

    public void addCopyTarget(Ds3Client client) throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();

        Ds3Target target = registerDockerDs3Target(client,authId, secretKey, null);
        PutDs3DataReplicationRuleSpectraS3Request putDs3DataReplicationRuleSpectraS3Request = new PutDs3DataReplicationRuleSpectraS3Request(singleCopyTapeDP.get().getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        client.putDs3DataReplicationRuleSpectraS3(putDs3DataReplicationRuleSpectraS3Request);
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        remote_client = Ds3ClientBuilder.create("http://localhost:8082", remote_creds).build();
        if (client != null) {
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            clearDs3Targets(client);
            TestUtils.cleanSetUp(client);
        }
        if (remote_client != null) {
            clearDs3ReplicationRules(remote_client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            clearDs3Targets(remote_client);
            TestUtils.cleanSetUp(remote_client);
            getTapesReady(remote_client);
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
            clearDs3ReplicationRules(remote_client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(remote_client);
            remote_client.close();
        }
    }

    private void updateUserDataPolicyRemote() throws IOException {
        //Remote client
        try {
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

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    public void testPutJob() throws IOException, InterruptedException {
        LOG.info("Starting test: IOMAddDs3TargetTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);
            updateUserDataPolicyRemote();
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

            UUID currentJobId = job.getJobId();
            addJobName(client, "IOMAddDs3TargetTest", currentJobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );

            isJobCompleted(client, currentJobId);


            System.out.println("IOMAddDs3TargetTest: Put Job is complete.");

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
                assertFalse(detail.getPhysicalPlacement().getTapes().isEmpty(), "Tape copies should be equal or greater than 1");
            }
            LOG.info("Adding DS3 replication target to create IOM jobs....");
            addCopyTarget(client);
            LOG.info("Waiting for IOM Jobs to be created.");
            while (client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep(1000);
            }
            LOG.info("IOM Jobs created.Waiting for them to complete...");
            while (!client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep(1000);
            }

            List<CompletedJob> completedJobs = getCompletedJobs(client);
            boolean hasIOMGetJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM GET Job"));
            boolean hasIOMPutJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM PUT Job"));
            assertTrue(hasIOMGetJob);
            assertTrue(hasIOMPutJob);
            getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
            getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
            for (BulkObject detail: details) {
                assertEquals(1, detail.getPhysicalPlacement().getDs3Targets().size() );
            }
        } catch (IOException | URISyntaxException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
