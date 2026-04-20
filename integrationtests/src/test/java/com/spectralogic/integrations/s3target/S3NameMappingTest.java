package com.spectralogic.integrations.s3target;

import com.azure.storage.blob.BlobContainerClient;
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

import com.spectralogic.integrations.CloudUtils;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.CloudUtils.deleteAllBuckets;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.isJobCompleted;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearDs3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.*;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.fail;

/*
    Test steps:
    1. Set up BlackPearl with an S3 cloud target.
    2. Add another Black pearl as a replication target and add the same cloud target.
    3. Complete a name mapping request for the primary Black Pearl.
    4. For the primary create a bucket with the data policy "Single Copy on Tape". Complete a PUT job.
    5. Verify if data is present in the mapped cloud bucket.
    6. Import the name-mapped bucket from secondary BP.


 */
public class S3NameMappingTest {

    private Ds3Client client;
    final String bucketName = "data-name-mapping-s3";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( S3NameMappingTest.class );
    S3Target target;
    S3Client s3Client = CloudUtils.createLocalStackClient();
    BlobContainerClient containerClient;
    UUID dataPolicyId;
    String mappingBucketName = "test-bucket-s3";
    UUID remote_dataPolicyId;
    public Ds3Client remote_client;
    Credentials remote_creds = new Credentials(authId, secretKey);

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertFalse(dataPolicies.isEmpty());

        Optional<DataPolicy> dataPolicy = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        dataPolicyId = dataPolicy.get().getId();
        target = registerS3LocalstackTarget(client);
        PutS3DataReplicationRuleSpectraS3Request putS3DataReplicationRuleSpectraS3Request = new PutS3DataReplicationRuleSpectraS3Request(dataPolicyId, target.getId(), DataReplicationRuleType.PERMANENT);
        client.putS3DataReplicationRuleSpectraS3(putS3DataReplicationRuleSpectraS3Request);
        ModifyS3TargetSpectraS3Request modifyS3TargetSpectraS3Request = new ModifyS3TargetSpectraS3Request(target.getId().toString());
        modifyS3TargetSpectraS3Request.withName("S3NameMappingTestTarget");
        client.modifyS3TargetSpectraS3(modifyS3TargetSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicyId ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        deleteAllBuckets(s3Client);
        client  = TestUtils.setTestParams();
        remote_client = Ds3ClientBuilder.create("http://localhost:8082", remote_creds).build();
        if (client != null) {
            cleanupBuckets(client, mappingBucketName);
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            clearS3Targets(client);

            TestUtils.cleanSetUp(client);
        }
        if (remote_client != null) {
            cleanupBuckets(remote_client, bucketName);
            cleanupBuckets(remote_client, mappingBucketName);
            clearDs3ReplicationRules(remote_client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            clearDs3Targets(remote_client);
            TestUtils.cleanSetUp(remote_client);
            FormatAllTapesSpectraS3Request formatAllTapesSpectraS3Request = new FormatAllTapesSpectraS3Request();
            remote_client.formatAllTapesSpectraS3(formatAllTapesSpectraS3Request);
            getTapesReady(remote_client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        cleanupBuckets(remote_client, bucketName);
        cleanupBuckets(client, bucketName);
        if (client != null) {

            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
        if (remote_client != null) {

            clearS3ReplicationRules(remote_client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(remote_client);
            remote_client.close();
        }
    }
    private UUID updateDataPolicyForClients(Ds3Client client) throws IOException {
        GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertFalse(dataPolicies.isEmpty());

        Optional<DataPolicy> dataPolicy = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        Assertions.assertTrue(dataPolicy.isPresent());
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicy.get().getId() ));
        return dataPolicy.get().getId();
    }
    @Test
    @Timeout(value = 25, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : S3NameMappingTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            dataPolicyId = updateDataPolicyForClients(client);
            remote_dataPolicyId = updateDataPolicyForClients(remote_client);

            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName);
            updateUserDataPolicy(client);
            PutS3TargetBucketNameSpectraS3Request putS3TargetBucketNameRequest = new PutS3TargetBucketNameSpectraS3Request(bucketName, mappingBucketName, target.getId());
            client.putS3TargetBucketNameSpectraS3(putS3TargetBucketNameRequest);
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
            addJobName(client, "S3NameMappingTest", currentJobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            isJobCompleted(client, currentJobId);


            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));


            // We now need to generate the list of Ds3Objects that we want to get from DS3.
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            S3Target remoteTarget = registerS3LocalstackTarget(remote_client);

            ModifyS3TargetSpectraS3Request modifyS3TargetSpectraS3Request = new ModifyS3TargetSpectraS3Request(remoteTarget.getId().toString());
            modifyS3TargetSpectraS3Request.withName("S3NameMappingTestTarget");
            remote_client.modifyS3TargetSpectraS3(modifyS3TargetSpectraS3Request);
            PutS3DataReplicationRuleSpectraS3Request putS3DataReplicationRuleSpectraS3Request = new PutS3DataReplicationRuleSpectraS3Request(remote_dataPolicyId, remoteTarget.getId(), DataReplicationRuleType.PERMANENT);
            remote_client.putS3DataReplicationRuleSpectraS3(putS3DataReplicationRuleSpectraS3Request);
            ImportS3TargetSpectraS3Request importS3TargetSpectraS3Request = new ImportS3TargetSpectraS3Request( mappingBucketName, "S3NameMappingTestTarget");
            importS3TargetSpectraS3Request.withDataPolicyId(remote_dataPolicyId);
            importS3TargetSpectraS3Request.withUserId(authId);
            remote_client.importS3TargetSpectraS3(importS3TargetSpectraS3Request);

            //  Wait a maximum of 600 seconds for tasks to appear
            long startTime = System.currentTimeMillis();
            long maxWaitMs = 600_000; // 600 seconds

            GetDataPlannerBlobStoreTasksSpectraS3Request getDataPlannerBlobStoreTasksSpectraS3Request = new GetDataPlannerBlobStoreTasksSpectraS3Request();
            getDataPlannerBlobStoreTasksSpectraS3Request.withFullDetails(true);
            GetDataPlannerBlobStoreTasksSpectraS3Response taskResponse = remote_client.getDataPlannerBlobStoreTasksSpectraS3(getDataPlannerBlobStoreTasksSpectraS3Request);

            System.out.println("Waiting for tasks to be created for the remote system.");
            TestUtil.sleep(1000);
            LOG.info("Waiting for  tasks to be completed in the remote system.");
            while (!taskResponse.getBlobStoreTasksInformationResult().getTasks().isEmpty()) {
                if (System.currentTimeMillis() - startTime > maxWaitMs) {
                    fail("Timed out waiting for task completion.");
                }
                TestUtil.sleep(1000);
                taskResponse = remote_client.getDataPlannerBlobStoreTasksSpectraS3(getDataPlannerBlobStoreTasksSpectraS3Request);
            }
            LOG.info("Confirming bucket contents in remote system.");

            GetBucketRequest getBucketRequest = new GetBucketRequest(bucketName);
            GetBucketResponse bucketResponse = remote_client.getBucket(getBucketRequest);
            bucketResponse.getListBucketResult().getObjects().size();
            Assertions.assertEquals(5, bucketResponse.getListBucketResult().getObjects().size());


        } catch (Exception e) {
            LOG.error("Test failed with exception ", e);
            Assertions.fail("Test failed with exception " + e.getMessage());
        }
    }
}
