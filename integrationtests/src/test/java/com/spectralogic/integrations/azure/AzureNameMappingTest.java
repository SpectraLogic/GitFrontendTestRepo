package com.spectralogic.integrations.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.AzureTarget;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.ds3client.models.DataReplicationRuleType;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Credentials;
import com.spectralogic.integrations.CloudUtils;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

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

import static com.spectralogic.integrations.CloudUtils.deleteAllContainers;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.isJobCompleted;
import static com.spectralogic.integrations.Ds3ReplicationUtils.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearAzureReplicationRules;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("LocalDevelopment")
@Tag("cloudtest")
public class AzureNameMappingTest {
    private Ds3Client client;
    final String bucketName = "data-name-mapping-azure";
    String mappingBucketName = "test-bucket-azure";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( AzureNameMappingTest.class );
    AzureTarget target;
    BlobServiceClient blobServiceClient = CloudUtils.createAzuriteClient();
    BlobContainerClient containerClient;
    UUID dataPolicyId;
    UUID remote_dataPolicyId;
    public Ds3Client remote_client;
    Credentials remote_creds = new Credentials(authId, secretKey);

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertFalse(dataPolicies.isEmpty());

        Optional<DataPolicy> singleCopyAzureDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        dataPolicyId = singleCopyAzureDP.get().getId();
        target = registerAzuriteTarget(client);
        PutAzureDataReplicationRuleSpectraS3Request putAzureDataReplicationRuleSpectraS3Request = new PutAzureDataReplicationRuleSpectraS3Request(dataPolicyId, target.getId(), DataReplicationRuleType.PERMANENT);
        client.putAzureDataReplicationRuleSpectraS3(putAzureDataReplicationRuleSpectraS3Request);
        ModifyAzureTargetSpectraS3Request modifyAzureTargetSpectraS3Request = new ModifyAzureTargetSpectraS3Request(target.getId().toString());
        modifyAzureTargetSpectraS3Request.withName("AzureNameMappingTestTarget");
        client.modifyAzureTargetSpectraS3(modifyAzureTargetSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dataPolicyId ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        deleteAllContainers(blobServiceClient);

        client  = TestUtils.setTestParams();
        remote_client = Ds3ClientBuilder.create("http://localhost:8082", remote_creds).build();
        if (client != null) {
            cleanupAllBuckets(client);
            clearAzureReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            clearAzureTargets(client);

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
        deleteAllContainers(blobServiceClient);
        cleanupBuckets(client, bucketName);
        cleanupBuckets(remote_client, bucketName);
        if (client != null) {

            clearAzureReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
        if (remote_client != null) {
            clearDs3ReplicationRules(remote_client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
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
        LOG.info("Starting test : AzureNameMappingTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            dataPolicyId = updateDataPolicyForClients(client);
            remote_dataPolicyId = updateDataPolicyForClients(remote_client);

            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName);
            updateUserDataPolicy(client);
            PutAzureTargetBucketNameSpectraS3Request putAzureTargetBucketNameRequest = new PutAzureTargetBucketNameSpectraS3Request(bucketName, mappingBucketName, target.getId());
            client.putAzureTargetBucketNameSpectraS3(putAzureTargetBucketNameRequest);
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
            addJobName(client, "AzureNameMappingTest", currentJobId);

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
            AzureTarget remoteTarget = registerAzuriteTarget(remote_client);

            ModifyAzureTargetSpectraS3Request modifyAzureTargetSpectraS3Request = new ModifyAzureTargetSpectraS3Request(remoteTarget.getId().toString());
            modifyAzureTargetSpectraS3Request.withName("AzureNameMappingTestTarget");
            remote_client.modifyAzureTargetSpectraS3(modifyAzureTargetSpectraS3Request);
            PutAzureDataReplicationRuleSpectraS3Request putAzureDataReplicationRuleSpectraS3Request = new PutAzureDataReplicationRuleSpectraS3Request(remote_dataPolicyId, remoteTarget.getId(), DataReplicationRuleType.PERMANENT);
            remote_client.putAzureDataReplicationRuleSpectraS3(putAzureDataReplicationRuleSpectraS3Request);
            ImportAzureTargetSpectraS3Request importAzureTargetSpectraS3Request = new ImportAzureTargetSpectraS3Request("AzureNameMappingTestTarget", mappingBucketName);
            importAzureTargetSpectraS3Request.withDataPolicyId(remote_dataPolicyId);
            importAzureTargetSpectraS3Request.withUserId(authId);
            remote_client.importAzureTargetSpectraS3(importAzureTargetSpectraS3Request);

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
