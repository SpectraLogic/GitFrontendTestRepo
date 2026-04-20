package com.spectralogic.integrations.iom;

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
import java.util.stream.Collectors;

import static com.spectralogic.integrations.DatabaseUtils.removeBlobs;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.getCompletedJobs;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("LocalDevelopment")
@Tag("iomtest")
public class IOMMigrationUnavailableBlobsTest {
    private final static Logger LOG = Logger.getLogger( IOMMigrationUnavailableBlobsTest.class );
    UUID newPersistenceRuleId;
    final String bucketName = "iom_tape_bucket";
    String inputPath = "testFiles";
    private Ds3Client client;

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));
    }

    public void addCopyTarget(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        assertTrue(responsePolicy.getDataPolicyListResult().getDataPolicies().size() > 1);
        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeSecondSD = storageDomains.stream()
                .filter(sd -> sd.getName().equals("Tape Second Copy")).findFirst();
        UUID storageDomainId = tapeSecondSD.get().getId();
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();


        if (copyTapeTargetExists(client) != null) {
            newPersistenceRuleId = copyTapeTargetExists(client).getId();
            return;
        }
        PutDataPersistenceRuleSpectraS3Request putDs3DataPolicySpectraS3Request = new PutDataPersistenceRuleSpectraS3Request(singleCopyTapeDP.get().getId(), DataIsolationLevel.STANDARD,storageDomainId, DataPersistenceRuleType.PERMANENT);
        PutDataPersistenceRuleSpectraS3Response resp= client.putDataPersistenceRuleSpectraS3(putDs3DataPolicySpectraS3Request);
        newPersistenceRuleId = resp.getDataPersistenceRuleResult().getId();
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
        }
    }

    public DataPersistenceRule copyTapeTargetExists(Ds3Client client) throws IOException {
        GetDataPersistenceRulesSpectraS3Response rules = client.getDataPersistenceRulesSpectraS3(new GetDataPersistenceRulesSpectraS3Request());
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        assertTrue(responsePolicy.getDataPolicyListResult().getDataPolicies().size() > 1);
        List<StorageDomain> storageDomains = getStorageDomains(client);
        Optional<StorageDomain> tapeSecondSD = storageDomains.stream()
                .filter(sd -> sd.getName().equals("Tape Second Copy")).findFirst();
        UUID storageDomainId = tapeSecondSD.get().getId();
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        List<DataPersistenceRule> dpPersistenceRule = rules.getDataPersistenceRuleListResult().getDataPersistenceRules().stream()
                .filter(rule -> (rule.getStorageDomainId().equals(storageDomainId) && rule.getDataPolicyId().equals(singleCopyTapeDP.get().getId()))).collect(Collectors.toList());

        if (!dpPersistenceRule.isEmpty()) {
            return dpPersistenceRule.get(0);
        }
        return null;
    }



    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testPutJob() throws IOException, InterruptedException {
        LOG.info("Starting test: IOMMigrationUnavailableBlobsTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);

            helper.ensureBucketExists(bucketName);



            // Get the testFiles folder as a resource
            final URL testFilesUrl = getClass().getClassLoader().getResource("testFiles");
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputPath));

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );
            UUID jobId = activeJobsResponse.getActiveJobListResult().getActiveJobs().get(0).getId();
            LOG.info("Waiting for PUT job to complete ....");
            isJobCompleted(client, jobId);
            LOG.info("PUT job completed ....");

            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(CompletedJob::getId) ;
            assertNotNull(completedId.get());
            assertEquals(5, getBlobCountOnTape(client, bucketName));
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, response.getListBucketResult().getObjects().size());

            reclaimCache(client);
            removeBlobs(2);

            DataPersistenceRule rule = copyTapeTargetExists(client);
            if (rule !=null) {
                client.deleteDataPersistenceRuleSpectraS3(new DeleteDataPersistenceRuleSpectraS3Request(rule.getId()));
            }

            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            LOG.info("Adding Tape data replication rule to create IOM jobs....");
            addCopyTarget(client);
            TestUtil.sleep(1000);

            LOG.info("Checking if IOM jobs are created.");
            assertTrue(client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty());




            DeleteDataPersistenceRuleSpectraS3Request deleteDataPersistenceRuleSpectraS3Request = new DeleteDataPersistenceRuleSpectraS3Request(newPersistenceRuleId);
            client.deleteDataPersistenceRuleSpectraS3(deleteDataPersistenceRuleSpectraS3Request);

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
