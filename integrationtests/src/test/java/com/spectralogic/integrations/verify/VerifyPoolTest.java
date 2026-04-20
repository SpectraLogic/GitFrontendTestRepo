package com.spectralogic.integrations.verify;

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

import static com.spectralogic.integrations.DatabaseUtils.corruptCheckSum;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_POOL_NAME;
import static com.spectralogic.integrations.TestConstants.STORAGE_DOMAIN_POOL_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VerifyPoolTest {
    private final static Logger LOG = Logger.getLogger( VerifyPoolTest.class );
    private static Ds3Client client;

    public static void updateUserDataPolicyPool(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();

        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> poolDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_POOL_NAME)).findFirst();
        if (poolDP.isEmpty()) {
            poolDP = Optional.ofNullable(createDataPolicy(client, DATA_POLICY_POOL_NAME));
        }

        clearPersistenceRules(client, poolDP.get().getId());
        UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_POOL_NAME);
        PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3Request =
                new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, storageDomainId, DataPersistenceRuleType.PERMANENT);
        PutDataPersistenceRuleSpectraS3Response rep = client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3Request);
        modifyUser(client, poolDP.get());
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            TestUtils.cleanSetUp(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testSingleCopyPool() throws IOException {
        final String bucketName = "pool_verify_bucket";
        String inputPath = "testFiles";
        LOG.info("Starting test : VerifyPoolTest" );
        try {

            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            PoolPartition poolPartition = findCreatePoolsPartition(client,"pool-partition");
            updateUserDataPolicyPool(client);
            helper.ensureBucketExists(bucketName);
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPathFiles = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPathFiles);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);

            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPathFiles));
            UUID currentJobId = job.getJobId();
            addJobName(client, "VerifyPoolTest", currentJobId);

            System.out.println("Waiting for PUT job to be completed.");
            isJobCompleted(client, currentJobId);

            LOG.info("VerifyPoolTest: PUT job completed." +  currentJobId);
            Assertions.assertEquals(5, getBlobCountOnPool(client, poolPartition.getId()));
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));

            LOG.info("VerifyPoolTest: Clearing cache");
            reclaimCache(client);
            corruptCheckSum();
            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));

            }
            VerifyBulkJobSpectraS3Request verifyBulkJobSpectraS3Request = new VerifyBulkJobSpectraS3Request(bucketName, objectList);
            VerifyBulkJobSpectraS3Response verifyJob = client.verifyBulkJobSpectraS3(verifyBulkJobSpectraS3Request);
            UUID verifyJobId = verifyJob.getMasterObjectListResult().getJobId();

            TestUtil.sleep(5000);

            System.out.println("IOM jobs created. Waiting for jobs to be completed ....");
            waitForJobsToComplete(client);
            GetCompletedJobsSpectraS3Request completedJobsSpectraS3Request = new GetCompletedJobsSpectraS3Request();
            GetCompletedJobsSpectraS3Response completedJobs = client.getCompletedJobsSpectraS3(completedJobsSpectraS3Request);
            GetCanceledJobSpectraS3Request canceledJobRequest = new GetCanceledJobSpectraS3Request(verifyJobId.toString());
            GetCanceledJobSpectraS3Response canceledJobResponse = client.getCanceledJobSpectraS3(canceledJobRequest);
            // 1- PUT
            assertEquals(1, completedJobs.getCompletedJobListResult().getCompletedJobs().size());
            assertEquals(verifyJobId, canceledJobResponse.getCanceledJobResult().getId());
        } catch (IOException | SQLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
