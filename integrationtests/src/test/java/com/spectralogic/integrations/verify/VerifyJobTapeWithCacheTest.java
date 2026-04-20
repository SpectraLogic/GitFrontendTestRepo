package com.spectralogic.integrations.verify;

import com.spectralogic.ds3client.Ds3Client;

import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;

import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.ds3client.models.bulk.Ds3Object;

import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.corruptCheckSum;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

// https://jira.spectralogic.com/browse/EMPROD-6632?filter=-1
public class VerifyJobTapeWithCacheTest {
    private Ds3Client client;
    final String bucketName = "single_copy_bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( VerifyJobTapeWithCacheTest.class );

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();

        modifyUser(client, singleCopyTapeDP.get());

    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client  = TestUtils.setTestParams();
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            getTapesReady(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 25, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : VerifyJobTapeWithCacheTest" );
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

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);

            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "VerifyJobTapeWithCacheTest", currentJobId);

            isJobCompleted(client, currentJobId);

            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, getBlobCountOnTape(client, bucketName));
            corruptCheckSum();

            final List<Ds3Object> objectList = new ArrayList<>();
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }

            VerifyBulkJobSpectraS3Request verifyBulkJobSpectraS3Request = new VerifyBulkJobSpectraS3Request(bucketName, objectList);
            VerifyBulkJobSpectraS3Response verifyJob = client.verifyBulkJobSpectraS3(verifyBulkJobSpectraS3Request);
            UUID verifyJobId = verifyJob.getMasterObjectListResult().getJobId();

            GetActiveJobsSpectraS3Request getActiveJobsSpectraS3Request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobs = client.getActiveJobsSpectraS3(getActiveJobsSpectraS3Request);
            System.out.println("Waiting for IOM jobs to be created ....");
            while (activeJobs.getActiveJobListResult().getActiveJobs().size() < 2) {
                Thread.sleep(1000);
                activeJobs = client.getActiveJobsSpectraS3(getActiveJobsSpectraS3Request);
            }
            System.out.println("IOM jobs created. Waiting for IOM jobs to be completed ....");
            waitForJobsToComplete(client);
            GetCompletedJobsSpectraS3Request completedJobsSpectraS3Request = new GetCompletedJobsSpectraS3Request();
            GetCompletedJobsSpectraS3Response completedJobs = client.getCompletedJobsSpectraS3(completedJobsSpectraS3Request);
            GetCanceledJobSpectraS3Request canceledJobRequest = new GetCanceledJobSpectraS3Request(verifyJobId.toString());
            GetCanceledJobSpectraS3Response canceledJobResponse = client.getCanceledJobSpectraS3(canceledJobRequest);
            // 1- PUT , 2 - IOM
            assertEquals(3, completedJobs.getCompletedJobListResult().getCompletedJobs().size());
            assertEquals(verifyJobId, canceledJobResponse.getCanceledJobResult().getId());

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
