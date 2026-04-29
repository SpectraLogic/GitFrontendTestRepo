package com.spectralogic.integrations.future;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.CompletedJob;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Range;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.revertBlobSize;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.getBlobCountOnTape;
import static com.spectralogic.integrations.Ds3ApiHelpers.getCompletedJobs;
import static com.spectralogic.integrations.Ds3ApiHelpers.reclaimCache;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// This was written to test
@Tag("LocalDevelopment")
public class GetObjectMultiBlobTest {
    final String bucketName = "single_copy_bucket";
    String inputPath = "SingleFileTest";
    private final static Logger LOG = Logger.getLogger( GetObjectMultiBlobTest.class );
    private Ds3Client client;

    Path pathWithoutCacheNakedRange = Paths.get("output-without-cache-naked-range.txt");

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));

        ModifyDataPolicySpectraS3Request dataPolicyRequest = new ModifyDataPolicySpectraS3Request(singleCopyTapeDP.get().getId() );
        dataPolicyRequest.withBlobbingEnabled(true);
        dataPolicyRequest.withDefaultBlobSize(50000L);
        client.modifyDataPolicySpectraS3(dataPolicyRequest);
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
        Files.deleteIfExists(Paths.get("output-without-cache-naked-range.txt"));
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            revertBlobSize(DATA_POLICY_TAPE_SINGLE_COPY_NAME);
            revertModifiedPartitions(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : GetObjectMultiBlobTest" );
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

            reclaimCache(client);
            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            UUID currentJobId = job.getJobId();
            addJobName(client, "GetObjectMultiBlobTest", currentJobId);
            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPath));



            isJobCompleted(client, currentJobId);

            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(CompletedJob::getId);
            Assertions.assertNotNull(completedId.get());

            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(6, getBlobCountOnTape(client, bucketName));

            // We now need to generate the list of Ds3Objects that we want to get from DS3.
            assertEquals(1, response.getListBucketResult().getObjects().size());

            reclaimCache(client);
            keepPartitionsWriteOnly(client);
            FileChannel rangeNakedChannel = FileChannel.open(
                    pathWithoutCacheNakedRange,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            GetObjectRequest getObjectRequest;
            getObjectRequest = new GetObjectRequest(bucketName, response.getListBucketResult().getObjects().get(0).getKey(), rangeNakedChannel);
            getObjectRequest.withByteRanges(new Range(50001, 50010));
            getObjectRequest.withOffset(50000);

            CompletableFuture<Void> getObjectFuture = CompletableFuture.runAsync(() -> {
                try {
                    client.getObject(getObjectRequest);
                } catch (IOException e) {
                    throw new RuntimeException("getObject failed in background thread", e);
                }
            });

            // 2. Wait a short moment to ensure the job has been created/started.
            Thread.sleep(1000);

            GetActiveJobsSpectraS3Request getActiveJobsSpectraS3Request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobs = client.getActiveJobsSpectraS3(getActiveJobsSpectraS3Request);

            while (activeJobs.getActiveJobListResult().getActiveJobs().size() < 1) {
                TestUtil.sleep(1000);
                activeJobs = client.getActiveJobsSpectraS3(getActiveJobsSpectraS3Request);
            }

            GetJobSpectraS3Request getJobSpectraS3Request = new GetJobSpectraS3Request(activeJobs.getActiveJobListResult().getActiveJobs().get(0).getId());
            GetJobSpectraS3Response jobResponse = client.getJobSpectraS3(getJobSpectraS3Request);
            GetActiveJobSpectraS3Request activeJobSpectraS3Request = new GetActiveJobSpectraS3Request(activeJobs.getActiveJobListResult().getActiveJobs().get(0).getId());
            GetActiveJobSpectraS3Response activeJobSpectraS3Response = client.getActiveJobSpectraS3(activeJobSpectraS3Request);

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
