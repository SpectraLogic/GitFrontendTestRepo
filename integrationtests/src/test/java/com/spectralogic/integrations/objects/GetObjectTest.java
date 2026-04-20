package com.spectralogic.integrations.objects;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("LocalDevelopment")
public class GetObjectTest {
    final String bucketName = "single_copy_bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( GetObjectTest.class );
    private Ds3Client client;
    Path pathWithoutCache = Paths.get("output.txt");
    Path pathWithCache = Paths.get("output-with-cache.txt");
    Path pathWithoutCacheRange = Paths.get("output-without-cache-range.txt");
    Path pathWithCacheRange = Paths.get("output-with-cache-range.txt");



    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));
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
        Files.deleteIfExists(Paths.get("output.txt"));
        Files.deleteIfExists(Paths.get("output-with-cache.txt"));
        Files.deleteIfExists(Paths.get("output-without-cache-range.txt"));
        Files.deleteIfExists(Paths.get("output-with-cache-range.txt"));
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : GetObjectTest" );
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
            addJobName(client, "GetObjectTest", currentJobId);

            isJobCompleted(client, currentJobId);

            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(CompletedJob::getId);
            Assertions.assertNotNull(completedId.get());

            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, getBlobCountOnTape(client, bucketName));

            // We now need to generate the list of Ds3Objects that we want to get from DS3.
            assertEquals(5, response.getListBucketResult().getObjects().size());


            FileChannel channelWithCache = FileChannel.open(
                    pathWithCache,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, response.getListBucketResult().getObjects().get(0).getKey(), channelWithCache);
            getObjectRequest.withCachedOnly(true);
            getObjectRequest.withOffset(0);
            client.getObject(getObjectRequest);

            waitForJobsToComplete(client);
            assertEquals(2, getCompletedJobs(client).size(), "There should be one active job created.");
            reclaimCache(client);

            // Try to get an object not in cache, should throw an exception.
            FileChannel fileNotInCache = FileChannel.open(
                    pathWithCache,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            getObjectRequest = new GetObjectRequest(bucketName, response.getListBucketResult().getObjects().get(0).getKey(), fileNotInCache);
            getObjectRequest.withCachedOnly(true);
            getObjectRequest.withOffset(0);

            try {
                client.getObject(getObjectRequest);
                Assertions.fail("Should throw an exception when trying to get an object not in cache with cachedOnly flag.");
            } catch (final Exception e) {
                assertTrue(e.getMessage().contains("503"));
            }
            assertEquals(2, getCompletedJobs(client).size(), "No new jobs should be created.");

            reclaimCache(client);

            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);

            FileChannel channelWithoutCache = FileChannel.open(
                    pathWithoutCache,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            getObjectRequest = new GetObjectRequest(bucketName, response.getListBucketResult().getObjects().get(0).getKey(), channelWithoutCache);

            getObjectRequest.withOffset(0);
            getObjectRequest.withJob(readJob.getJobId());
            client.getObject(getObjectRequest);
            TestUtil.sleep(1000);

            assertEquals(-1, Files.mismatch(pathWithoutCache, pathWithCache), "Files are not identical");


            final Ds3ClientHelpers.Job readRangeJob = helper.startReadAllJob(bucketName);
            FileChannel rangeChannel = FileChannel.open(
                    pathWithoutCacheRange,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            getObjectRequest = new GetObjectRequest(bucketName, response.getListBucketResult().getObjects().get(0).getKey(), rangeChannel);
            getObjectRequest.withByteRanges(new Range(0, 10));
            getObjectRequest.withOffset(0);
            getObjectRequest.withJob(readRangeJob.getJobId());
            client.getObject(getObjectRequest);

            FileChannel rangeCacheChannel = FileChannel.open(
                    pathWithCacheRange,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            getObjectRequest = new GetObjectRequest(bucketName, response.getListBucketResult().getObjects().get(0).getKey(), rangeCacheChannel);
            getObjectRequest.withByteRanges(new Range(0, 10));
            getObjectRequest.withOffset(0);
            getObjectRequest.withCachedOnly(true);
            client.getObject(getObjectRequest);

            assertEquals(-1, Files.mismatch(pathWithCacheRange, pathWithoutCacheRange), "Files are not identical");

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
