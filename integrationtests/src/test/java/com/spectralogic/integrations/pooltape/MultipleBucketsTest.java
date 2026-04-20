package com.spectralogic.integrations.pooltape;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.bulk.PartialDs3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.apache.log4j.Logger;
import com.spectralogic.ds3client.models.common.Range;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_DUAL_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.*;
@Tag("LocalDevelopment")
public class MultipleBucketsTest  {
    private final static Logger LOG = Logger.getLogger( MultipleBucketsTest.class );

    final String firstBucketName = "multiple_bucket_one";
    final String secondBucketName = "multiple_bucket_two";
    final String thirdBucketName = "multiple_bucket_three";
    String outputPath = "output";
    private Ds3Client client;

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> dualCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_DUAL_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dualCopyTapeDP.get().getId() ));
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    public void testPutJobToTapes() throws IOException {
        LOG.info("Starting test: MultipleBucketsTest" );
        try {
            client = TestUtils.setTestParams();


            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);

            reclaimCache(client);
            cleanupBuckets(client, firstBucketName);
            helper.ensureBucketExists(firstBucketName);
            cleanupBuckets(client, secondBucketName);
            helper.ensureBucketExists(secondBucketName);
            cleanupBuckets(client, thirdBucketName);
            helper.ensureBucketExists(thirdBucketName);

            // Get the testFiles folder as a resource
            final URL testFilesUrl = getClass().getClassLoader().getResource("testFiles");
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());

            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);
            final Iterable<Ds3Object> objects2 = helper.listObjectsForDirectory(inputPath);
            final Iterable<Ds3Object> objects3 = helper.listObjectsForDirectory(inputPath);

            final Ds3ClientHelpers.Job firstJob = helper.startWriteJob(firstBucketName, objects);
            final Ds3ClientHelpers.Job secondJob = helper.startWriteJob(secondBucketName, objects2);
            final Ds3ClientHelpers.Job thirdJob = helper.startWriteJob(thirdBucketName, objects3);
            firstJob.transfer(new FileObjectPutter(inputPath));
            secondJob.transfer(new FileObjectPutter(inputPath));
            thirdJob.transfer(new FileObjectPutter(inputPath));

            UUID firstJobId = firstJob.getJobId();
            UUID secondJobId = secondJob.getJobId();
            UUID thirdJobId = thirdJob.getJobId();

            addJobName(client, "MultipleBucketsTest-FirstBucket", firstJobId);
            addJobName(client, "MultipleBucketsTest-SecondBucket", secondJobId);
            addJobName(client, "MultipleBucketsTest-ThirdBucket", thirdJobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3(request);




            LOG.info("Waiting for all PUT jobs to finish." );
            waitForJobsToComplete(client);


            LOG.info("PUT jobs completed ....");


            assertEquals(3,getCompletedJobs(client).size());


            // Check to make sure output exists, if not create the directory
            if (!Files.exists(Paths.get(outputPath))) {
                Files.createDirectory(Paths.get(outputPath));
            }

            final List<Ds3Object> filesGetFirstBucket = new ArrayList<>();
            filesGetFirstBucket.add(new PartialDs3Object("beowulf.txt", Range.byLength(0, 10)));
            filesGetFirstBucket.add(new PartialDs3Object("beowulf.txt", Range.byLength(250, 100)));
            filesGetFirstBucket.add(new Ds3Object("ulysses.txt"));

            final List<Ds3Object> filesGetSecondBucket = new ArrayList<>();
            filesGetSecondBucket.add(new Ds3Object("lesmis.txt"));

            // When the helper function writes the data to a file it will write it in the sorted over of the Ranges
            // where the range with the lowest starting offset is first.  Any ranges that overlap will be consolidated
            // into a single range, and all the ranges will be written to the same file.

            final Ds3ClientHelpers.Job job = helper.startReadJob(firstBucketName, filesGetFirstBucket);
            job.transfer(new FileObjectGetter(Paths.get(outputPath)));
            final Ds3ClientHelpers.Job job2 = helper.startReadJob(secondBucketName, filesGetSecondBucket);
            job2.transfer(new FileObjectGetter(Paths.get(outputPath)));
            LOG.info("Waiting for all PUT jobs to finish." );
            while (!client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                TestUtil.sleep(1000);
            }


        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        } finally {
            clearAllCompletedJobs(client);
            cleanupBuckets(client, firstBucketName);
            cleanupBuckets(client, secondBucketName);
            cleanupBuckets(client, thirdBucketName);
            TestUtils.cleanupFiles(outputPath);

            client.close();
        }
    }
}
