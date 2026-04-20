
package com.spectralogic.integrations.future;

import com.google.common.collect.Lists;
import com.spectralogic.ds3client.Ds3Client;

import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.*;



@Order(1)
public class TestMultipleJobs {
    private Ds3Client client;
    final String bucketName = "single_copy_bucket";
    String inputPath = "testFilesSet2";
    private final Logger LOG = Logger.getLogger( TestMultipleJobs.class );

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
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException {
        if (client != null) {
           // clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
           // TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    //@Test
    public void testPutJobToTape() throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        LOG.info("Starting test : TestMultipleJobs" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);


            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName);


            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            for (int i = 0; i < 1000; i++) {
                final byte[] content = new byte[100];
                new Random().nextBytes(content);
                Files.write(inputPath.resolve("file_" + i), content);
            }
            LOG.info("Files for  PUT job created." );
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);

            final List<UUID> jobIds = new ArrayList<>();
            for (final Ds3Object obj : objects) {
                final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, Collections.singletonList(obj));
                job.transfer(new FileObjectPutter(inputPath));
                final UUID currentJobId = job.getJobId();
                jobIds.add(currentJobId);
                addJobName(client, "MultipleJobsTest"+obj.getName(), currentJobId);
            }
            LOG.info("PUT jobs created. Waiting to complete" );
            final GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();

            boolean anyJobActive = true;
            while (anyJobActive) {
                GetActiveJobsSpectraS3Response readActiveJobsResponse = client.getActiveJobsSpectraS3(request);

                anyJobActive = readActiveJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .map(ActiveJob::getId)
                        .anyMatch(jobIds::contains);

                if (anyJobActive) {
                    TestUtil.sleep(2 * 60 * 1000);
                }
            }

            LOG.info("PUT jobs completed." );
            long putEndTime = System.currentTimeMillis();
            long putDuration = putEndTime - startTime;

            System.out.println("PUT jobs completed in " + (putDuration/1000) + " seconds");
            String outputPath = "testFilesOutput";
            final URL resourcesUrl = getClass().getClassLoader().getResource("");
            assert resourcesUrl != null;


            final Path resourcesPath = Paths.get(resourcesUrl.toURI());
            final Path outputPathFiles = resourcesPath.resolve(outputPath);


            //Output will be created in the build/classes
            try {
                Files.createDirectories(outputPathFiles);
                System.out.println("Created output directory: " + outputPathFiles.toAbsolutePath());
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + e.getMessage());
                throw new RuntimeException("Directory setup failed.", e);
            }

            reclaimCache(client);
            LOG.info("Cache cleared" );
            Set<UUID> readIds = new HashSet<>();
            GetObjectsDetailsSpectraS3Request objectDetailsSpectraS3Request = new GetObjectsDetailsSpectraS3Request();
            objectDetailsSpectraS3Request.withBucketId(bucketName);
            LOG.info("Going to create GET jobs ." );
            long getStartTime = System.currentTimeMillis();
            GetObjectsDetailsSpectraS3Response objectDetailsSpectraS3Response = client.getObjectsDetailsSpectraS3(objectDetailsSpectraS3Request);
            List<S3Object> objectList  = objectDetailsSpectraS3Response.getS3ObjectListResult().getS3Objects();
            for (S3Object obj : objectList) {
                final Ds3Object object = new Ds3Object(obj.getName());
                final Ds3ClientHelpers.Job readJob = helper.startReadJob(bucketName,  Lists.newArrayList(object));
                final UUID currentJobId = readJob.getJobId();
                addJobName(client, "ReadJob"+obj.getName(), currentJobId);
                readJob.transfer(new FileObjectGetter(outputPathFiles));
                final UUID readJobId = readJob.getJobId();
                readIds.add(readJobId);
            }
            LOG.info("GET jobs created.Waiting to complete" );
            final GetActiveJobsSpectraS3Request readRequest = new GetActiveJobsSpectraS3Request();

             anyJobActive = true;
            while (anyJobActive) {
                GetActiveJobsSpectraS3Response readActiveJobsResponse = client.getActiveJobsSpectraS3(readRequest);

                // Check if any of the readIds are still in the active jobs list
                anyJobActive = readActiveJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .map(ActiveJob::getId)
                        .anyMatch(readIds::contains);

                if (anyJobActive) {
                    TestUtil.sleep(3 * 60 * 1000);
                }
            }


            LOG.info("GET jobs completed." );

            long getEndTime = System.currentTimeMillis();
            long getDuration = getEndTime - getStartTime;
            System.out.println("GET completed in " + (getDuration/1000) + " milliseconds");

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            System.out.println("Test completed in " + (duration/1000) + " milliseconds");


        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}



