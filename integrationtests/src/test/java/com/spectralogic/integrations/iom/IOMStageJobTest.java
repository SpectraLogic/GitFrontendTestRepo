/**
 * This test is for Stage Jobs.
 */
package com.spectralogic.integrations.iom;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.ActiveJob;
import com.spectralogic.ds3client.models.BlobStoreTaskInformation;
import com.spectralogic.ds3client.models.CompletedJob;
import com.spectralogic.ds3client.models.Contents;
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
import java.util.stream.Collectors;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.*;

import static org.junit.jupiter.api.Assertions.*;
@Tag("LocalDevelopment")
@Tag("iomtest")
public class IOMStageJobTest {
    private final static Logger LOG = Logger.getLogger( IOMStageJobTest.class );
    final String bucketName = "iom_stage_bucket";
    Ds3Client client;
    String inputPath = "testFiles";

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        assertTrue(responsePolicy.getDataPolicyListResult().getDataPolicies().size() > 1);
        for ( final com.spectralogic.ds3client.models.DataPolicy model
                : responsePolicy.getDataPolicyListResult().getDataPolicies() )
        {
            if (model.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)) {
                client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(model.getId() ));
            }
        }
    }

    @Test
    public void testPutJobToTape() throws IOException, InterruptedException {
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

            // Get the list of files that are contained in the inputPath
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);

            // Create the write job with the bucket we want to write to and the list
            // of objects that will be written
            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);

            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPath));
            UUID jobId = job.getJobId();
            addJobName(client, "IOMStageJobTest", jobId);


            isJobCompleted(client, jobId);


            final List<Ds3Object> objectList = new ArrayList<>();
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            LOG.info("Reclaiming cache before sending Stage job request ....");
            ForceFullCacheReclaimSpectraS3Request forceFullCacheReclaimRequest = new ForceFullCacheReclaimSpectraS3Request();
            client.forceFullCacheReclaimSpectraS3(forceFullCacheReclaimRequest);

            LOG.info("Sending Stage job request ....");
            StageObjectsJobSpectraS3Request stageJobRequest = new StageObjectsJobSpectraS3Request(bucketName, objectList);
            client.stageObjectsJobSpectraS3(stageJobRequest);
            LOG.info("Waiting for the creation of Stage jobs ....");
            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( request );
            while (!checkStageJobs(client) && client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                TestUtil.sleep(30*1000);
            }
            LOG.info("IOM jobs created. Waiting for them to complete ....");
            while (!client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                TestUtil.sleep(30*1000);
            }
            LOG.info("IOM jobs completed....");
            assertTrue(checkStageJobs(client));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }


    public boolean checkStageJobs(Ds3Client client ) throws IOException {
        List<CompletedJob> completedJobs = getCompletedJobs(client);
        boolean hasStageGetJob = completedJobs.stream()
                .anyMatch(completedJob -> completedJob.getName().contains("Stage GET Job"));
        boolean hasStagePutJob = completedJobs.stream()
                .anyMatch(completedJob -> completedJob.getName().contains("Stage PUT Job"));
        return hasStageGetJob && hasStagePutJob;
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
}
