package com.spectralogic.integrations.bulk;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.helpers.options.WriteJobOptions;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


public class CloseAggregatingJobTest {
    private final static Logger LOG = Logger.getLogger(CloseAggregatingJobTest.class);

    private static final String BUCKET_NAME = "close-aggregate-bucket";

    private Ds3Client client;

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
            cleanupBuckets(client, BUCKET_NAME);
            clearAllJobs(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    private void updateUserDataPolicy() throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3(new GetDataPoliciesSpectraS3Request());
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        assertTrue(singleCopyTapeDP.isPresent());
        client.modifyUserSpectraS3(
                new ModifyUserSpectraS3Request(authId)
                        .withDefaultDataPolicyId(singleCopyTapeDP.get().getId()));
    }

    private Path createTempDirWithFiles(final String[] fileNames, final long sizeInBytes) throws IOException {
        final Path tempDir = Files.createTempDirectory("close-agg-test");
        tempDir.toFile().deleteOnExit();
        for (final String fileName : fileNames) {
            final File file = new File(tempDir.toFile(), fileName);
            file.deleteOnExit();
            try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(sizeInBytes);
            }
        }
        return tempDir;
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testAggregateAndCloseJob() throws IOException, InterruptedException {
        LOG.info("Starting test: CloseAggregatingJobTest - testAggregateAndCloseJob");
        final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

        // Set user's default data policy so bucket creation doesn't fail
        updateUserDataPolicy();

        // Create bucket
        helper.ensureBucketExists(BUCKET_NAME);

        // First aggregating bulk PUT with obj1.txt, obj2.txt
        final String[] firstFiles = {"obj1.txt", "obj2.txt"};
        final Path firstTempDir = createTempDirWithFiles(firstFiles, 1000);
        final Iterable<Ds3Object> firstObjects = helper.listObjectsForDirectory(firstTempDir);

        final Ds3ClientHelpers.Job firstJob = helper.startWriteJob(BUCKET_NAME, firstObjects,
                WriteJobOptions.create().withAggregating());
        firstJob.transfer(new FileObjectPutter(firstTempDir));
        final UUID jobId = firstJob.getJobId();
        LOG.info("First aggregating PUT job created and transferred: " + jobId);

        // Second aggregating bulk PUT with obj3.txt, obj4.txt — should aggregate into same job
        final String[] secondFiles = {"obj3.txt", "obj4.txt"};
        final Path secondTempDir = createTempDirWithFiles(secondFiles, 1000);
        final Iterable<Ds3Object> secondObjects = helper.listObjectsForDirectory(secondTempDir);

        final Ds3ClientHelpers.Job secondJob = helper.startWriteJob(BUCKET_NAME, secondObjects,
                WriteJobOptions.create().withAggregating());
        final UUID secondJobId = secondJob.getJobId();
        LOG.info("Second aggregating PUT job returned: " + secondJobId);

        assertEquals(jobId, secondJobId,
                "Both aggregating PUT requests should return the same job ID");

        // Close the aggregating job from further aggregation
        client.closeAggregatingJobSpectraS3(new CloseAggregatingJobSpectraS3Request(jobId));
        LOG.info("Closed aggregating job: " + jobId);

        // Transfer second batch of objects
        secondJob.transfer(new FileObjectPutter(secondTempDir));
        LOG.info("Second batch transferred.");

        // Verify there is exactly 1 active job
        final List<ActiveJob> activeJobs = client.getActiveJobsSpectraS3(new GetActiveJobsSpectraS3Request())
                .getActiveJobListResult().getActiveJobs();
        assertEquals(1, activeJobs.size(),
                "There should be exactly 1 active job");
        LOG.info("Active jobs count: " + activeJobs.size());

        // Wait for the job to complete
        waitForJobsToComplete(client);
        LOG.info("All jobs completed.");

        // Verify there is exactly 1 completed job
        final List<CompletedJob> completedJobs = getCompletedJobs(client);
        assertEquals(1, completedJobs.size(),
                "There should be exactly 1 completed job");
        LOG.info("Completed jobs count: " + completedJobs.size());
    }
}
