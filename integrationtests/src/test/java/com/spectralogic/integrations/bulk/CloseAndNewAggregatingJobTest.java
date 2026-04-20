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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;


public class CloseAndNewAggregatingJobTest {
    private final static Logger LOG = Logger.getLogger(CloseAndNewAggregatingJobTest.class);

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
        final Path tempDir = Files.createTempDirectory("close-new-agg-test");
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
    public void testClosedAggregatingJobDoesNotAggregateWithNewJob() throws IOException, InterruptedException {
        LOG.info("Starting test: CloseAndNewAggregatingJobTest - testClosedAggregatingJobDoesNotAggregateWithNewJob");
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
        final UUID firstJobId = firstJob.getJobId();
        LOG.info("First aggregating PUT job created: " + firstJobId);

        // Close the first aggregating job and perform the bulk put
        client.closeAggregatingJobSpectraS3(new CloseAggregatingJobSpectraS3Request(firstJobId));
        LOG.info("Closed first aggregating job: " + firstJobId);
        firstJob.transfer(new FileObjectPutter(firstTempDir));
        LOG.info("First batch transferred.");

        // Second aggregating bulk PUT with obj3.txt, obj4.txt — should NOT aggregate into closed job
        final String[] secondFiles = {"obj3.txt", "obj4.txt"};
        final Path secondTempDir = createTempDirWithFiles(secondFiles, 1000);
        final Iterable<Ds3Object> secondObjects = helper.listObjectsForDirectory(secondTempDir);

        final Ds3ClientHelpers.Job secondJob = helper.startWriteJob(BUCKET_NAME, secondObjects,
                WriteJobOptions.create().withAggregating());
        final UUID secondJobId = secondJob.getJobId();
        LOG.info("Second aggregating PUT job created: " + secondJobId);

        assertNotEquals(firstJobId, secondJobId,
                "A new aggregating job should NOT aggregate into a closed aggregating job");

        // Close the second aggregating job and perform the bulk put
        client.closeAggregatingJobSpectraS3(new CloseAggregatingJobSpectraS3Request(secondJobId));
        LOG.info("Closed second aggregating job: " + secondJobId);
        secondJob.transfer(new FileObjectPutter(secondTempDir));
        LOG.info("Second batch transferred.");

        // Verify there are exactly 2 active jobs
        final List<ActiveJob> activeJobs = client.getActiveJobsSpectraS3(new GetActiveJobsSpectraS3Request())
                .getActiveJobListResult().getActiveJobs();
        assertEquals(2, activeJobs.size(),
                "There should be exactly 2 active jobs");
        LOG.info("Active jobs count: " + activeJobs.size());

        // Wait for the jobs to complete
        waitForJobsToComplete(client);
        LOG.info("All jobs completed.");

        // Verify there are exactly 2 completed jobs
        final List<CompletedJob> completedJobs = getCompletedJobs(client);
        assertEquals(2, completedJobs.size(),
                "There should be exactly 2 completed jobs");
        LOG.info("Completed jobs count: " + completedJobs.size());
    }
}
