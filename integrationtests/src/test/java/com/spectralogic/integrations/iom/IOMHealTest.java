/**
 * This test tries to mock data corruption by adding an entry to suspect_blob table. This will trigger IOM jobs.
 */
package com.spectralogic.integrations.iom;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
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
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.spectralogic.integrations.DatabaseUtils.getTestDatabaseConnection;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_DUAL_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;

@Tag("LocalDevelopment")
@Tag("iomtest")
public class IOMHealTest  {
    private final static Logger LOG = Logger.getLogger( IOMHealTest.class );
    final String bucketName = "iom_heal_bucket";
    String inputPath = "testFiles";
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
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> dualCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_DUAL_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dualCopyTapeDP.get().getId() ));
    }

    @Test
    public void testPutJobToTapes() throws IOException, InterruptedException {
        LOG.info("Starting test: IOMHealTest.");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);
            helper.ensureBucketExists(bucketName);

            // Get the testFiles folder as a resource
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);

            // Create the write job with the bucket we want to write to and the list
            // of objects that will be written
            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);

            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPath));
            UUID currentJobId = job.getJobId();
            addJobName(client, "IOMHealTest", currentJobId);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3(request);

            Optional<UUID> filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(ActiveJob::getId);

            LOG.info("Waiting for PUT job to be complete..");
            while (!filteredId.isEmpty()) {
                activeJobsResponse = client.getActiveJobsSpectraS3(request);
                filteredId = activeJobsResponse.getActiveJobListResult().getActiveJobs().stream()
                        .filter(model -> model.getId().equals(currentJobId)).findFirst().map(ActiveJob::getId);
                TestUtil.sleep(1000);


            }
            LOG.info("PUT job completed ....");

            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(currentJobId)).findFirst().map(CompletedJob::getId);
            Assertions.assertNotNull(completedId.get());
            Assertions.assertEquals(10, getBlobCountOnTape(client, bucketName));

            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            final List<Ds3Object> objectList = new ArrayList<>();

            Assertions.assertEquals(5, response.getListBucketResult().getObjects().size());
            for (final Contents contents : response.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request(bucketName, objectList);
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            List<BulkObject> details = getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();
            for (BulkObject detail : details) {
                Assertions.assertEquals(2, detail.getPhysicalPlacement().getTapes().size());
            }
            LOG.info("Adding entry to suspect_blob_table to create IOM jobs ....");
            UUID blobId = details.get(0).getId();
            UUID tapeId = details.get(0).getPhysicalPlacement().getTapes().get(0).getId();
            String getIdSql = "SELECT id, order_index FROM tape.blob_tape WHERE blob_id = ?";
            String sql = "INSERT INTO tape.suspect_blob_tape (id,tape_id, blob_id, order_index) VALUES (?, ?, ?, ?)";

            Connection connection = null;
            PreparedStatement statement = null;

            try {
                connection = getTestDatabaseConnection(); // Placeholder for your connection logic
                statement = connection.prepareStatement(getIdSql);
                statement.setObject(1, blobId);
                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    UUID id = (UUID) resultSet.getObject("id");
                    int orderIndex = resultSet.getInt("order_index");
                    statement = connection.prepareStatement(sql);
                    statement.setObject(1, id);
                    statement.setObject(2, tapeId);
                    statement.setObject(3, blobId);
                    statement.setInt(4, orderIndex);
                    int rowsAffected = statement.executeUpdate();
                    if (rowsAffected == 0) {
                        throw new SQLException("Creating suspect blob entry failed, no rows affected.");
                    }
                }

            } catch (SQLException e) {
                throw new RuntimeException("Failed to insert suspect blob entry via JDBC", e);
            }
            finally {
                assert connection != null;
                connection.close();
            }
            LOG.info("Inserted suspect blob entry to database. Waiting for IOM jobs to be created ....");
            while (client.getActiveJobsSpectraS3(request).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep( 1000);
            }
            LOG.info("IOM jobs created. Waiting for them to complete ....");
            while (!client.getActiveJobsSpectraS3(request).getActiveJobListResult().getActiveJobs().isEmpty()) {
                if (getCompletedJobs(client).size() == 3) {
                    break;
                }
                TestUtil.sleep( 1000);
            }
            LOG.info("IOM jobs completed.");

            List<CompletedJob> completedJobs = getCompletedJobs(client);
            boolean hasIOMGetJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM GET Job"));
            boolean hasIOMPutJob = completedJobs.stream()
                    .anyMatch(completedJob -> completedJob.getName().contains("IOM PUT Job"));
            Assertions.assertTrue(hasIOMGetJob);
            Assertions.assertTrue(hasIOMPutJob);
        } catch (IOException | URISyntaxException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
