package com.spectralogic.integrations.objects;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetBucketResponse;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.BulkObject;
import com.spectralogic.ds3client.models.CompletedJob;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static com.spectralogic.integrations.TestConstants.authId;
import static org.junit.jupiter.api.Assertions.*;

@Order(5)
public class ReadLibraryDisconnectTest {
    final String bucketName = "library_disconnect_bucket";
    final String secondBucketName = "second_bucket";
    String inputPath = "testFiles";
    private Ds3Client client;

    private final static Logger LOG = Logger.getLogger( ReadLibraryDisconnectTest.class );
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
            TestUtils.cleanSetUp(client);
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
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
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test: ReadLibraryDisconnectTest" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);

            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName);
            helper.ensureBucketExists(secondBucketName);
            // Get the testFiles folder as a resource
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);
            final Iterable<Ds3Object> objects2 = helper.listObjectsForDirectory(inputPath);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);

            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPath));
            UUID jobId = job.getJobId();
            addJobName(client, "ReadLibraryDisconnectTest", jobId);
            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();


            final List<Ds3Object> objectList = new ArrayList<>();
            final GetBucketResponse bucketResponse = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, bucketResponse.getListBucketResult().getObjects().size());
            for (final Contents contents : bucketResponse.getListBucketResult().getObjects()) {
                objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
            }
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request = new GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request( bucketName, objectList );
            GetPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response = client.getPhysicalPlacementForObjectsWithFullDetailsSpectraS3(getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Request);
            List<BulkObject> details =  getPhysicalPlacementForObjectsWithFullDetailsSpectraS3Response.getBulkObjectListResult().getObjects();

            HttpClient simClient = HttpClient.newHttpClient();


            HttpRequest simRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:7071/swapErrorTapeEnvironment"))
                    .GET()
                    .build();

            HttpResponse<String> getResponse = simClient.send(simRequest, HttpResponse.BodyHandlers.ofString());
            if (getResponse.statusCode() != 200) {
                System.err.println("GET failed: " + getResponse.body());
                return;
            }

            final Ds3ClientHelpers.Job secondJob = helper.startWriteJob(secondBucketName, objects2);
            secondJob.transfer(new FileObjectPutter(inputPath));
            addJobName(client, "CopyTapeDualTest-AfterDisconnect", secondJob.getJobId());
            TestUtil.sleep(1000 * 60 );

            simRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:7071/swapTapeEnvironment"))
                    .header("Content-Type", "application/json")
                    .GET()
                    .build();
            simClient.send(simRequest, HttpResponse.BodyHandlers.ofString());

            getTapesReady( client);
            TestUtil.sleep(1000 * 60  );
            while (!client.getActiveJobsSpectraS3( request ).getActiveJobListResult().getActiveJobs().isEmpty()) {
                TestUtil.sleep(1000);
            }

            Optional<UUID> completedId = getCompletedJobs(client).stream()
                    .filter(model -> model.getId().equals(jobId)).findFirst().map(CompletedJob::getId) ;
            assertNotNull(completedId.get());

            // Get the list of objects from the bucket that you want to perform the bulk get with.
            final GetBucketResponse response = client.getBucket(new GetBucketRequest(bucketName));
            assertEquals(5, getBlobCountOnTape(client, bucketName));
            assertEquals(5, response.getListBucketResult().getObjects().size());

        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
