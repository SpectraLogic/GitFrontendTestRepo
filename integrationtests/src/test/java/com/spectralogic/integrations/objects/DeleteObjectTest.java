package com.spectralogic.integrations.objects;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.DataIsolationLevel;
import com.spectralogic.ds3client.models.DataPersistenceRuleType;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.ds3client.models.VersioningLevel;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.integrations.TestUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("LocalDevelopment")
public class DeleteObjectTest {
    final String bucketName = "single_copy_bucket";

    private final static Logger LOG = Logger.getLogger( DeleteObjectTest.class );
    private Ds3Client client;
    DataPolicy dp;

    public DataPolicy updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> versionDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_VERSIONS)).findFirst();

        if (!versionDP.isPresent()) {
            PutDataPolicySpectraS3Request newDataPolicy = new PutDataPolicySpectraS3Request(DATA_POLICY_VERSIONS);
            PutDataPolicySpectraS3Response dataPolicyResponse = client.putDataPolicySpectraS3(newDataPolicy);
            dp = dataPolicyResponse.getDataPolicyResult();
            UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME);
            PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3Request = new PutDataPersistenceRuleSpectraS3Request(dp.getId(), DataIsolationLevel.BUCKET_ISOLATED, storageDomainId, DataPersistenceRuleType.PERMANENT);
            client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3Request);
            ModifyDataPolicySpectraS3Request request = new ModifyDataPolicySpectraS3Request(dp.getId());
            request.withVersioning(VersioningLevel.KEEP_MULTIPLE_VERSIONS);
            client.modifyDataPolicySpectraS3(request);
        } else {
            dp = versionDP.get();

        }

        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(dp.getId() ));
        return dp;
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_VERSIONS );
            TestUtils.cleanSetUp(client);
            getTapesReady(client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            cleanupAllBuckets(client);
            clearS3ReplicationRules(client, DATA_POLICY_VERSIONS );
            clearPersistenceRules(client, dp.getId());
            DeleteDataPolicySpectraS3Request deleteDataPolicySpectraS3Request = new DeleteDataPolicySpectraS3Request(dp.getId());
            client.deleteDataPolicySpectraS3(deleteDataPolicySpectraS3Request);
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test

    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : DeleteObjectTest" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            DataPolicy dp = updateUserDataPolicy(client);


            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName, dp.getId());
            // Get the testFiles folder as a resource



            final Path tempDir = Files.createTempDirectory("DeleteObjectTest_Input");
            tempDir.toFile().deleteOnExit();


            // Create some files in the temp directory
            for (int i = 0; i < 1; i++) {
                Path file = tempDir.resolve("file_" + i + ".txt");
                Files.write(file, ("Content for file " + i).getBytes());
                file.toFile().deleteOnExit();
            }


            Path path = Paths.get(tempDir.toString(), "file_0.txt");
            SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.READ);
            PutObjectRequest put1 = new PutObjectRequest(bucketName, "obj1", seekableByteChannel,Files.size(path) );
            client.putObject(put1);
            Thread.sleep(100);
            GetActiveJobsSpectraS3Response activeJobs = client.getActiveJobsSpectraS3(new GetActiveJobsSpectraS3Request());
            UUID putJobId = activeJobs.getActiveJobListResult().getActiveJobs().get(0).getId();
            isJobCompleted(client, putJobId);

            SeekableByteChannel seekableByteChannel2 = Files.newByteChannel(path, StandardOpenOption.READ);
            PutObjectRequest put2 = new PutObjectRequest(bucketName, "obj1", seekableByteChannel2,Files.size(path) );
            client.putObject(put2);
            Thread.sleep(100);
            activeJobs = client.getActiveJobsSpectraS3(new GetActiveJobsSpectraS3Request());
            UUID putJobId2 = activeJobs.getActiveJobListResult().getActiveJobs().get(0).getId();
            isJobCompleted(client, putJobId2);

            SeekableByteChannel seekableByteChannel3 = Files.newByteChannel(path, StandardOpenOption.READ);
            PutObjectRequest put3 = new PutObjectRequest(bucketName, "obj1", seekableByteChannel3,Files.size(path) );
            client.putObject(put3);
            Thread.sleep(100);
            activeJobs = client.getActiveJobsSpectraS3(new GetActiveJobsSpectraS3Request());
            UUID putJobId3 = activeJobs.getActiveJobListResult().getActiveJobs().get(0).getId();
            isJobCompleted(client, putJobId3);


            Path pathAfterDelete = Paths.get("output-without-cache.txt");
            Path pathWithCache = Paths.get("output-with-cache.txt");

            FileChannel channelWithCache = FileChannel.open(
                    pathWithCache,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);

            FileChannel channelAfterDelete = FileChannel.open(
                    pathAfterDelete,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, "obj1", channelWithCache);
            client.getObject(getObjectRequest);

            GetActiveJobsSpectraS3Request request = new GetActiveJobsSpectraS3Request();
            client.getActiveJobsSpectraS3(request);
            assertEquals(4, getCompletedJobs(client).size(), "There should be four  jobs completed.");


            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, "obj1" );
            client.deleteObject(deleteObjectRequest);

             getObjectRequest = new GetObjectRequest(bucketName, "obj1", channelAfterDelete);

            try {
                client.getObject(getObjectRequest);
                Assertions.fail("Should have thrown an exception because the object was deleted");
            } catch (FailedRequestException e) {
                assertEquals(404, e.getStatusCode(), "Should return 404 Not Found");
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
