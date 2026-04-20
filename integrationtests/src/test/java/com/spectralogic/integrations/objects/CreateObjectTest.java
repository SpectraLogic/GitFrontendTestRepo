package com.spectralogic.integrations.objects;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.*;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearS3ReplicationRules;
import static com.spectralogic.integrations.TestConstants.DATA_POLICY_TAPE_SINGLE_COPY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("LocalDevelopment")
public class CreateObjectTest {
    final String bucketName = "single_copy_bucket";
    String inputPath = "testFiles";
    private final static Logger LOG = Logger.getLogger( CreateObjectTest.class );
    private Ds3Client client;

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        modifyUser(client, singleCopyTapeDP.get());
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
        if (client != null) {
            clearS3ReplicationRules(client, DATA_POLICY_TAPE_SINGLE_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }
    }


    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testPutJobToTape() throws IOException, InterruptedException {
        LOG.info("Starting test : CreateObjectTest" );
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);


            // Make sure that the bucket exists, if it does not this will create it
            helper.ensureBucketExists(bucketName);

            final Path tempFilePath = Files.createTempFile("ds3test", "upload.txt");
            final Path tempFilePathOutput = Files.createTempFile("ds3test", "output.txt");

            tempFilePath.toFile().deleteOnExit();
            tempFilePathOutput.toFile().deleteOnExit();

            final byte[] content = "0123456789\n".getBytes();
            final long objectSize = content.length;

            Files.write(tempFilePath, content, StandardOpenOption.CREATE);

            // 4. Open the FileChannel for READING, as it is an upload (PutObjectRequest)
            try (FileChannel readChannel = FileChannel.open(tempFilePath, StandardOpenOption.READ)) {

                // 5. Use the readable channel for the PutObjectRequest
                PutObjectRequest request = new PutObjectRequest(bucketName,"CopyTapeTest", readChannel, objectSize);
                client.putObject(request);
            }

            GetActiveJobsSpectraS3Request jobRequest = new GetActiveJobsSpectraS3Request();
            GetActiveJobsSpectraS3Response activeJobsResponse = client.getActiveJobsSpectraS3( jobRequest );

            while (!activeJobsResponse.getActiveJobListResult().getActiveJobs().isEmpty()) {
                activeJobsResponse = client.getActiveJobsSpectraS3( jobRequest );
                TestUtil.sleep(1000);

            }

            //Try to get the object to verify it was written correctly

            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
            FileChannel channelWithoutCache = FileChannel.open(
                    tempFilePathOutput,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, "CopyTapeTest", channelWithoutCache);

            getObjectRequest.withOffset(0);
            getObjectRequest.withJob(readJob.getJobId());
            client.getObject(getObjectRequest);

            assertEquals(-1, Files.mismatch(tempFilePathOutput, tempFilePath), "Files are not identical");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
