package com.spectralogic.integrations.future;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.BlobStoreTaskInformation;
import com.spectralogic.ds3client.models.DataPolicy;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.DatabaseUtils.corruptCheckSum;
import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearDs3ReplicationRules;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearDs3Targets;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

//https://jira.spectralogic.com/browse/EMPROD-5637
@Tag("LocalDevelopment")

public class IOMDisabledSuspectTest {

    private final static Logger LOG = Logger.getLogger( IOMDisabledSuspectTest.class );
    final String bucketName = "iom-test-bucket";
    String inputPath = "testFiles";
    private Ds3Client client;

    public void updateUserDataPolicy(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_DUAL_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        if (client != null) {
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_DUAL_COPY_NAME );
            clearDs3Targets(client);
            TestUtils.cleanSetUp(client);
        }

    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearDs3ReplicationRules(client, DATA_POLICY_TAPE_DUAL_COPY_NAME );
            TestUtils.cleanSetUp(client);
            client.close();
        }

    }

    //@Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    public void testPutJob() throws IOException, InterruptedException {
        LOG.info("Starting test: IOMDisabledSuspectTest");
        try {
            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);
            updateUserDataPolicy(client);

            //Disable IOM
            toggleIOM(client, false);

            helper.ensureBucketExists(bucketName);
            // Get the testFiles folder as a resource
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPath = Paths.get(testFilesUrl.toURI());
            final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPath);


            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            job.transfer(new FileObjectPutter(inputPath));

            UUID currentJobId = job.getJobId();
            addJobName(client, "IOMDisabledSuspectTest", currentJobId);
            isJobCompleted(client, currentJobId);
            assertEquals(10, getBlobCountOnTape(client, bucketName));

            LOG.info("Put job completed for  IOMDisabledSuspectTest");
            reclaimCache(client);

            corruptCheckSum();

            TestUtil.sleep(1000);

            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);

            UUID readJobId = readJob.getJobId();

            TestUtil.sleep(1000);

            GetDataPlannerBlobStoreTasksSpectraS3Request getDataPlannerBlobStoreTasksSpectraS3Request = new GetDataPlannerBlobStoreTasksSpectraS3Request();
            getDataPlannerBlobStoreTasksSpectraS3Request.withFullDetails(true);
            GetDataPlannerBlobStoreTasksSpectraS3Response tasks = client.getDataPlannerBlobStoreTasksSpectraS3(getDataPlannerBlobStoreTasksSpectraS3Request);
            List<BlobStoreTaskInformation> taskDetails = tasks.getBlobStoreTasksInformationResult().getTasks();



        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
