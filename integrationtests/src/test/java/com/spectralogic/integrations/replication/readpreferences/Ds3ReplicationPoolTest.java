package com.spectralogic.integrations.replication.readpreferences;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.spectrads3.*;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.FileObjectGetter;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.common.Credentials;
import com.spectralogic.integrations.TestUtils;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.spectralogic.integrations.Ds3ApiHelpers.*;
import static com.spectralogic.integrations.Ds3ApiHelpers.addJobName;
import static com.spectralogic.integrations.Ds3ApiHelpers.createPoolsPartition;
import static com.spectralogic.integrations.Ds3ApiHelpers.getBlobCountOnPool;
import static com.spectralogic.integrations.Ds3ApiHelpers.getPoolsPartition;
import static com.spectralogic.integrations.Ds3ApiHelpers.isJobCompleted;
import static com.spectralogic.integrations.Ds3ApiHelpers.reclaimCache;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearDs3ReplicationRules;
import static com.spectralogic.integrations.Ds3ReplicationUtils.clearDs3Targets;
import static com.spectralogic.integrations.Ds3ReplicationUtils.registerDockerDs3Target;
import static com.spectralogic.integrations.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/*
Test setup
On BP-B...

Create a data policy
Create a data policy acl for group "Everyone"
Create a user and set their default data policy ID to the data policy created above

On BP-A...

Create a data policy
Create a data policy acl for group "Everyone"
Create a storage domain with MediaEjectionAllowed set to false
Create a storage domain member with that storage domain and the default pool partition
Create a persistence rule with that data policy and storage domain
Create a user with the same secret key as that created for BP-B
Register BP-B as a DS3 replication target with default read preference set to AFTER_ONLINE_POOL
Create a permanent replication rule for the above referenced data policy targeting BP-B
Create a bucket with that data policy with the user created above
PUT a few objects to the bucket (we used sizes 10000B, 1000B and 99B) and wait for jobs to complete
Clear cache
GET the objects from the bucket
 */
public class Ds3ReplicationPoolTest {
    private final static Logger LOG = Logger.getLogger( Ds3ReplicationPoolTest.class );
    private static Ds3Client client;
    public Ds3Client remote_client;
    Credentials remote_creds = new Credentials(authId, secretKey);

    public void updateUserDataPolicyRemote(Ds3Client client) throws IOException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();
        assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> singleCopyTapeDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_TAPE_SINGLE_COPY_NAME)).findFirst();
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(singleCopyTapeDP.get().getId() ));
    }
    public static void updateUserDataPolicyPool(Ds3Client client) throws IOException, SQLException {
        final GetDataPoliciesSpectraS3Response responsePolicy =
                client.getDataPoliciesSpectraS3( new GetDataPoliciesSpectraS3Request() );
        List<DataPolicy> dataPolicies = responsePolicy.getDataPolicyListResult().getDataPolicies();

        Assertions.assertTrue(dataPolicies.size() > 1);
        Optional<DataPolicy> poolDP = dataPolicies.stream()
                .filter(dp -> dp.getName().equals(DATA_POLICY_POOL_NAME)).findFirst();
        if (poolDP.isEmpty()) {
            poolDP = Optional.ofNullable(createDataPolicy(client, DATA_POLICY_POOL_NAME));
        }

        clearPersistenceRules(client, poolDP.get().getId());
        UUID storageDomainId = getStorageDomainId(client, STORAGE_DOMAIN_POOL_NAME);
        PutDataPersistenceRuleSpectraS3Request putDataPersistenceRuleSpectraS3Request =
                new PutDataPersistenceRuleSpectraS3Request(poolDP.get().getId(), DataIsolationLevel.BUCKET_ISOLATED, storageDomainId, DataPersistenceRuleType.PERMANENT);
        PutDataPersistenceRuleSpectraS3Response rep = client.putDataPersistenceRuleSpectraS3(putDataPersistenceRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(poolDP.get().getId() ));

        clearDs3ReplicationRules(client, poolDP.get().getName()  );
        clearDs3Targets(client);


        Ds3Target target = registerDockerDs3Target(client,authId, secretKey, TargetReadPreferenceType.AFTER_ONLINE_POOL);
        PutDs3DataReplicationRuleSpectraS3Request putDs3DataReplicationRuleSpectraS3Request = new PutDs3DataReplicationRuleSpectraS3Request(poolDP.get().getId(), target.getId(), DataReplicationRuleType.PERMANENT);
        client.putDs3DataReplicationRuleSpectraS3(putDs3DataReplicationRuleSpectraS3Request);
        client.modifyUserSpectraS3(new ModifyUserSpectraS3Request(authId).withDefaultDataPolicyId(poolDP.get().getId() ));
    }

    @BeforeEach
    public void setUp() throws IOException, InterruptedException, SQLException {
        client = TestUtils.setTestParams();
        remote_client = Ds3ClientBuilder.create("http://localhost:8082", remote_creds).build();
        if (client != null) {
            clearDs3ReplicationRules(client, DATA_POLICY_POOL_NAME );
            clearDs3Targets(client);
            TestUtils.cleanSetUp(client);
        }
        if (remote_client != null) {
            clearDs3ReplicationRules(remote_client, DATA_POLICY_POOL_NAME );
            clearDs3Targets(remote_client);
            TestUtils.cleanSetUp(remote_client);
            FormatAllTapesSpectraS3Request formatAllTapesSpectraS3Request = new FormatAllTapesSpectraS3Request();
            remote_client.formatAllTapesSpectraS3(formatAllTapesSpectraS3Request);
            getTapesReady(remote_client);
        }
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, SQLException {
        if (client != null) {
            clearDs3ReplicationRules(client, DATA_POLICY_POOL_NAME );
            clearDs3Targets(client);
            TestUtils.cleanSetUp(client);
            client.close();
        }
        if (remote_client != null) {
            clearDs3ReplicationRules(remote_client, DATA_POLICY_POOL_NAME );
            clearDs3Targets(remote_client);
            TestUtils.cleanSetUp(remote_client);
            FormatAllTapesSpectraS3Request formatAllTapesSpectraS3Request = new FormatAllTapesSpectraS3Request();
            remote_client.formatAllTapesSpectraS3(formatAllTapesSpectraS3Request);
            getTapesReady(remote_client);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    public void testSingleCopyPool() throws  InterruptedException {
        final String bucketName = "pool_copy_bucket";
        String inputPath = "testFiles";

        LOG.info("Starting test : GetPoolTest" );
        try {

            final Ds3ClientHelpers helper = Ds3ClientHelpers.wrap(client);

            PoolPartition poolPartition ;
            try {
                poolPartition = getPoolsPartition(client, "test-partition");
            } catch ( NoSuchElementException e ) {
                createPoolsPartition(client, "test-partition");
                poolPartition = getPoolsPartition(client, "test-partition");
            } catch(Exception e) {
                throw new RuntimeException("Failed to create or retrieve pool partition", e);
            }


            updateUserDataPolicyPool(client);
            updateUserDataPolicyRemote(remote_client);
            helper.ensureBucketExists(bucketName);
            final URL testFilesUrl = getClass().getClassLoader().getResource(inputPath);
            if (testFilesUrl == null) {
                throw new RuntimeException("Could not find testFiles directory in resources.");
            }
            final Path inputPathFiles = Paths.get(testFilesUrl.toURI());
            Iterable<Ds3Object> objects = helper.listObjectsForDirectory(inputPathFiles);

            final Ds3ClientHelpers.Job job = helper.startWriteJob(bucketName, objects);
            UUID currentJobId = job.getJobId();
            addJobName(client, "CopyPoolTest", currentJobId);
            // Start the write job using an Object Putter that will read the files
            // from the local file system.
            job.transfer(new FileObjectPutter(inputPathFiles));
            isJobCompleted(client, currentJobId);
            waitForJobsToComplete(remote_client);
            Thread.sleep(10000);
            LOG.info("CopyPoolTest job completed." +  currentJobId);
            assertEquals(5, getBlobCountOnPool(client, poolPartition.getId()));

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
            reclaimCache(remote_client);

            final Ds3ClientHelpers.Job readJob = helper.startReadAllJob(bucketName);
            UUID readJobId = readJob.getJobId();

            GetJobSpectraS3Request getJobSpectraS3Request = new GetJobSpectraS3Request(readJobId);
            GetJobSpectraS3Response getJobResponse = client.getJobSpectraS3(getJobSpectraS3Request);

            MasterObjectList mol = getJobResponse.getMasterObjectListResult();
            List<Objects> objs = mol.getObjects();
            for (Objects o: objs) {
                GetJobChunkDaoSpectraS3Request getJobChunkDaoSpectraS3Request = new GetJobChunkDaoSpectraS3Request(o.getChunkId().toString());
                GetJobChunkDaoSpectraS3Response jobChunkResponse = client.getJobChunkDaoSpectraS3(getJobChunkDaoSpectraS3Request);
                assertNotNull(jobChunkResponse.getJobChunkResult().getReadFromDs3TargetId(), "Read preference was not honored. Expected read from target but read from pool instead.");
                assertNull(jobChunkResponse.getJobChunkResult().getReadFromPoolId(), "Read preference was not honored. Expected read from target but read from pool instead.");
            }

            addJobName(client, "ReadTest", readJobId);

            readJob.transfer(new FileObjectGetter(outputPathFiles));

            isJobCompleted(client, readJobId);
            waitForJobsToComplete(remote_client);

            GetCompletedJobsSpectraS3Request request = new GetCompletedJobsSpectraS3Request();
            GetCompletedJobsSpectraS3Response completedJobs = remote_client.getCompletedJobsSpectraS3(request);
            assertEquals(2, completedJobs.getCompletedJobListResult().getCompletedJobs().size());


            LOG.info("Read job completed." +  readJobId);


        } catch (IOException | SQLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
